# -*- coding: utf-8 -*-
"""
engine/cyc005_score_compute.py
==============================
CYC-005 Sector-Specific Factor Score Computation Pipeline

Computes 11 novel sector-specific factors — one keystone metric per GICS sector —
designed to capture the primary valuation/quality driver that is MOST meaningful
within each sector's unique business model.

All factors are computed globally (for all stocks where data exists) and stored in
scores.obq_cyc005_scores. The GPU run uses sector masks to restrict each factor's
quintile universe to its target sector, plus optional cross-sectional runs.

Factor definitions:
  cyc5_energy_midcycle_fcf     — Energy: 3yr rolling avg FCF / Market Cap (mid-cycle yield)
  cyc5_materials_ccc           — Materials: Cash Conversion Cycle (days, lower better)
  cyc5_industrials_efficiency  — Industrials: Asset Turnover × OCF Margin composite
  cyc5_consumer_disc_brand     — Consumer Disc: Gross Margin × Rev CAGR 3yr composite
  cyc5_staples_div_growth      — Consumer Staples: 2yr Dividend CAGR (management confidence)
  cyc5_healthcare_rnd_yield    — Health Care: (R&D/Market Cap) × (1 + Rev CAGR 3yr)
  cyc5_financials_capital      — Financials: Equity / Total Assets (capital adequacy proxy)
  cyc5_it_rule_of_40           — IT: Rev Growth % + FCF Margin % (Rule of 40)
  cyc5_comms_content_roi       — Comm Services: Revenue / (OpInc + R&D + SGA) spend
  cyc5_utilities_safe_yield    — Utilities: Dividend Yield × min(OCF Coverage, 3)
  cyc5_realestate_ffo_yield    — Real Estate: OCF / Market Cap (FFO proxy yield)
"""
from __future__ import annotations

import os
import time
import logging
import math
from typing import Optional

import numpy as np
import pandas as pd
import duckdb

log = logging.getLogger(__name__)

FUND_DB   = os.environ.get("OBQ_FUND_DB",   r"D:/OBQ_AI/obq_fundamentals.duckdb")
MIRROR_DB = os.environ.get("OBQ_MIRROR_DB", r"D:/OBQ_AI/obq_eodhd_mirror.duckdb")

START_DATE = "1995-03-31"
END_DATE   = "2024-12-31"

OUTPUT_SCHEMA = "scores"
OUTPUT_TABLE  = "obq_cyc005_scores"

# ── Factor column definitions ──────────────────────────────────────────────────
CYC005_FACTOR_DEFS = [
    ("cyc5_energy_midcycle_fcf",    "higher_better", "Energy: Mid-Cycle FCF Yield",              "cyc005"),
    ("cyc5_materials_ccc",          "lower_better",  "Materials: Cash Conversion Cycle",         "cyc005"),
    ("cyc5_industrials_efficiency", "higher_better", "Industrials: Efficiency Composite",        "cyc005"),
    ("cyc5_consumer_disc_brand",    "higher_better", "Cons Disc: Brand-Growth Composite",        "cyc005"),
    ("cyc5_staples_div_growth",     "higher_better", "Cons Staples: Dividend Growth Rate",       "cyc005"),
    ("cyc5_healthcare_rnd_yield",   "higher_better", "Health Care: R&D Yield",                   "cyc005"),
    ("cyc5_financials_capital",     "higher_better", "Financials: Capital Adequacy (Eq/Assets)", "cyc005"),
    ("cyc5_it_rule_of_40",          "higher_better", "IT: Rule of 40",                           "cyc005"),
    ("cyc5_comms_content_roi",      "higher_better", "Comm Services: Content ROI",               "cyc005"),
    ("cyc5_utilities_safe_yield",   "higher_better", "Utilities: Safe Dividend Yield",           "cyc005"),
    ("cyc5_realestate_ffo_yield",   "higher_better", "Real Estate: FFO Proxy Yield",             "cyc005"),
]

SCORE_COLS = [f[0] for f in CYC005_FACTOR_DEFS]

DDL_CREATE = f"""
CREATE TABLE IF NOT EXISTS {OUTPUT_SCHEMA}.{OUTPUT_TABLE} (
    symbol              VARCHAR NOT NULL,
    month_date          DATE    NOT NULL,
    as_of_filing_date   DATE,
    gic_sector          VARCHAR,
    {", ".join(f"{c}  DOUBLE" for c in SCORE_COLS)},
    PRIMARY KEY (symbol, month_date)
)
"""


def _safe_div(num: pd.Series, denom: pd.Series, min_denom: float = 1e-9) -> pd.Series:
    d = denom.copy()
    d[d.abs() < min_denom] = np.nan
    return num / d


def run_score_pipeline(
    fund_db: str = FUND_DB,
    mirror_db: str = MIRROR_DB,
    start_date: str = START_DATE,
    end_date: str = END_DATE,
    overwrite: bool = True,
    dry_run: bool = False,
) -> dict:
    """Compute all 11 CYC-005 sector-specific factor scores."""
    t_total = time.time()
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    log.info("=" * 65)
    log.info("CYC-005 Score Computation — Sector-Specific Factors")
    log.info(f"  Period: {start_date} -> {end_date}")
    log.info("=" * 65)

    fund_con   = duckdb.connect(fund_db, read_only=dry_run)
    mirror_con = duckdb.connect(mirror_db, read_only=True)

    # Rebalance dates
    dates_rows = mirror_con.execute(f"""
        SELECT DISTINCT month_date FROM v_backtest_scores
        WHERE month_date >= '{start_date}' AND month_date <= '{end_date}'
          AND EXTRACT(MONTH FROM month_date) IN (6, 12)
          AND EXTRACT(DAY FROM month_date) >= 28
        ORDER BY month_date
    """).fetchall()
    rebal_dates = [r[0] for r in dates_rows]
    dates_str   = "', '".join(str(d) for d in rebal_dates)
    log.info(f"  Rebalance dates: {len(rebal_dates)} ({rebal_dates[0]} to {rebal_dates[-1]})")

    # ── Base: filings_ttm at all rebalance dates ────────────────────────────────
    log.info("\nStep 1/4: Loading base filings data...")
    base = fund_con.execute(f"""
        SELECT symbol, rebalance_date AS month_date, as_of_filing_date, gic_sector,
               ttm_revenue, ttm_gross_profit, ttm_operating_income AS ttm_ebit,
               ttm_net_income, ttm_operating_cashflow AS ttm_ocf,
               ttm_free_cashflow AS ttm_fcf, ttm_rnd_expense,
               ttm_sga_expense, ttm_depreciation, ttm_interest_expense,
               ttm_revenue_per_share,
               mrq_total_assets, mrq_equity, mrq_cash, mrq_net_debt,
               mrq_book_value, mrq_short_term_debt, mrq_long_term_debt,
               mrq_net_working_capital AS mrq_nwc, mrq_inventory,
               mrq_shares_outstanding, quarters_in_ttm, has_full_ttm
        FROM fundamentals.filings_ttm
        WHERE rebalance_date IN ('{dates_str}')
          AND has_full_ttm = TRUE
          AND mrq_total_assets > 0
        ORDER BY symbol, rebalance_date
    """).df()
    base['month_date'] = pd.to_datetime(base['month_date'])
    log.info(f"  Base rows: {len(base):,} | symbols: {base['symbol'].nunique():,}")

    # ── EOD extended fields (dividends, AR, AP, current A/L) ───────────────────
    log.info("\nStep 2/4: Loading EOD extended fields...")
    eod = mirror_con.execute("""
        SELECT
            CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
            CAST(filing_date AS DATE) AS eod_filing_date,
            bs_totalCurrentAssets        AS eod_ca,
            bs_totalCurrentLiabilities  AS eod_cl,
            bs_netReceivables       AS eod_ar,
            bs_accountsPayable      AS eod_ap,
            bs_retainedEarnings     AS eod_re,
            cf_dividendsPaid        AS eod_div_paid,
            cf_salePurchaseOfStock  AS eod_repurchases,
            dividend_yield          AS eod_div_yield
        FROM PROD_EOD_Fundamentals
        WHERE date >= '1990-01-01'
          AND filing_date IS NOT NULL
    """).df()
    eod['eod_filing_date'] = pd.to_datetime(eod['eod_filing_date'])
    eod_sorted  = eod.dropna(subset=['eod_filing_date']).sort_values(['symbol','eod_filing_date']).reset_index(drop=True)
    base_sorted = base.sort_values(['symbol','month_date']).reset_index(drop=True)

    tmp = duckdb.connect()
    tmp.register('_base', base_sorted)
    tmp.register('_eod',  eod_sorted)
    eod_join_cols = [c for c in eod_sorted.columns if c not in ('symbol','eod_filing_date')]
    merged = tmp.execute(f"""
        SELECT b.*, {', '.join(f'e.{c}' for c in eod_join_cols)}
        FROM _base b
        ASOF LEFT JOIN _eod e
        ON b.symbol = e.symbol AND b.month_date >= e.eod_filing_date
    """).df()
    tmp.close()
    log.info(f"  After EOD join: {len(merged):,} rows")

    # ── Market cap join ────────────────────────────────────────────────────────
    log.info("\nStep 3/4: Joining market cap...")
    mc_df = mirror_con.execute(f"""
        SELECT
            CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
            price_date AS month_date, market_cap
        FROM v_backtest_prices
        WHERE price_date IN ('{dates_str}') AND market_cap > 0
    """).df()
    mc_df['month_date'] = pd.to_datetime(mc_df['month_date'])
    df = merged.merge(mc_df, on=['symbol','month_date'], how='left')
    mirror_con.close()

    # ── Compute all 11 sector-specific factors ─────────────────────────────────
    log.info("\nStep 4/4: Computing 11 sector-specific factors...")

    rev  = df['ttm_revenue']
    gp   = df['ttm_gross_profit']
    ebit = df['ttm_ebit']
    ni   = df['ttm_net_income']
    ocf  = df['ttm_ocf']
    fcf  = df['ttm_fcf']
    ta   = df['mrq_total_assets']
    eq   = df['mrq_equity']
    mc   = df['market_cap']
    rnd  = df['ttm_rnd_expense'].fillna(0)
    sga  = df['ttm_sga_expense'].fillna(0)
    cogs = (rev - gp).clip(lower=0)

    # ── 1. Energy: Mid-Cycle FCF Yield (3yr rolling avg FCF / market cap) ──────
    # Use 3-period (1.5yr) rolling avg for semi-annual data to approximate 3yr:
    # shift(0)=now, shift(2)=1yr ago, shift(4)=2yr ago → mean = 1.5yr avg
    log.info("  1. Energy: Mid-Cycle FCF Yield")
    fcf_t0 = fcf
    fcf_t2 = df.groupby('symbol')['ttm_fcf'].shift(2)
    fcf_t4 = df.groupby('symbol')['ttm_fcf'].shift(4)
    # Average of 3 semi-annual observations, clip negative avg to 0
    fcf_3yr_avg = ((fcf_t0.fillna(0) + fcf_t2.fillna(0) + fcf_t4.fillna(0)) / 3).clip(lower=0)
    df['cyc5_energy_midcycle_fcf'] = _safe_div(fcf_3yr_avg, mc)
    # Zero out for non-Energy (sectors use mask in GPU run, but store globally for cross-sectional test)

    # ── 2. Materials: Cash Conversion Cycle ────────────────────────────────────
    log.info("  2. Materials: Cash Conversion Cycle")
    ar  = df.get('eod_ar', pd.Series(np.nan, index=df.index))
    ap  = df.get('eod_ap', pd.Series(np.nan, index=df.index))
    inv = df['mrq_inventory']
    dso = _safe_div(ar * 365, rev)
    dio = _safe_div(inv * 365, cogs)
    dpo = _safe_div(ap * 365, cogs)
    df['cyc5_materials_ccc'] = dso + dio - dpo

    # ── 3. Industrials: Efficiency Composite (asset turnover × OCF margin) ─────
    log.info("  3. Industrials: Efficiency Composite")
    asset_turn  = _safe_div(rev, ta)
    ocf_margin  = _safe_div(ocf, rev)
    # Percentile rank GLOBALLY then average — GPU will rank within Industrials sector mask
    df['cyc5_industrials_efficiency'] = asset_turn * ocf_margin  # raw product; GPU ranks
    # Clip outliers
    df['cyc5_industrials_efficiency'] = df['cyc5_industrials_efficiency'].clip(
        df['cyc5_industrials_efficiency'].quantile(0.01),
        df['cyc5_industrials_efficiency'].quantile(0.99)
    )

    # ── 4. Consumer Disc: Brand-Growth Composite ───────────────────────────────
    log.info("  4. Consumer Disc: Brand-Growth Composite")
    gross_margin = _safe_div(gp, rev)
    # 3yr revenue CAGR from rolling semi-annual data (6 periods = 3yr)
    rev_prior6 = df.groupby('symbol')['ttm_revenue'].shift(6)
    rev_cagr_3yr = _safe_div(rev - rev_prior6, rev_prior6.abs()).clip(-0.5, 5.0)
    df['cyc5_consumer_disc_brand'] = (gross_margin + rev_cagr_3yr) / 2.0
    df['cyc5_consumer_disc_brand'] = df['cyc5_consumer_disc_brand'].clip(
        df['cyc5_consumer_disc_brand'].quantile(0.01),
        df['cyc5_consumer_disc_brand'].quantile(0.99)
    )

    # ── 5. Consumer Staples: Dividend Growth Rate ──────────────────────────────
    log.info("  5. Consumer Staples: Dividend Growth Rate")
    div_paid = df.get('eod_div_paid', pd.Series(np.nan, index=df.index)).abs()
    div_prior4 = div_paid.groupby(df['symbol']).shift(4)  # 2yr ago (4 semi-annual periods)
    # Annualized 2yr dividend CAGR
    div_cagr = _safe_div(
        (div_paid - div_prior4), div_prior4.abs()
    ).clip(-0.5, 3.0)
    # Flag firms with zero dividends as NaN (not paying = not applicable)
    div_cagr[div_prior4.isna() | (div_prior4.abs() < 1e-6)] = np.nan
    df['cyc5_staples_div_growth'] = div_cagr

    # ── 6. Health Care: R&D Yield ──────────────────────────────────────────────
    log.info("  6. Health Care: R&D Yield")
    rnd_intensity = _safe_div(rnd, mc)   # R&D per dollar of market cap
    rev_cagr_3yr_hc = rev_cagr_3yr.clip(lower=0)  # only reward positive growth
    df['cyc5_healthcare_rnd_yield'] = rnd_intensity * (1 + rev_cagr_3yr_hc)
    df['cyc5_healthcare_rnd_yield'][rnd < 1e-6] = np.nan  # NaN if no R&D (not a pharma/device co)
    df['cyc5_healthcare_rnd_yield'] = df['cyc5_healthcare_rnd_yield'].clip(
        0, df['cyc5_healthcare_rnd_yield'].quantile(0.99)
    )

    # ── 7. Financials: Capital Adequacy (Equity / Total Assets) ───────────────
    log.info("  7. Financials: Capital Adequacy Proxy")
    df['cyc5_financials_capital'] = _safe_div(eq, ta)
    # Exclude negative equity (distressed) and implausibly high ratios
    df.loc[eq < 0, 'cyc5_financials_capital'] = np.nan
    df['cyc5_financials_capital'] = df['cyc5_financials_capital'].clip(0, 0.9)

    # ── 8. IT: Rule of 40 (Rev Growth % + FCF Margin %) ──────────────────────
    log.info("  8. IT: Rule of 40")
    # 1yr revenue growth
    rev_prior2 = df.groupby('symbol')['ttm_revenue'].shift(2)
    rev_growth_1yr = _safe_div(rev - rev_prior2, rev_prior2.abs()).clip(-0.5, 5.0) * 100
    fcf_margin_pct = _safe_div(fcf, rev).clip(-1.0, 1.0) * 100
    rule_of_40 = rev_growth_1yr + fcf_margin_pct
    df['cyc5_it_rule_of_40'] = rule_of_40.clip(-100, 200)
    df.loc[rev_prior2.isna(), 'cyc5_it_rule_of_40'] = np.nan

    # ── 9. Communication Services: Content ROI ─────────────────────────────────
    log.info("  9. Comm Services: Content ROI")
    total_spend = (ebit.clip(lower=0) + rnd + sga).clip(lower=0)
    df['cyc5_comms_content_roi'] = _safe_div(rev, total_spend)
    df['cyc5_comms_content_roi'] = df['cyc5_comms_content_roi'].clip(
        0, df['cyc5_comms_content_roi'].quantile(0.99)
    )

    # ── 10. Utilities: Safe Dividend Yield ────────────────────────────────────
    log.info("  10. Utilities: Safe Dividend Yield")
    div_yield_pct = df.get('eod_div_yield', pd.Series(np.nan, index=df.index)).fillna(
        _safe_div(div_paid, mc)
    )
    # OCF / |dividends paid| = payout coverage (cap at 3x to avoid outliers)
    payout_coverage = _safe_div(ocf.abs(), div_paid).clip(0, 3)
    df['cyc5_utilities_safe_yield'] = div_yield_pct * payout_coverage
    df.loc[div_paid.isna() | (div_paid.abs() < 1e-6), 'cyc5_utilities_safe_yield'] = np.nan

    # ── 11. Real Estate: FFO Proxy Yield ──────────────────────────────────────
    log.info("  11. Real Estate: FFO Proxy Yield (OCF / Market Cap)")
    df['cyc5_realestate_ffo_yield'] = _safe_div(ocf, mc)
    df['cyc5_realestate_ffo_yield'] = df['cyc5_realestate_ffo_yield'].clip(
        df['cyc5_realestate_ffo_yield'].quantile(0.01),
        df['cyc5_realestate_ffo_yield'].quantile(0.99)
    )

    # ── Build output ───────────────────────────────────────────────────────────
    meta_cols = ['symbol','month_date','as_of_filing_date','gic_sector']
    out_df = df[meta_cols + SCORE_COLS].copy()
    out_df['month_date']         = out_df['month_date'].dt.date
    out_df['as_of_filing_date']  = pd.to_datetime(out_df['as_of_filing_date']).dt.date

    # Fill rate summary
    for c in SCORE_COLS:
        filled = out_df[c].notna().sum()
        log.info(f"  {c:<35} fill={filled/len(out_df)*100:.0f}%  ({filled:,}/{len(out_df):,})")

    # ── Write to DB ────────────────────────────────────────────────────────────
    if dry_run:
        log.info(f"\n  DRY RUN — {len(out_df):,} rows computed, not written")
        log.info(f"\n  Sample:\n{out_df.head(3).to_string()}")
    else:
        fund_con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")
        if overwrite:
            fund_con.execute(f"DROP TABLE IF EXISTS {OUTPUT_SCHEMA}.{OUTPUT_TABLE}")
        fund_con.execute(DDL_CREATE)
        fund_con.register('_out', out_df)
        inserted = fund_con.execute(f"""
            INSERT OR REPLACE INTO {OUTPUT_SCHEMA}.{OUTPUT_TABLE}
            SELECT * FROM _out WHERE symbol IS NOT NULL AND month_date IS NOT NULL
        """).rowcount
        log.info(f"\n  Inserted {inserted:,} rows into {OUTPUT_SCHEMA}.{OUTPUT_TABLE}")

    if not dry_run:
        fund_con.close()
    elapsed = time.time() - t_total
    log.info(f"\nCYC-005 score pipeline complete: {elapsed:.1f}s")
    return {"rows": len(out_df), "symbols": out_df['symbol'].nunique(), "elapsed_s": elapsed}


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--start',   default=START_DATE)
    ap.add_argument('--end',     default=END_DATE)
    args = ap.parse_args()
    result = run_score_pipeline(start_date=args.start, end_date=args.end, dry_run=args.dry_run)
    print(f"\nDone: {result['rows']:,} rows | {result['symbols']:,} symbols | {result['elapsed_s']:.1f}s")
