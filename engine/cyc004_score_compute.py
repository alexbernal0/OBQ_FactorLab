# -*- coding: utf-8 -*-
"""
engine/cyc004_score_compute.py
==============================
CYC-004 Factor Score Computation Pipeline

Computes all 37 new pure-factor scores from:
  1. fundamentals.filings_ttm        (primary TTM fundamentals)
  2. PROD_EOD_Fundamentals           (extended BS/CF fields: current A/L, retained earnings,
                                      AR, dividends, buybacks, pretax income)
  3. v_backtest_prices               (market_cap for yield/EV factors)
  4. Norgate daily prices (via mirror DB) (realized vol, market beta, skip-month momentum,
                                           idiosyncratic volatility)

Output: obq_fundamentals.duckdb -> scores.obq_cyc004_scores
  Table format: (symbol, month_date, as_of_filing_date, gic_sector, [37 raw factor columns])
  Raw values stored — GPU ranks/scores them during batch run (same pattern as CYC-002/003).

Factor list (37 total):
  TIER 1 (12): accruals_ratio, asset_turnover, ebit_assets, ocf_assets, roe,
               dividend_yield, net_payout_yield, eps_stability, sales_stability,
               current_ratio, fscore, realized_vol
  TIER 2 (13): ebit_ev, fcf_ev, sales_ev, pretax_margin, pretax_margin_dev,
               wc_assets, tax_paid_sales, op_leverage, retained_earnings_ta,
               shareholder_yield, market_beta, log_market_cap, intangibles_pb
  TIER 3 (12): cash_conv_cycle, change_ar_assets, change_inv_assets, skip_month_mom,
               gross_margin, debt_equity, quick_ratio, idio_vol,
               altman_z, repurchase_yield, roa_dev, roe_dev

GPU compute: All ranking, quintile assignment, IC, and backtest metrics happen on GPU.
             This pipeline only computes raw fundamental values.
"""
from __future__ import annotations

import os
import sys
import time
import logging
import math
from pathlib import Path
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import duckdb

log = logging.getLogger(__name__)

# ── Database paths ─────────────────────────────────────────────────────────────
FUND_DB   = os.environ.get("OBQ_FUND_DB",   r"D:/OBQ_AI/obq_fundamentals.duckdb")
MIRROR_DB = os.environ.get("OBQ_MIRROR_DB", r"D:/OBQ_AI/obq_eodhd_mirror.duckdb")

# ── Semi-annual rebalance dates (must align with CYC-003) ─────────────────────
START_DATE = "1995-03-31"
END_DATE   = "2024-12-31"

# ── Output table ──────────────────────────────────────────────────────────────
OUTPUT_SCHEMA = "scores"
OUTPUT_TABLE  = "obq_cyc004_scores"

# ── Factor column definitions ──────────────────────────────────────────────────
# (score_col, direction, display_name, tier)
CYC004_FACTOR_DEFS = [
    # TIER 1
    ("cyc4_accruals_ratio",      "lower_better",  "Accruals Ratio (Cash-Based)",           "cyc004"),
    ("cyc4_asset_turnover",      "higher_better", "Asset Turnover",                         "cyc004"),
    ("cyc4_ebit_assets",         "higher_better", "EBIT / Total Assets",                    "cyc004"),
    ("cyc4_ocf_assets",          "higher_better", "Operating Cash Flow / Assets",           "cyc004"),
    ("cyc4_roe",                 "higher_better", "Return on Equity",                       "cyc004"),
    ("cyc4_dividend_yield",      "higher_better", "Dividend Yield",                         "cyc004"),
    ("cyc4_net_payout_yield",    "higher_better", "Net Payout Yield (Div + Buybacks)",      "cyc004"),
    ("cyc4_eps_stability",       "lower_better",  "EPS Stability (20Q CoV)",                "cyc004"),
    ("cyc4_sales_stability",     "lower_better",  "Sales Stability (20Q CoV)",              "cyc004"),
    ("cyc4_current_ratio",       "higher_better", "Current Ratio",                          "cyc004"),
    ("cyc4_fscore",              "higher_better", "Piotroski F-Score",                      "cyc004"),
    ("cyc4_realized_vol",        "lower_better",  "Realized Volatility (26w Annualized)",   "cyc004"),
    # TIER 2
    ("cyc4_ebit_ev",             "higher_better", "EBIT / Enterprise Value",                "cyc004"),
    ("cyc4_fcf_ev",              "higher_better", "FCF / Enterprise Value",                 "cyc004"),
    ("cyc4_sales_ev",            "higher_better", "Sales / Enterprise Value",               "cyc004"),
    ("cyc4_pretax_margin",       "higher_better", "Pretax Margin",                          "cyc004"),
    ("cyc4_pretax_margin_dev",   "higher_better", "Pretax Margin vs 5yr Avg",               "cyc004"),
    ("cyc4_wc_assets",           "lower_better",  "Working Capital / Assets",               "cyc004"),
    ("cyc4_tax_paid_sales",      "higher_better", "Tax Expense / Sales (Quality Signal)",   "cyc004"),
    ("cyc4_op_leverage",         "higher_better", "Operating Leverage (5yr Smoothed)",      "cyc004"),
    ("cyc4_retained_earnings_ta","higher_better", "Retained Earnings / Total Assets",       "cyc004"),
    ("cyc4_shareholder_yield",   "higher_better", "Shareholder Yield (Div+Buyback+Debt Repay)", "cyc004"),
    ("cyc4_market_beta",         "lower_better",  "Market Beta (60m Rolling)",              "cyc004"),
    ("cyc4_log_market_cap",      "higher_better", "Log Market Cap (Size Factor)",           "cyc004"),
    ("cyc4_intangibles_pb",      "lower_better",  "Intangibles-Adjusted P/B",               "cyc004"),
    # TIER 3
    ("cyc4_cash_conv_cycle",     "lower_better",  "Cash Conversion Cycle (Days)",           "cyc004"),
    ("cyc4_change_ar_assets",    "lower_better",  "Change in AR / Assets (1yr)",            "cyc004"),
    ("cyc4_change_inv_assets",   "lower_better",  "Change in Inventory / Assets (1yr)",     "cyc004"),
    ("cyc4_skip_month_mom",      "higher_better", "Skip-Month Momentum (12m - 1m)",         "cyc004"),
    ("cyc4_gross_margin",        "higher_better", "Gross Profit Margin",                    "cyc004"),
    ("cyc4_debt_equity",         "lower_better",  "Debt / Equity",                          "cyc004"),
    ("cyc4_quick_ratio",         "higher_better", "Quick Ratio",                            "cyc004"),
    ("cyc4_idio_vol",            "lower_better",  "Idiosyncratic Volatility (60m)",         "cyc004"),
    ("cyc4_altman_z",            "higher_better", "Altman Z-Score",                         "cyc004"),
    ("cyc4_repurchase_yield",    "higher_better", "Repurchase Yield (Buybacks Only)",       "cyc004"),
    ("cyc4_roa_dev",             "higher_better", "ROA vs 5yr Avg (Trend)",                 "cyc004"),
    ("cyc4_roe_dev",             "higher_better", "ROE vs 5yr Avg (Trend)",                 "cyc004"),
]

SCORE_COLS = [f[0] for f in CYC004_FACTOR_DEFS]


# ── Schema creation ────────────────────────────────────────────────────────────

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


# ── Helper functions ───────────────────────────────────────────────────────────

def _safe_div(num: pd.Series, denom: pd.Series, min_denom: float = 1e-9) -> pd.Series:
    """Safe division, returns NaN where denominator is too small."""
    d = denom.copy()
    d[d.abs() < min_denom] = np.nan
    return num / d


def _rolling_cov(a: pd.Series, b: pd.Series, min_periods: int = 3) -> pd.Series:
    """Rolling 20Q covariance for operating leverage."""
    return a.rolling(window=20, min_periods=min_periods).cov(b)


def _compute_price_factors(mirror_con: duckdb.DuckDBPyConnection,
                            rebal_dates: list[date]) -> pd.DataFrame:
    """
    Compute price-based factors for all symbols at each rebalance date:
      - cyc4_realized_vol:  26w annualized realized volatility
      - cyc4_market_beta:   60m rolling beta vs equal-weight R3K
      - cyc4_skip_month_mom: 12m - 1m momentum
      - cyc4_idio_vol:      residual from 60m CAPM regression
      - cyc4_log_market_cap: log(market_cap)
      - cyc4_dividend_yield: dividend_yield from price table (if available)

    Returns: DataFrame indexed (symbol, month_date) with price factor columns.
    """
    log.info("  Computing price-based factors from Norgate data...")
    t0 = time.time()

    # Pull weekly prices going back 5 years before START_DATE for rolling windows
    price_start = "1990-01-01"
    price_end   = END_DATE

    prices = mirror_con.execute(f"""
        SELECT
            CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
            price_date, adjusted_close, market_cap
        FROM v_backtest_prices
        WHERE price_date >= '{price_start}'
          AND price_date <= '{price_end}'
          AND adjusted_close > 0
        ORDER BY symbol, price_date
    """).df()

    if prices.empty:
        log.warning("  No price data found — price factors will be NaN")
        return pd.DataFrame()

    prices['price_date'] = pd.to_datetime(prices['price_date'])
    prices = prices.set_index(['symbol', 'price_date']).sort_index()

    # Weekly resample — use Friday close (or last available)
    weekly = prices['adjusted_close'].unstack(level=0)  # date x symbol
    weekly = weekly.resample('W-FRI').last().ffill(limit=3)
    mktcap = prices['market_cap'].unstack(level=0).resample('W-FRI').last().ffill(limit=3)

    # Weekly returns
    weekly_rets = weekly.pct_change().replace([np.inf, -np.inf], np.nan)

    # Equal-weight market return (R3K proxy)
    mkt_ret = weekly_rets.mean(axis=1)

    results = []
    n_dates = len(rebal_dates)
    log.info(f"  Processing {n_dates} rebalance dates (vectorized GPU-style per date)...")

    # Pre-convert to numpy for fast slicing
    weekly_arr = weekly_rets.values         # (T_weeks, N_symbols) float64
    mkt_arr_full = mkt_ret.values           # (T_weeks,)
    mktcap_arr = mktcap.values              # (T_weeks, N_symbols)
    all_symbols = weekly_rets.columns.tolist()
    all_dates_idx = weekly_rets.index       # DatetimeIndex

    w26  = 26
    w52  = 52
    w4   = 4
    w260 = 260

    for i, rd in enumerate(rebal_dates):
        if i % 10 == 0:
            log.info(f"    Price factors: {i+1}/{n_dates} dates")
        rd_ts = pd.Timestamp(rd)

        # Index of last week on or before rebalance date
        t_idx = int((all_dates_idx <= rd_ts).sum()) - 1
        if t_idx < w26:
            continue

        # ── Realized volatility (26w) — fully vectorized across all stocks ────
        vol_start = max(0, t_idx - w26 + 1)
        vol_block = weekly_arr[vol_start:t_idx+1, :]           # (26, N)
        realized_vol = np.nanstd(vol_block, axis=0, ddof=1) * np.sqrt(52)  # (N,)

        # ── Log Market Cap ─────────────────────────────────────────────────────
        mc_row = mktcap_arr[t_idx, :]                          # (N,)
        log_mc = np.where(mc_row > 0, np.log(np.maximum(mc_row, 1e-6)), np.nan)

        # ── Skip-month momentum ────────────────────────────────────────────────
        if t_idx >= w52 + w4:
            mom_start = max(0, t_idx - w52 - w4 + 1)
            mom_end   = t_idx - w4 + 1
            skip_block = weekly_arr[mom_start:mom_end, :]      # (52w excluding 4w)
            ret_1m_block = weekly_arr[t_idx-w4+1:t_idx+1, :]  # (4w)
            ret_52w = np.nanprod(1 + skip_block, axis=0) - 1
            ret_1m  = np.nanprod(1 + ret_1m_block, axis=0) - 1
            skip_mom = ret_52w - ret_1m
        else:
            skip_mom = np.full(len(all_symbols), np.nan)

        # ── Market Beta + Idiosyncratic Volatility (60m = 260w) ───────────────
        if t_idx >= w260:
            beta_start = max(0, t_idx - w260 + 1)
            beta_block = weekly_arr[beta_start:t_idx+1, :]     # (260, N)
            mkt_w      = mkt_arr_full[beta_start:t_idx+1]      # (260,)

            # Vectorized: cov(stock, mkt) / var(mkt) for all stocks at once
            # Demean
            valid_mkt = ~np.isnan(mkt_w)
            mkt_v = np.where(valid_mkt, mkt_w, 0.0)
            mkt_mean = np.nanmean(mkt_w)
            mkt_dm = mkt_w - mkt_mean                          # (260,)
            mkt_var = np.nanvar(mkt_w, ddof=1)

            if mkt_var > 1e-12:
                # stock_dm shape: (260, N) — demean each column
                stock_mean = np.nanmean(beta_block, axis=0, keepdims=True)  # (1, N)
                stock_dm   = beta_block - stock_mean                        # (260, N)

                # Fill NaN with 0 for dot product (NaN stocks excluded)
                valid_stocks = (~np.isnan(beta_block)).sum(axis=0) >= 60   # (N,) bool
                stock_dm_safe = np.where(np.isnan(stock_dm), 0.0, stock_dm)
                mkt_dm_safe   = np.where(np.isnan(mkt_dm),   0.0, mkt_dm)

                cov_sm = (mkt_dm_safe[:, None] * stock_dm_safe).mean(axis=0)  # (N,)
                betas_arr  = np.where(valid_stocks, cov_sm / mkt_var, np.nan)

                # Residuals and idiosyncratic vol
                resid = beta_block - betas_arr[None, :] * mkt_w[:, None]   # (260, N)
                idio_vols_arr = np.nanstd(resid, axis=0, ddof=1) * np.sqrt(52)
                idio_vols_arr = np.where(valid_stocks, idio_vols_arr, np.nan)
            else:
                betas_arr     = np.full(len(all_symbols), np.nan)
                idio_vols_arr = np.full(len(all_symbols), np.nan)
        else:
            betas_arr     = np.full(len(all_symbols), np.nan)
            idio_vols_arr = np.full(len(all_symbols), np.nan)

        # Build output DataFrame for this date
        date_df = pd.DataFrame({
            'symbol':             all_symbols,
            'month_date':         rd,
            'cyc4_realized_vol':  realized_vol,
            'cyc4_log_market_cap':log_mc,
            'cyc4_skip_month_mom':skip_mom,
            'cyc4_market_beta':   betas_arr,
            'cyc4_idio_vol':      idio_vols_arr,
        })
        results.append(date_df)

    if not results:
        return pd.DataFrame()

    out = pd.concat(results, ignore_index=True)
    log.info(f"  Price factors complete: {len(out):,} rows in {time.time()-t0:.1f}s")
    return out


def _compute_fundamental_factors(fund_con: duckdb.DuckDBPyConnection,
                                  mirror_con: duckdb.DuckDBPyConnection,
                                  rebal_dates: list[date]) -> pd.DataFrame:
    """
    Compute all 37 fundamental + derived CYC-004 factor scores.
    Returns wide DataFrame: (symbol, month_date) + all factor columns.
    """
    t0 = time.time()
    log.info("  Loading filings_ttm base data...")

    # ── Base query: filings_ttm for all rebalance dates ────────────────────────
    dates_str = "', '".join(str(d) for d in rebal_dates)

    base = fund_con.execute(f"""
        SELECT
            symbol,
            rebalance_date         AS month_date,
            as_of_filing_date,
            gic_sector,
            -- Income / profitability
            ttm_revenue,
            ttm_gross_profit,
            ttm_operating_income   AS ttm_ebit,   -- EODHD: operating_income = EBIT
            ttm_ebitda,
            ttm_net_income,
            ttm_operating_cashflow AS ttm_ocf,
            ttm_free_cashflow      AS ttm_fcf,
            ttm_interest_expense,
            ttm_rnd_expense,
            ttm_depreciation,
            -- Per-share (for stability calcs)
            ttm_revenue_per_share,
            ttm_eps_diluted_ps     AS ttm_eps,
            -- Balance sheet
            mrq_total_assets,
            mrq_equity,            -- common + preferred; ≈ common equity for most non-financials
            mrq_cash,
            mrq_short_term_debt,
            mrq_long_term_debt,
            mrq_net_debt,
            mrq_net_working_capital AS mrq_nwc,
            mrq_inventory,
            mrq_book_value,
            mrq_shares_outstanding,
            -- Flags
            quarters_in_ttm,
            has_full_ttm
        FROM fundamentals.filings_ttm
        WHERE rebalance_date IN ('{dates_str}')
          AND has_full_ttm = TRUE
          AND mrq_total_assets > 0
        ORDER BY symbol, rebalance_date
    """).df()

    log.info(f"  Base data: {len(base):,} rows across {base['symbol'].nunique():,} symbols")

    # ── Extended data from PROD_EOD_Fundamentals (annual/quarterly filing data) ─
    # We join on symbol + closest filing date before or on rebalance_date
    log.info("  Loading PROD_EOD_Fundamentals extended fields...")

    # Use EODHD fundamentals for: current assets/liabilities, retained earnings,
    # net receivables (AR), accounts payable, dividends, repurchases, pretax income
    eod = mirror_con.execute("""
        SELECT
            CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
            CAST(filing_date AS DATE) AS eod_filing_date,
            CAST(date AS DATE) AS eod_date,
            -- Balance sheet
            bs_totalCurrentAssets      AS eod_ca,
            bs_totalCurrentLiabilities AS eod_cl,
            bs_retainedEarnings        AS eod_retained_earnings,
            bs_netReceivables          AS eod_ar,
            bs_accountsPayable         AS eod_ap,
            -- Cash flows
            cf_dividendsPaid           AS eod_dividends_paid,
            cf_salePurchaseOfStock     AS eod_repurchases,   -- negative = buyback, positive = issuance
            cf_netBorrowings           AS eod_net_borrowings,
            cf_totalCashFromOperatingActivities  AS eod_ocf_full,
            cf_totalCashflowsFromInvestingActivities AS eod_icf,
            -- Income
            is_incomeBeforeTax         AS eod_pretax_income,
            is_incomeTaxExpense        AS eod_tax_expense,
            -- Other
            dividend_yield             AS eod_div_yield_pct,
            beta                       AS eod_beta
        FROM PROD_EOD_Fundamentals
        WHERE date >= '1990-01-01'
    """).df()
    eod['eod_filing_date'] = pd.to_datetime(eod['eod_filing_date'])
    eod['eod_date'] = pd.to_datetime(eod['eod_date'])
    eod = eod.sort_values(['symbol', 'eod_filing_date'])

    log.info(f"  EOD Fundamentals: {len(eod):,} rows, {eod['symbol'].nunique():,} symbols")

    # Merge: for each (symbol, month_date) in base, get the latest EOD row
    # with eod_filing_date <= month_date (PIT-accurate)
    base['month_date'] = pd.to_datetime(base['month_date'])
    base_sorted = base.sort_values(['symbol', 'month_date']).reset_index(drop=True)

    # PIT join: for each (symbol, month_date) find the latest EOD row with
    # eod_filing_date <= month_date within 1yr tolerance.
    # Using DuckDB ASOF JOIN — more robust than pandas merge_asof for mixed types.
    log.info("  Joining EOD extended fields (PIT-accurate via DuckDB ASOF)...")
    eod_join_cols = ['symbol','eod_filing_date','eod_ca','eod_cl','eod_retained_earnings',
                     'eod_ar','eod_ap','eod_dividends_paid','eod_repurchases',
                     'eod_net_borrowings','eod_ocf_full','eod_icf','eod_pretax_income',
                     'eod_tax_expense','eod_div_yield_pct','eod_beta']
    eod_clean = eod[eod_join_cols].dropna(subset=['eod_filing_date']).copy()
    eod_clean['eod_filing_date'] = pd.to_datetime(eod_clean['eod_filing_date'])

    # Register both in-memory for DuckDB join (faster than manual per-symbol join)
    import duckdb as _duckdb
    tmp_con = _duckdb.connect()
    tmp_con.register('_base', base_sorted)
    tmp_con.register('_eod',  eod_clean)

    # ASOF JOIN: for each base row, get the latest eod row with
    # eod_filing_date <= month_date (and within 365 days)
    eod_col_list = ', '.join(
        f'e.{c}' for c in eod_join_cols if c not in ('symbol','eod_filing_date')
    )
    merged = tmp_con.execute(f"""
        SELECT b.*,
               {eod_col_list}
        FROM _base b
        ASOF LEFT JOIN _eod e
        ON b.symbol = e.symbol
        AND b.month_date >= e.eod_filing_date
        WHERE e.eod_filing_date IS NULL
           OR DATEDIFF('day', e.eod_filing_date, b.month_date) <= 365
    """).df()
    tmp_con.close()

    df = merged.copy()
    log.info(f"  After join: {len(df):,} rows")

    # ── Compute all factor scores ──────────────────────────────────────────────
    log.info("  Computing fundamental factor scores...")

    ta   = df['mrq_total_assets']
    eq   = df['mrq_equity']
    ni   = df['ttm_net_income']
    rev  = df['ttm_revenue']
    ocf  = df['ttm_ocf']
    fcf  = df['ttm_fcf']
    ebit = df['ttm_ebit']

    # Market cap — will be joined from price data later; use placeholder
    # We need market_cap for yield/EV factors — join from v_backtest_prices
    log.info("  Joining market_cap from v_backtest_prices...")
    mc_df = mirror_con.execute(f"""
SELECT
    CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
    price_date AS month_date, market_cap
FROM v_backtest_prices
WHERE price_date IN ('{dates_str}')
  AND market_cap > 0
""").df()
    mc_df['month_date'] = pd.to_datetime(mc_df['month_date'])
    df = df.merge(mc_df, on=['symbol','month_date'], how='left')
    mc = df['market_cap']

    # Enterprise Value = market_cap + net_debt
    net_debt = df['mrq_net_debt']
    ev = mc + net_debt
    ev[ev <= 0] = np.nan

    # ── TIER 1 ─────────────────────────────────────────────────────────────────

    # 1. Accruals Ratio: (Net Income - OCF) / Total Assets
    #    Simplified cash-based Sloan version. Negative = higher cash quality.
    #    If EOD investing CF available, use full version.
    eod_ocf = df.get('eod_ocf_full', pd.Series(np.nan, index=df.index))
    eod_icf = df.get('eod_icf', pd.Series(np.nan, index=df.index))
    accruals_full = ni - (eod_ocf + eod_icf)
    accruals_simple = ni - ocf
    # Use full where available, fallback to simple
    accruals = np.where(eod_ocf.notna() & eod_icf.notna(), accruals_full, accruals_simple)
    df['cyc4_accruals_ratio'] = _safe_div(pd.Series(accruals, index=df.index), ta)

    # 2. Asset Turnover
    df['cyc4_asset_turnover'] = _safe_div(rev, ta)

    # 3. EBIT / Assets (using operating_income as EBIT)
    df['cyc4_ebit_assets'] = _safe_div(ebit, ta)

    # 4. OCF / Assets
    df['cyc4_ocf_assets'] = _safe_div(ocf, ta)

    # 5. ROE = Net Income / Equity
    df['cyc4_roe'] = _safe_div(ni, eq)

    # 6. Dividend Yield = |dividends_paid| / market_cap (or pre-calculated)
    div_paid = df.get('eod_dividends_paid', pd.Series(np.nan, index=df.index)).abs()
    div_yield_pct = df.get('eod_div_yield_pct', pd.Series(np.nan, index=df.index))
    # Prefer pre-calculated yield; fallback to computed
    div_yield_calc = _safe_div(div_paid, mc)
    df['cyc4_dividend_yield'] = div_yield_pct.fillna(div_yield_calc)

    # 7. Net Payout Yield = (dividends + buybacks) / market_cap
    #    cf_salePurchaseOfStock: negative = cash out for buybacks, positive = stock sold
    repurchases = df.get('eod_repurchases', pd.Series(0.0, index=df.index))
    buybacks = repurchases.clip(upper=0).abs()  # only outflows (buybacks)
    total_payout = div_paid + buybacks
    df['cyc4_net_payout_yield'] = _safe_div(total_payout, mc)

    # 8. EPS Stability = rolling 20Q CoV (coefficient of variation) — lower = better
    # 9. Sales Stability = rolling 20Q CoV
    # These require the time series — compute per symbol
    log.info("  Computing rolling stability metrics (20Q)...")
    eps_cv  = df.groupby('symbol')['ttm_eps'].transform(
        lambda x: x.rolling(20, min_periods=8).std() / x.rolling(20, min_periods=8).mean().abs()
    )
    sales_cv = df.groupby('symbol')['ttm_revenue_per_share'].transform(
        lambda x: x.rolling(20, min_periods=8).std() / x.rolling(20, min_periods=8).mean().abs()
    )
    df['cyc4_eps_stability']   = eps_cv
    df['cyc4_sales_stability'] = sales_cv

    # 10. Current Ratio = CA / CL (from EOD)
    ca = df.get('eod_ca', pd.Series(np.nan, index=df.index))
    cl = df.get('eod_cl', pd.Series(np.nan, index=df.index))
    df['cyc4_current_ratio'] = _safe_div(ca, cl)

    # 11. Piotroski F-Score (9 binary signals, sum = 0-9)
    log.info("  Computing Piotroski F-Score...")
    df['_roa']  = _safe_div(ni, ta)
    df['_roa_prior'] = df.groupby('symbol')['_roa'].shift(2)  # 2 semi-annual periods = 1 year
    df['_ocf_ta']    = _safe_div(ocf, ta)
    df['_d_roa']     = df['_roa'] - df['_roa_prior']
    df['_accrual_fscore'] = df['_ocf_ta'] - df['_roa']  # positive = OCF > NI (quality)
    df['_debt_ratio'] = _safe_div(df['mrq_long_term_debt'], ta)
    df['_debt_prior'] = df.groupby('symbol')['_debt_ratio'].shift(2)
    df['_d_debt'] = df['_debt_ratio'] - df['_debt_prior']  # negative = improving
    df['_cr_prior'] = df.groupby('symbol')['cyc4_current_ratio'].shift(2)
    df['_d_cr'] = df['cyc4_current_ratio'] - df['_cr_prior']
    df['_shares'] = df['mrq_shares_outstanding']
    df['_shares_prior'] = df.groupby('symbol')['_shares'].shift(2)
    df['_d_shares'] = df['_shares'] - df['_shares_prior']  # negative = no dilution
    gm = _safe_div(df['ttm_gross_profit'], rev)
    df['_gm'] = gm
    df['_gm_prior'] = df.groupby('symbol')['_gm'].shift(2)
    df['_d_gm'] = df['_gm'] - df['_gm_prior']
    at = _safe_div(rev, ta)
    df['_at'] = at
    df['_at_prior'] = df.groupby('symbol')['_at'].shift(2)
    df['_d_at'] = df['_at'] - df['_at_prior']

    # F-Score = sum of 9 binary signals
    f1 = (df['_roa'] > 0).astype(float)
    f2 = (df['_ocf_ta'] > 0).astype(float)
    f3 = (df['_d_roa'] > 0).astype(float)
    f4 = (df['_accrual_fscore'] > 0).astype(float)
    f5 = (df['_d_debt'] < 0).astype(float)              # leverage decreased
    f6 = (df['_d_cr'] > 0).astype(float)                # liquidity improved
    f7 = (df['_d_shares'] <= 0).astype(float)           # no dilution
    f8 = (df['_d_gm'] > 0).astype(float)                # gross margin improved
    f9 = (df['_d_at'] > 0).astype(float)                # asset turnover improved

    # Only count where we have prior-year data
    has_prior = df['_roa_prior'].notna()
    fscore = (f1 + f2 + f3 + f4 + f5 + f6 + f7 + f8 + f9)
    fscore[~has_prior] = np.nan
    df['cyc4_fscore'] = fscore

    # Clean temp columns
    df.drop(columns=[c for c in df.columns if c.startswith('_')], inplace=True)

    # ── TIER 2 ─────────────────────────────────────────────────────────────────

    # 12. EBIT/EV
    df['cyc4_ebit_ev'] = _safe_div(ebit, ev)

    # 13. FCF/EV
    df['cyc4_fcf_ev'] = _safe_div(fcf, ev)

    # 14. Sales/EV
    df['cyc4_sales_ev'] = _safe_div(rev, ev)

    # 15. Pretax Margin (prefer EOD is_incomeBeforeTax; fallback to EBIT - interest proxy)
    eod_pretax = df.get('eod_pretax_income', pd.Series(np.nan, index=df.index))
    pretax_proxy = ebit - df['ttm_interest_expense'].fillna(0)
    pretax = eod_pretax.fillna(pretax_proxy)
    df['cyc4_pretax_margin'] = _safe_div(pretax, rev)

    # 16. Pretax Margin 5yr deviation (10 semi-annual periods)
    df['cyc4_pretax_margin_dev'] = df.groupby('symbol')['cyc4_pretax_margin'].transform(
        lambda x: x - x.rolling(10, min_periods=4).mean().shift(1)
    )

    # 17. Working Capital / Assets (lower = more capital-light, better signal)
    df['cyc4_wc_assets'] = _safe_div(df['mrq_nwc'], ta)

    # 18. Tax Paid / Sales (earnings quality signal — accrual basis)
    eod_tax = df.get('eod_tax_expense', pd.Series(np.nan, index=df.index)).abs()
    df['cyc4_tax_paid_sales'] = _safe_div(eod_tax, rev)

    # 19. Operating Leverage = smoothed 5yr (ΔEBIT / ΔRevenue)
    # Use rolling correlation × (σ_EBIT / σ_Rev) as proxy for sensitivity
    df['cyc4_op_leverage'] = df.groupby('symbol').apply(
        lambda g: (
            g['ttm_ebit'].diff()
            .rolling(10, min_periods=4)
            .mean() / g['ttm_revenue'].diff().rolling(10, min_periods=4).mean()
        ).clip(-5, 20)
    ).reset_index(level=0, drop=True)

    # 20. Retained Earnings / Total Assets
    ret_earn = df.get('eod_retained_earnings', pd.Series(np.nan, index=df.index))
    df['cyc4_retained_earnings_ta'] = _safe_div(ret_earn, ta)

    # 21. Shareholder Yield = (dividends + buybacks + debt repayment) / market_cap
    net_borrow = df.get('eod_net_borrowings', pd.Series(0.0, index=df.index)).fillna(0)
    debt_repayment = (-net_borrow).clip(lower=0)  # positive when net debt decreased
    df['cyc4_shareholder_yield'] = _safe_div(total_payout + debt_repayment, mc)

    # 22. Log Market Cap (size factor — lower OBQ expected, confirms cap-tier analysis)
    df['cyc4_log_market_cap'] = np.log(mc.clip(lower=1))

    # 23. Intangibles-Adjusted P/B
    # Capitalize R&D using Peters & Taylor (2017) 5-year declining balance (30% annual amort)
    # Accumulated RnD capital = sum(rnd_t × (0.7)^(years_ago))
    log.info("  Computing intangibles-adjusted P/B (R&D capitalization)...")
    rnd = df['ttm_rnd_expense'].fillna(0)
    rnd_1 = df.groupby('symbol')['ttm_rnd_expense'].shift(2).fillna(0)
    rnd_2 = df.groupby('symbol')['ttm_rnd_expense'].shift(4).fillna(0)
    rnd_3 = df.groupby('symbol')['ttm_rnd_expense'].shift(6).fillna(0)
    rnd_4 = df.groupby('symbol')['ttm_rnd_expense'].shift(8).fillna(0)
    # Capitalized knowledge capital (Peters & Taylor declining balance)
    knowl_cap = (rnd * 1.0 + rnd_1 * 0.7 + rnd_2 * 0.49 + rnd_3 * 0.343 + rnd_4 * 0.240)
    adj_book = df['mrq_book_value'] + knowl_cap
    df['cyc4_intangibles_pb'] = _safe_div(mc, adj_book.clip(lower=1))

    # ── TIER 3 ─────────────────────────────────────────────────────────────────

    # 24. Cash Conversion Cycle = DIO + DSO - DPO
    ar   = df.get('eod_ar', pd.Series(np.nan, index=df.index))
    ap   = df.get('eod_ap', pd.Series(np.nan, index=df.index))
    inv  = df['mrq_inventory']
    cogs = rev - df['ttm_gross_profit']  # COGS = Revenue - Gross Profit
    cogs[cogs <= 0] = np.nan

    dso = _safe_div(ar * 365, rev)
    dio = _safe_div(inv * 365, cogs)
    dpo = _safe_div(ap * 365, cogs)
    df['cyc4_cash_conv_cycle'] = dso + dio - dpo

    # 25. Change in AR / Assets (1yr = 2 semi-annual periods)
    ar_ta = _safe_div(ar, ta)
    df['cyc4_change_ar_assets'] = df.groupby('symbol')['cyc4_current_ratio'].transform(
        lambda x: x - x.shift(2)  # placeholder — overwrite below
    )
    # Actually compute from ar_ta directly
    ar_ta_series = ar_ta.copy()
    ar_ta_series.index = df.index
    df['_ar_ta'] = ar_ta_series
    df['cyc4_change_ar_assets'] = df.groupby('symbol')['_ar_ta'].transform(
        lambda x: x - x.shift(2)
    )
    df.drop(columns=['_ar_ta'], inplace=True)

    # 26. Change in Inventory / Assets
    inv_ta = _safe_div(inv, ta)
    df['_inv_ta'] = inv_ta
    df['cyc4_change_inv_assets'] = df.groupby('symbol')['_inv_ta'].transform(
        lambda x: x - x.shift(2)
    )
    df.drop(columns=['_inv_ta'], inplace=True)

    # 27. Gross Profit Margin (note: different from GPA = gross_profit/assets)
    df['cyc4_gross_margin'] = _safe_div(df['ttm_gross_profit'], rev)

    # 28. Debt / Equity
    total_debt = df['mrq_short_term_debt'].fillna(0) + df['mrq_long_term_debt'].fillna(0)
    df['cyc4_debt_equity'] = _safe_div(total_debt, eq)

    # 29. Quick Ratio = (Cash + AR) / Current Liabilities
    quick = df['mrq_cash'].fillna(0) + ar.fillna(0)
    df['cyc4_quick_ratio'] = _safe_div(quick, cl)

    # 30. Altman Z-Score (modified for public companies)
    # Z = 1.2*(NWC/TA) + 1.4*(RE/TA) + 3.3*(EBIT/TA) + 0.6*(MV/TL) + 1.0*(S/TA)
    tl = df['mrq_total_assets'] - df['mrq_equity']  # Total Liabilities ≈ Assets - Equity
    tl[tl <= 0] = np.nan
    mv_tl = _safe_div(mc, tl)
    z1 = 1.2 * _safe_div(df['mrq_nwc'], ta)
    z2 = 1.4 * _safe_div(ret_earn, ta)
    z3 = 3.3 * _safe_div(ebit, ta)
    z4 = 0.6 * mv_tl
    z5 = 1.0 * _safe_div(rev, ta)
    df['cyc4_altman_z'] = z1 + z2 + z3 + z4 + z5

    # 31. Repurchase Yield (buybacks only)
    df['cyc4_repurchase_yield'] = _safe_div(buybacks, mc)

    # 32. ROA 5yr deviation
    roa = _safe_div(ni, ta)
    df['_roa_full'] = roa
    df['cyc4_roa_dev'] = df.groupby('symbol')['_roa_full'].transform(
        lambda x: x - x.rolling(10, min_periods=4).mean().shift(1)
    )
    df.drop(columns=['_roa_full'], inplace=True)

    # 33. ROE 5yr deviation
    roe = _safe_div(ni, eq)
    df['_roe_full'] = roe
    df['cyc4_roe_dev'] = df.groupby('symbol')['_roe_full'].transform(
        lambda x: x - x.rolling(10, min_periods=4).mean().shift(1)
    )
    df.drop(columns=['_roe_full'], inplace=True)

    # Note: cyc4_skip_month_mom, cyc4_market_beta, cyc4_idio_vol, cyc4_realized_vol
    # are computed separately in _compute_price_factors()

    elapsed = time.time() - t0
    log.info(f"  Fundamental factors complete: {len(df):,} rows in {elapsed:.1f}s")

    return df


# ── Main pipeline ──────────────────────────────────────────────────────────────

def run_score_pipeline(
    fund_db: str = FUND_DB,
    mirror_db: str = MIRROR_DB,
    start_date: str = START_DATE,
    end_date: str = END_DATE,
    overwrite: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Full CYC-004 score computation pipeline.

    Steps:
      1. Get rebalance dates from mirror DB
      2. Compute fundamental factors (CPU, ~3-5 min)
      3. Compute price factors (CPU, ~10-15 min for 30yr × 3000 stocks)
      4. Merge and write to obq_fundamentals.duckdb
    """
    t_total = time.time()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    log.info("=" * 65)
    log.info("CYC-004 Score Computation Pipeline")
    log.info(f"  Period:   {start_date} -> {end_date}")
    log.info(f"  Dry-run:  {dry_run}")
    log.info("=" * 65)

    fund_con   = duckdb.connect(fund_db, read_only=dry_run)
    mirror_con = duckdb.connect(mirror_db, read_only=True)

    # ── Step 1: Get semi-annual rebalance dates ──────────────────────────────
    dates_rows = mirror_con.execute(f"""
        SELECT DISTINCT month_date
        FROM v_backtest_scores
        WHERE month_date >= '{start_date}'
          AND month_date <= '{end_date}'
          AND EXTRACT(MONTH FROM month_date) IN (6, 12)
          AND EXTRACT(DAY FROM month_date) >= 28
        ORDER BY month_date
    """).fetchall()
    rebal_dates = [r[0] for r in dates_rows]
    log.info(f"  Rebalance dates: {len(rebal_dates)} ({rebal_dates[0]} to {rebal_dates[-1]})")

    # ── Step 2: Fundamental factors ──────────────────────────────────────────
    log.info("\nStep 1/3: Computing fundamental factors...")
    fund_df = _compute_fundamental_factors(fund_con, mirror_con, rebal_dates)
    fund_df['month_date'] = pd.to_datetime(fund_df['month_date'])

    # ── Step 3: Price factors ────────────────────────────────────────────────
    log.info("\nStep 2/3: Computing price-based factors...")
    price_df = _compute_price_factors(mirror_con, rebal_dates)

    # ── Step 4: Merge ────────────────────────────────────────────────────────
    log.info("\nStep 3/3: Merging and writing to bank...")
    if not price_df.empty:
        price_df['month_date'] = pd.to_datetime(price_df['month_date'])
        # Price DF has cyc4_realized_vol, cyc4_market_beta, cyc4_skip_month_mom,
        # cyc4_idio_vol, cyc4_log_market_cap (price-based), cyc4_dividend_yield_price
        merged = fund_df.merge(
            price_df[['symbol','month_date',
                      'cyc4_realized_vol','cyc4_market_beta',
                      'cyc4_skip_month_mom','cyc4_idio_vol']],
            on=['symbol','month_date'], how='left'
        )
        # Use price-computed vol/beta (override fundamentals NaN)
    else:
        merged = fund_df
        for c in ['cyc4_realized_vol','cyc4_market_beta','cyc4_skip_month_mom','cyc4_idio_vol']:
            if c not in merged.columns:
                merged[c] = np.nan

    # Select final output columns
    meta_cols = ['symbol','month_date','as_of_filing_date','gic_sector']
    factor_cols = [c for c in SCORE_COLS if c in merged.columns]
    missing_cols = [c for c in SCORE_COLS if c not in merged.columns]
    if missing_cols:
        log.warning(f"  Missing factor columns (will be NaN): {missing_cols}")
        for c in missing_cols:
            merged[c] = np.nan

    out_df = merged[meta_cols + SCORE_COLS].copy()
    out_df['month_date'] = out_df['month_date'].dt.date
    out_df['as_of_filing_date'] = pd.to_datetime(out_df['as_of_filing_date']).dt.date

    log.info(f"  Output: {len(out_df):,} rows | {out_df['symbol'].nunique():,} symbols")

    # ── Step 5: Write to DB ──────────────────────────────────────────────────
    if dry_run:
        log.info("  DRY RUN — not writing to database")
        log.info(f"  Sample output:\n{out_df.head(3).to_string()}")
    else:
        log.info(f"  Writing to {fund_db} -> {OUTPUT_SCHEMA}.{OUTPUT_TABLE}...")

        # Ensure schema exists
        fund_con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")

        if overwrite:
            fund_con.execute(f"DROP TABLE IF EXISTS {OUTPUT_SCHEMA}.{OUTPUT_TABLE}")

        fund_con.execute(DDL_CREATE)

        # Bulk insert via register
        fund_con.register('_out_df', out_df)
        inserted = fund_con.execute(f"""
            INSERT OR REPLACE INTO {OUTPUT_SCHEMA}.{OUTPUT_TABLE}
            SELECT * FROM _out_df
            WHERE symbol IS NOT NULL AND month_date IS NOT NULL
        """).rowcount

        log.info(f"  Inserted {inserted:,} rows")

        # Quick QC
        qc = fund_con.execute(f"""
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT symbol) AS symbols,
              MIN(month_date) AS start,
              MAX(month_date) AS end,
              AVG(CASE WHEN cyc4_asset_turnover IS NOT NULL THEN 1 ELSE 0 END) AS at_fill_pct,
              AVG(CASE WHEN cyc4_fscore IS NOT NULL THEN 1 ELSE 0 END) AS fscore_fill_pct
            FROM {OUTPUT_SCHEMA}.{OUTPUT_TABLE}
        """).fetchone()
        log.info(f"\n  QC: rows={qc[0]:,} | symbols={qc[1]:,} | "
                 f"{qc[2]} to {qc[3]} | AT fill={qc[4]:.0%} | F-Score fill={qc[5]:.0%}")

    mirror_con.close()
    if not dry_run:
        fund_con.close()

    elapsed = time.time() - t_total
    log.info(f"\nCYC-004 score pipeline complete: {elapsed:.1f}s ({elapsed/60:.1f}min)")

    return {
        "rows": len(out_df),
        "symbols": out_df['symbol'].nunique(),
        "elapsed_s": elapsed,
        "factor_cols": factor_cols,
        "missing_cols": missing_cols,
    }


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='CYC-004 Score Computation Pipeline')
    ap.add_argument('--dry-run', action='store_true', help='No DB writes')
    ap.add_argument('--start',   default=START_DATE)
    ap.add_argument('--end',     default=END_DATE)
    ap.add_argument('--no-overwrite', action='store_true')
    args = ap.parse_args()

    result = run_score_pipeline(
        start_date=args.start,
        end_date=args.end,
        overwrite=not args.no_overwrite,
        dry_run=args.dry_run,
    )
    print(f"\nResult: {result['rows']:,} rows | {result['symbols']:,} symbols | {result['elapsed_s']:.1f}s")
    if result['missing_cols']:
        print(f"Missing factor columns: {result['missing_cols']}")
