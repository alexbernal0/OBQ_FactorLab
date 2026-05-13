# -*- coding: utf-8 -*-
"""
engine/cyc007_score_compute.py
==============================
CYC-007 Sector-Optimized Composite Score Computation Pipeline

Builds 9 multi-factor composite scores — one or two per priority sector —
by rank-combining the best within-sector signals discovered in CYC-005.

Method: rank-based equal weighting
  1. For each component factor, compute percentile rank (0-1) across the
     full R3K universe at each rebalance date (direction-adjusted so higher=better).
  2. Average the component percentile ranks to form the composite.
  3. Result is a single score per stock per date, ready for GPU quintile backtest
     with a sector mask (within-sector ranking, same as CYC-005).

Composite definitions (grounded in CYC-005 OBQ baselines):
  cyc7_hc_quality      Health Care  OCF/Assets (0.876) + EBIT/Assets (0.853) + F-Score (0.840)
  cyc7_hc_alpha        Health Care  Alpha Trifecta (0.854) + OCF/Assets (0.876) + FCF Margin (0.812)
  cyc7_it_quality      IT           Alpha Trifecta (0.883) + OBQ Quality (0.849) + OCF/Assets (0.831)
  cyc7_it_fcf          IT           FCF Margin (0.839) + OCF/Assets (0.831) + Rule of 40 (0.322)
  cyc7_fin_quality     Financials   F-Score (0.547) + Alpha Trifecta (0.568) + QARP (0.504)
  cyc7_condisc_quality ConDisc      OCF/Assets (0.625) + F-Score (0.583) + Alpha Trifecta (0.683)
  cyc7_staples_quality Staples      OCF/Assets (0.651) + QARP (0.596) + Alpha Trifecta (0.596)
  cyc7_ind_quality     Industrials  F-Score (0.432) + Alpha Trifecta (0.660) + Ind Efficiency (0.367)
  cyc7_mat_quality     Materials    Alpha Trifecta (0.737) + F-Score (0.572) + OCF/Assets (0.479)

Score table: scores.obq_cyc007_scores in obq_fundamentals.duckdb

Usage:
    python engine/cyc007_score_compute.py
    python engine/cyc007_score_compute.py --dry-run
"""
from __future__ import annotations

import os
import time
import logging
from typing import Optional

import warnings
import numpy as np
import pandas as pd
import duckdb

log = logging.getLogger(__name__)

FUND_DB   = os.environ.get("OBQ_FUND_DB",    r"D:/OBQ_AI/obq_fundamentals.duckdb")
MIRROR_DB = os.environ.get("OBQ_MIRROR_DB",  r"D:/OBQ_AI/obq_eodhd_mirror.duckdb")

START_DATE = "1995-03-31"
END_DATE   = "2024-12-31"

OUTPUT_SCHEMA = "scores"
OUTPUT_TABLE  = "obq_cyc007_scores"

# ── Composite definitions ──────────────────────────────────────────────────────
# Each tuple: (col_name, display_name, [(component_col, direction), ...])
# direction: 'higher_better' | 'lower_better'
# Components must exist in one of: cyc004 scores, cyc005 scores, or v_backtest_scores

CYC007_COMPOSITES = [
    (
        "cyc7_hc_quality",
        "HC Quality Composite",
        [
            ("cyc4_ocf_assets",        "higher_better"),
            ("cyc4_ebit_assets",       "higher_better"),
            ("cyc4_fscore",            "higher_better"),
        ],
    ),
    (
        "cyc7_hc_alpha",
        "HC Alpha Composite",
        [
            ("jcn_alpha_trifecta",     "higher_better"),
            ("cyc4_ocf_assets",        "higher_better"),
            ("cyc2_fcf_margin",        "higher_better"),
        ],
    ),
    (
        "cyc7_it_quality",
        "IT Quality Composite",
        [
            ("jcn_alpha_trifecta",     "higher_better"),
            ("quality_score_universe", "higher_better"),
            ("cyc4_ocf_assets",        "higher_better"),
        ],
    ),
    (
        "cyc7_it_fcf",
        "IT FCF Composite",
        [
            ("cyc2_fcf_margin",        "higher_better"),
            ("cyc4_ocf_assets",        "higher_better"),
            ("cyc5_it_rule_of_40",     "higher_better"),
        ],
    ),
    (
        "cyc7_fin_quality",
        "Financials Quality Composite",
        [
            ("cyc4_fscore",            "higher_better"),
            ("jcn_alpha_trifecta",     "higher_better"),
            ("jcn_qarp",               "higher_better"),
        ],
    ),
    (
        "cyc7_condisc_quality",
        "Consumer Disc Quality Composite",
        [
            ("cyc4_ocf_assets",        "higher_better"),
            ("cyc4_fscore",            "higher_better"),
            ("jcn_alpha_trifecta",     "higher_better"),
        ],
    ),
    (
        "cyc7_staples_quality",
        "Consumer Staples Quality Composite",
        [
            ("cyc4_ocf_assets",        "higher_better"),
            ("jcn_qarp",               "higher_better"),
            ("jcn_alpha_trifecta",     "higher_better"),
        ],
    ),
    (
        "cyc7_ind_quality",
        "Industrials Quality Composite",
        [
            ("cyc4_fscore",            "higher_better"),
            ("jcn_alpha_trifecta",     "higher_better"),
            ("cyc5_industrials_efficiency", "higher_better"),
        ],
    ),
    (
        "cyc7_mat_quality",
        "Materials Quality Composite",
        [
            ("jcn_alpha_trifecta",     "higher_better"),
            ("cyc4_fscore",            "higher_better"),
            ("cyc4_ocf_assets",        "higher_better"),
        ],
    ),
]

SCORE_COLS = [c[0] for c in CYC007_COMPOSITES]

DDL_CREATE = f"""
CREATE TABLE IF NOT EXISTS {OUTPUT_SCHEMA}.{OUTPUT_TABLE} (
    symbol            VARCHAR NOT NULL,
    month_date        DATE    NOT NULL,
    gic_sector        VARCHAR,
    {", ".join(f"{c}  DOUBLE" for c in SCORE_COLS)},
    PRIMARY KEY (symbol, month_date)
)
"""


def _percentile_rank(series: pd.Series, lower_better: bool = False) -> pd.Series:
    """
    Compute percentile rank (0.0–1.0) within a cross-section.
    NaN inputs produce NaN outputs (not ranked).
    lower_better=True inverts: rank 1.0 = lowest raw value.
    """
    valid = series.notna()
    if valid.sum() == 0:
        return pd.Series(np.nan, index=series.index)
    ranked = series[valid].rank(method='average', ascending=not lower_better)
    n = valid.sum()
    pct = ranked / n
    out = pd.Series(np.nan, index=series.index)
    out[valid] = pct
    return out


def _build_composites(
    scores_df: pd.DataFrame,
    component_sources: dict,
) -> pd.DataFrame:
    """
    For each composite definition, compute percentile ranks per date per component,
    then average ranks to produce the composite.

    scores_df: wide DataFrame with columns [symbol, month_date, gic_sector, *all_components]
    component_sources: maps component_col -> (series, direction)  (already merged into df)
    """
    out_cols: dict = {}

    for comp_col, _, components in CYC007_COMPOSITES:
        available = [(col, d) for col, d in components if col in scores_df.columns]
        if not available:
            log.warning(f"  {comp_col}: NO components available — skipping")
            out_cols[comp_col] = pd.Series(np.nan, index=scores_df.index)
            continue

        missing = [col for col, _ in components if col not in scores_df.columns]
        if missing:
            log.warning(f"  {comp_col}: missing {missing} — using {len(available)}/{len(components)} components")

        # Compute per-date percentile ranks for each component, then average
        rank_frames = []
        for comp_col_src, direction in available:
            lower_b = (direction == 'lower_better')
            grp = scores_df.groupby('month_date')[comp_col_src].transform(
                lambda s: _percentile_rank(s, lower_better=lower_b)
            )
            rank_frames.append(grp)

        # Stack and average — NaN if ALL components are NaN for that stock-date
        rank_mat = np.column_stack([r.values for r in rank_frames])
        # nanmean: only average available components; require at least 2 of N valid
        n_valid = np.sum(~np.isnan(rank_mat), axis=1)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            composite = np.where(
                n_valid >= max(2, len(available) - 1),
                np.nanmean(rank_mat, axis=1),
                np.nan,
            )
        out_cols[comp_col] = composite

        fill_rate = np.sum(~np.isnan(composite)) / len(composite) * 100
        log.info(f"  {comp_col:<30}  components={len(available)}/{len(components)}  fill={fill_rate:.0f}%")

    return pd.DataFrame(out_cols, index=scores_df.index)


def run_score_pipeline(
    fund_db: str = FUND_DB,
    mirror_db: str = MIRROR_DB,
    start_date: str = START_DATE,
    end_date: str = END_DATE,
    overwrite: bool = True,
    dry_run: bool = False,
) -> dict:
    """Compute all 9 CYC-007 sector composite scores."""
    t_total = time.time()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    log.info("=" * 65)
    log.info("CYC-007 Score Computation — Sector-Optimized Composites")
    log.info(f"  Period: {start_date} -> {end_date}")
    log.info(f"  Output: {OUTPUT_SCHEMA}.{OUTPUT_TABLE}")
    log.info("=" * 65)

    # ── Open connections ────────────────────────────────────────────────────────
    fund_con   = duckdb.connect(fund_db,   read_only=dry_run)
    mirror_con = duckdb.connect(mirror_db, read_only=True)

    # ── Rebalance dates (SA-JUN-DEC) ───────────────────────────────────────────
    dates_rows = mirror_con.execute(f"""
        SELECT DISTINCT month_date FROM v_backtest_scores
        WHERE month_date >= '{start_date}' AND month_date <= '{end_date}'
          AND EXTRACT(MONTH FROM month_date) IN (6, 12)
          AND EXTRACT(DAY FROM month_date) >= 28
        ORDER BY month_date
    """).fetchall()
    rebal_dates = [r[0] for r in dates_rows]
    dates_str = "', '".join(str(d) for d in rebal_dates)
    log.info(f"  Rebalance dates: {len(rebal_dates)} ({rebal_dates[0]} to {rebal_dates[-1]})")

    # ── Step 1: Identify all required component columns ─────────────────────────
    all_components: set = set()
    for _, _, components in CYC007_COMPOSITES:
        for col, _ in components:
            all_components.add(col)

    # Split by source table:
    #   v_backtest_scores  → mirror_db   (jcn_*, quality_score_universe)
    #   obq_cyc004_scores  → fund_db
    #   obq_cyc005_scores  → fund_db
    #   PROD_OBQ_Quality_Scores → mirror_db  (cyc2_fcf_margin)

    VBS_COLS      = ['jcn_alpha_trifecta', 'jcn_qarp', 'quality_score_universe']
    CYC4_COLS     = ['cyc4_ocf_assets', 'cyc4_ebit_assets', 'cyc4_fscore']
    CYC5_COLS     = ['cyc5_industrials_efficiency', 'cyc5_it_rule_of_40']
    QUALITY_COLS  = ['cyc2_fcf_margin']   # from PROD_OBQ_Quality_Scores

    # Filter to what we actually need
    vbs_needed     = [c for c in VBS_COLS    if c in all_components]
    cyc4_needed    = [c for c in CYC4_COLS   if c in all_components]
    cyc5_needed    = [c for c in CYC5_COLS   if c in all_components]
    quality_needed = [c for c in QUALITY_COLS if c in all_components]

    # ── Step 2: Load symbol × date universe + sector ───────────────────────────
    log.info("\nStep 1/5: Loading base universe (symbol, date, sector)...")
    base = mirror_con.execute(f"""
        SELECT
            CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
            month_date,
            gic_sector
        FROM v_backtest_scores
        WHERE month_date IN ('{dates_str}')
          AND jcn_full_composite IS NOT NULL
        ORDER BY month_date, symbol
    """).df()
    base['month_date'] = pd.to_datetime(base['month_date'])
    log.info(f"  Base rows: {len(base):,} | symbols: {base['symbol'].nunique():,} | dates: {base['month_date'].nunique()}")

    # ── Step 3: Load v_backtest_scores components ──────────────────────────────
    if vbs_needed:
        log.info(f"\nStep 2/5: Loading v_backtest_scores components: {vbs_needed}")
        cols_sql = ", ".join(vbs_needed)
        vbs_df = mirror_con.execute(f"""
            SELECT
                CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
                month_date,
                {cols_sql}
            FROM v_backtest_scores
            WHERE month_date IN ('{dates_str}')
        """).df()
        vbs_df['month_date'] = pd.to_datetime(vbs_df['month_date'])
        base = base.merge(vbs_df, on=['symbol', 'month_date'], how='left')
        log.info(f"  After VBS join: {len(base):,} rows")

    # ── Step 4: Load CYC-004 scores ────────────────────────────────────────────
    # Use direct IN filter — CYC-004 is stored at the same SA dates as our universe.
    if cyc4_needed:
        log.info(f"\nStep 3/5: Loading CYC-004 scores: {cyc4_needed}")
        try:
            cyc4_check = fund_con.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema='scores' AND table_name='obq_cyc004_scores'
            """).fetchone()[0]
            if cyc4_check:
                cols_sql = ", ".join(cyc4_needed)
                cyc4_df = fund_con.execute(f"""
                    SELECT
                        symbol,
                        month_date,
                        {cols_sql}
                    FROM scores.obq_cyc004_scores
                    WHERE month_date IN ('{dates_str}')
                    ORDER BY month_date, symbol
                """).df()
                cyc4_df['month_date'] = pd.to_datetime(cyc4_df['month_date'])
                base = base.merge(cyc4_df, on=['symbol', 'month_date'], how='left')
                filled = base[cyc4_needed[0]].notna().sum()
                log.info(f"  After CYC-004 join: {len(base):,} rows | {cyc4_needed[0]} fill={filled/len(base)*100:.0f}%")
            else:
                log.warning("  CYC-004 table not found in fund_db — components will be missing")
        except Exception as e:
            log.warning(f"  CYC-004 load error (non-fatal): {e}")

    # ── Step 5: Load CYC-005 scores ────────────────────────────────────────────
    # CYC-005 is also stored at SA dates — use direct IN filter.
    if cyc5_needed:
        log.info(f"\nStep 4/5: Loading CYC-005 scores: {cyc5_needed}")
        try:
            cyc5_check = fund_con.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema='scores' AND table_name='obq_cyc005_scores'
            """).fetchone()[0]
            if cyc5_check:
                cols_sql = ", ".join(cyc5_needed)
                cyc5_df = fund_con.execute(f"""
                    SELECT
                        symbol,
                        month_date,
                        {cols_sql}
                    FROM scores.obq_cyc005_scores
                    WHERE month_date IN ('{dates_str}')
                    ORDER BY month_date, symbol
                """).df()
                cyc5_df['month_date'] = pd.to_datetime(cyc5_df['month_date'])
                base = base.merge(cyc5_df, on=['symbol', 'month_date'], how='left')
                filled = base[cyc5_needed[0]].notna().sum()
                log.info(f"  After CYC-005 join: {len(base):,} rows | {cyc5_needed[0]} fill={filled/len(base)*100:.0f}%")
            else:
                log.warning("  CYC-005 table not found in fund_db — components will be missing")
        except Exception as e:
            log.warning(f"  CYC-005 load error (non-fatal): {e}")

    # ── Step 6: Load quality scores (FCF Margin) ──────────────────────────────
    if quality_needed:
        log.info(f"\nStep 5/5: Loading PROD_OBQ_Quality_Scores: {quality_needed}")
        try:
            # cyc2_fcf_margin maps to fcf_margin in PROD_OBQ_Quality_Scores (mirror_db)
            q_df = mirror_con.execute(f"""
                SELECT
                    CASE WHEN symbol LIKE '%.US' THEN LEFT(symbol, LENGTH(symbol)-3) ELSE symbol END AS symbol,
                    month_date,
                    fcf_margin AS cyc2_fcf_margin
                FROM PROD_OBQ_Quality_Scores
                WHERE month_date IN ('{dates_str}')
            """).df()
            q_df['month_date'] = pd.to_datetime(q_df['month_date'])
            base = base.merge(q_df, on=['symbol', 'month_date'], how='left')
            log.info(f"  After quality join: {len(base):,} rows")
        except Exception as e:
            log.warning(f"  Quality scores load error (non-fatal): {e}")

    mirror_con.close()

    # ── Step 7: Build composites ───────────────────────────────────────────────
    log.info(f"\nBuilding {len(CYC007_COMPOSITES)} composite scores (rank-based equal weight)...")
    comp_df = _build_composites(base, {})

    # ── Assemble output ────────────────────────────────────────────────────────
    meta_cols = ['symbol', 'month_date', 'gic_sector']
    out_df = pd.concat([base[meta_cols].reset_index(drop=True), comp_df.reset_index(drop=True)], axis=1)
    out_df['month_date'] = pd.to_datetime(out_df['month_date']).dt.date

    log.info(f"\nOutput: {len(out_df):,} rows | {out_df['symbol'].nunique():,} unique symbols")

    # ── Write to DB ────────────────────────────────────────────────────────────
    if dry_run:
        log.info(f"\n  DRY RUN — {len(out_df):,} rows computed, not written")
        log.info(f"\n  Sample (first 3):\n{out_df.head(3).to_string()}")
    else:
        fund_con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")
        if overwrite:
            fund_con.execute(f"DROP TABLE IF EXISTS {OUTPUT_SCHEMA}.{OUTPUT_TABLE}")
        fund_con.execute(DDL_CREATE)
        fund_con.register('_out', out_df)
        inserted = fund_con.execute(f"""
            INSERT OR REPLACE INTO {OUTPUT_SCHEMA}.{OUTPUT_TABLE}
            SELECT * FROM _out
            WHERE symbol IS NOT NULL AND month_date IS NOT NULL
        """).rowcount
        log.info(f"\n  Written {inserted:,} rows to {OUTPUT_SCHEMA}.{OUTPUT_TABLE}")

    if not dry_run:
        fund_con.close()

    elapsed = time.time() - t_total
    log.info(f"\nCYC-007 score pipeline complete: {elapsed:.1f}s")
    return {
        "rows":     len(out_df),
        "symbols":  out_df['symbol'].nunique(),
        "elapsed_s": elapsed,
        "composites": len(CYC007_COMPOSITES),
    }


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='CYC-007 sector composite score builder')
    ap.add_argument('--dry-run', action='store_true', help='Compute but do not write to DB')
    ap.add_argument('--start',   default=START_DATE)
    ap.add_argument('--end',     default=END_DATE)
    args = ap.parse_args()
    result = run_score_pipeline(start_date=args.start, end_date=args.end, dry_run=args.dry_run)
    print(f"\nDone: {result['rows']:,} rows | {result['symbols']:,} symbols | "
          f"{result['composites']} composites | {result['elapsed_s']:.1f}s")
