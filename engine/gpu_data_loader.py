# -*- coding: utf-8 -*-
"""
engine/gpu_data_loader.py
=========================
GPU Factor Engine — Data Loader

ONE DuckDB query loads ALL data for ALL 326 factor runs into flat numpy arrays,
then transfers to GPU VRAM where it stays for the entire batch.

Design:
  - Single query joins all score tables + prices at semi-annual rebalance dates
  - Pads to (n_periods, max_stocks) uniform shape — NaN = invalid/absent
  - Transfers everything to GPU VRAM in a single batch
  - Score columns dict: all 91 factors pre-loaded as (n_periods, max_stocks) cp arrays
  - VRAM footprint: ~820MB (3.4% of 24GB on RTX 3090)

Usage:
    from engine.gpu_data_loader import load_all_data
    pack = load_all_data(mirror_db, fund_db, '1995-03-31', '2024-12-31', 'semi-annual')
    # pack.score_columns['jcn_full_composite']  # cp.ndarray (n_periods, max_stocks)
    # pack.returns_gpu                           # cp.ndarray (n_periods, max_stocks)
"""
from __future__ import annotations

import os
import math
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import duckdb

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import cupy as cp

log = logging.getLogger(__name__)

# ── Database locations ────────────────────────────────────────────────────────
MIRROR_DB = os.environ.get(
    "OBQ_EODHD_MIRROR_DB",
    r"D:/OBQ_AI/obq_eodhd_mirror.duckdb"
)

# ── Score column registry ─────────────────────────────────────────────────────
# CYC-001 composites from v_backtest_scores
CYC001_SCORE_COLS = [
    ('jcn_full_composite',         'higher_better', 'v_backtest_scores'),
    ('jcn_qarp',                   'higher_better', 'v_backtest_scores'),
    ('jcn_garp',                   'higher_better', 'v_backtest_scores'),
    ('jcn_quality_momentum',       'higher_better', 'v_backtest_scores'),
    ('jcn_value_momentum',         'higher_better', 'v_backtest_scores'),
    ('jcn_growth_quality_momentum','higher_better', 'v_backtest_scores'),
    ('jcn_fortress',               'higher_better', 'v_backtest_scores'),
    ('jcn_alpha_trifecta',         'higher_better', 'v_backtest_scores'),
    ('quality_score',              'higher_better', 'v_backtest_scores'),
    ('quality_score_universe',     'higher_better', 'v_backtest_scores'),
    ('value_score',                'higher_better', 'v_backtest_scores'),
    ('value_score_universe',       'higher_better', 'v_backtest_scores'),
    ('growth_score',               'higher_better', 'v_backtest_scores'),
    ('growth_score_universe',      'higher_better', 'v_backtest_scores'),
    ('finstr_score',               'higher_better', 'v_backtest_scores'),
    ('finstr_score_universe',      'higher_better', 'v_backtest_scores'),
    ('momentum_score',             'higher_better', 'v_backtest_scores'),
    ('momentum_sys_score',         'higher_better', 'v_backtest_scores'),
    ('momentum_af_score',          'higher_better', 'v_backtest_scores'),
    ('momentum_fip_score',         'higher_better', 'v_backtest_scores'),
    ('af_universe_score',          'higher_better', 'v_backtest_scores'),
]

# Separate-table scores (CYC-001 non-composite)
SEPARATE_TABLE_COLS = [
    ('moat_score',        'higher_better', 'PROD_MOAT_SCORES',        'moat_score',  False),
    ('moat_rank',         'lower_better',  'PROD_MOAT_SCORES',        'moat_rank',   False),
    ('fundsmith_rank',    'lower_better',  'PROD_FUNDSMITH_SCORES',   'fundsmith_rank', False),
    ('longeq_rank',       'higher_better', 'PROD_LONGEQ_SCORES',      'longeq_rank', False),
    ('rulebreaker_rank',  'lower_better',  'PROD_RULEBREAKER_SCORES', 'rulebreaker_rank', False),
]

# CYC-002 single factors (table, col, direction)
CYC002_TABLE_COLS = [
    # GROUP A: PROFITABILITY
    ("cyc2_roic",         "PROD_OBQ_Quality_Scores",  "roic",              "higher_better"),
    ("cyc2_cash_roc",     "PROD_LONGEQ_SCORES",       "cash_roc",          "higher_better"),
    ("cyc2_roce",         "PROD_FUNDSMITH_SCORES",    "roce",              "higher_better"),
    ("cyc2_roa",          "PROD_OBQ_Quality_Scores",  "roa",               "higher_better"),
    ("cyc2_gpa",          "PROD_OBQ_Quality_Scores",  "gpa",               "higher_better"),
    ("cyc2_op_margin",    "PROD_OBQ_Quality_Scores",  "op_margin",         "higher_better"),
    ("cyc2_fcf_margin",   "PROD_OBQ_Quality_Scores",  "fcf_margin",        "higher_better"),
    ("cyc2_gross_margin", "PROD_OBQ_Quality_Scores",  "gross_margin",      "higher_better"),
    ("cyc2_earn_quality", "PROD_OBQ_Quality_Scores",  "earnings_quality",  "higher_better"),
    # GROUP B: FINANCIAL STRENGTH
    ("cyc2_int_cov",      "PROD_OBQ_FinStr_Scores",   "interest_coverage", "higher_better"),
    ("cyc2_fcf_debt",     "PROD_OBQ_FinStr_Scores",   "fcf_debt",          "higher_better"),
    ("cyc2_nd_ebitda",    "PROD_OBQ_FinStr_Scores",   "net_debt_ebitda",   "lower_better"),
    ("cyc2_debt_assets",  "PROD_OBQ_FinStr_Scores",   "debt_assets",       "lower_better"),
    ("cyc2_cash_assets",  "PROD_OBQ_FinStr_Scores",   "cash_assets",       "higher_better"),
    ("cyc2_int_pct_op",   "PROD_LONGEQ_SCORES",       "interest_pct_op",   "lower_better"),
    ("cyc2_capex_ocf",    "PROD_LONGEQ_SCORES",       "capex_pct_ocf",     "lower_better"),
    ("cyc2_share_chg",    "PROD_LONGEQ_SCORES",       "sharecount_chg_5yr","lower_better"),
    ("cyc2_cash_conv",    "PROD_FUNDSMITH_SCORES",    "cash_conversion",   "higher_better"),
    ("cyc2_nd_ebit",      "PROD_FUNDSMITH_SCORES",    "net_debt_ebit",     "lower_better"),
    # GROUP C: VALUATION
    ("cyc2_ev_ebitda",    "PROD_OBQ_Value_Scores",    "ev_ebitda_ttm",     "lower_better"),
    ("cyc2_pfcf",         "PROD_OBQ_Value_Scores",    "pfcf_ttm",          "lower_better"),
    ("cyc2_fcf_yield",    "PROD_LONGEQ_SCORES",       "fcf_yield",         "higher_better"),
    ("cyc2_ps",           "PROD_OBQ_Value_Scores",    "ps_ttm",            "lower_better"),
    ("cyc2_pb",           "PROD_OBQ_Value_Scores",    "pb_mrq",            "lower_better"),
    ("cyc2_pe",           "PROD_OBQ_Value_Scores",    "pe_ttm",            "lower_better"),
    ("cyc2_rev_growth_3y","PROD_FUNDSMITH_SCORES",    "revenue_growth_3yr","higher_better"),
    # GROUP D: GROWTH
    ("cyc2_rev_cagr_1y",  "PROD_OBQ_Growth_Scores",  "revenue_ps_cagr_1y","higher_better"),
    ("cyc2_rev_cagr_3y",  "PROD_OBQ_Growth_Scores",  "revenue_ps_cagr_3y","higher_better"),
    ("cyc2_rev_cagr_5y",  "PROD_OBQ_Growth_Scores",  "revenue_ps_cagr_5y","higher_better"),
    ("cyc2_eps_cagr_1y",  "PROD_OBQ_Growth_Scores",  "eps_cagr_1y",       "higher_better"),
    ("cyc2_eps_cagr_3y",  "PROD_OBQ_Growth_Scores",  "eps_cagr_3y",       "higher_better"),
    ("cyc2_eps_cagr_5y",  "PROD_OBQ_Growth_Scores",  "eps_cagr_5y",       "higher_better"),
    ("cyc2_fcf_cagr_3y",  "PROD_OBQ_Growth_Scores",  "fcf_ps_cagr_3y",    "higher_better"),
    ("cyc2_fcf_cagr_5y",  "PROD_LONGEQ_SCORES",      "fcf_cagr_5yr",      "higher_better"),
    # GROUP E: MOMENTUM
    ("cyc2_mom_3m",        "PROD_OBQ_Momentum_Scores","af_r3m",            "higher_better"),
    ("cyc2_mom_6m",        "PROD_OBQ_Momentum_Scores","af_r6m",            "higher_better"),
    ("cyc2_mom_12m",       "PROD_OBQ_Momentum_Scores","af_r12m",           "higher_better"),
    ("cyc2_fip_6m",        "PROD_OBQ_Momentum_Scores","fip_6m",            "higher_better"),
    ("cyc2_fip_12m",       "PROD_OBQ_Momentum_Scores","fip_12m",           "higher_better"),
    ("cyc2_sys_score",     "PROD_OBQ_Momentum_Scores","systemscore",       "higher_better"),
    # GROUP F: CAPITAL ALLOCATION
    ("cyc2_rd_ratio",      "PROD_MOAT_SCORES",         "rd_ratio",          "higher_better"),
    # GROUP G: MOAT COMPONENTS
    ("cyc2_moat_intangible","PROD_MOAT_SCORES",        "score_intangible",  "higher_better"),
    ("cyc2_moat_switching", "PROD_MOAT_SCORES",        "score_switching",   "higher_better"),
    ("cyc2_moat_network",   "PROD_MOAT_SCORES",        "score_network",     "higher_better"),
    ("cyc2_moat_cost",      "PROD_MOAT_SCORES",        "score_cost",        "higher_better"),
    ("cyc2_moat_scale",     "PROD_MOAT_SCORES",        "score_scale",       "higher_better"),
]

# CYC-002 combo pairs (computed on GPU from pairs of singles)
CYC002_COMBOS_DEF = [
    ("T1", "cyc2_ev_ebitda",   "cyc2_cash_roc"),
    ("T2", "cyc2_ev_ebitda",   "cyc2_roic"),
    ("T3", "cyc2_pfcf",        "cyc2_roic"),
    ("T4", "cyc2_ps",          "cyc2_fcf_margin"),
    ("T5", "cyc2_ev_ebitda",   "cyc2_int_cov"),
    ("T6", "cyc2_ev_ebitda",   "cyc2_op_margin"),
    ("T7", "cyc2_fcf_yield",   "cyc2_nd_ebitda"),
    ("T8", "cyc2_ev_ebitda",   "cyc2_earn_quality"),
    ("O1", "cyc2_ps",          "cyc2_mom_12m"),
    ("O2", "cyc2_ev_ebitda",   "cyc2_mom_12m"),
    ("O3", "cyc2_ps",          "cyc2_roa"),
    ("O4", "cyc2_fcf_yield",   "cyc2_mom_12m"),
    ("O5", "cyc2_ev_ebitda",   "cyc2_fip_12m"),
    ("L1", "cyc2_roic",        "cyc2_mom_12m"),
    ("L2", "cyc2_gpa",         "cyc2_pb"),
    ("L3", "cyc2_int_cov",     "cyc2_ev_ebitda"),
    ("L4", "cyc2_fcf_margin",  "cyc2_eps_cagr_3y"),
    ("L5", "cyc2_op_margin",   "cyc2_rev_cagr_3y"),
    ("L6", "cyc2_roic",        "cyc2_debt_assets"),
]


# ── GPUDataPack dataclass ─────────────────────────────────────────────────────

@dataclass
class GPUDataPack:
    """All data needed for 326 factor runs, living in VRAM."""
    # Core arrays in VRAM
    returns_gpu:    "cp.ndarray"   # (n_periods, max_stocks) float64 — 6mo fwd returns
    market_cap_gpu: "cp.ndarray"   # (n_periods, max_stocks) float64 — USD
    sector_gpu:     "cp.ndarray"   # (n_periods, max_stocks) int8 — GICS sector ID
    valid_gpu:      "cp.ndarray"   # (n_periods, max_stocks) bool
    n_valid:        "cp.ndarray"   # (n_periods,) int32 — stock count per period

    # Pre-loaded score columns: factor_id -> (n_periods, max_stocks) float64 in VRAM
    score_columns:  dict = field(default_factory=dict)

    # Direction registry: factor_id -> 'higher_better' | 'lower_better'
    directions:     dict = field(default_factory=dict)

    # CPU-side metadata
    symbols:        list = field(default_factory=list)   # list[list[str]] per period
    dates:          list = field(default_factory=list)   # list[str] semi-annual dates
    symbol_index:   dict = field(default_factory=dict)   # date -> {symbol: col_idx}

    # Geometry
    n_periods:  int = 0
    max_stocks: int = 0

    # Timing
    load_seconds: float = 0.0
    vram_mb:      float = 0.0

    def cap_mask(self, tier: str) -> "cp.ndarray":
        """
        Build a boolean cap-tier mask from market_cap_gpu.
        tier: 'all' | 'micro' | 'small' | 'mid' | 'large' | 'mega'
        Returns cp.ndarray (n_periods, max_stocks) bool
        """
        CAP_RANGES = {
            'all':   (0,      9e18),
            'micro': (0,      300e6),
            'small': (300e6,  2e9),
            'mid':   (2e9,    10e9),
            'large': (10e9,   200e9),
            'mega':  (200e9,  9e18),
        }
        lo, hi = CAP_RANGES.get(tier, (0, 9e18))
        return (self.market_cap_gpu >= lo) & (self.market_cap_gpu <= hi)

    def gpu_status(self) -> str:
        dev = cp.cuda.Device()
        free, total = dev.mem_info
        return (f"VRAM {free/1e9:.1f}/{total/1e9:.1f}GB free | "
                f"Pack: {self.n_periods}p × {self.max_stocks}s | "
                f"{len(self.score_columns)} scores | {self.vram_mb:.0f}MB loaded")


# ── GICS sector string → integer mapping ──────────────────────────────────────
_SECTOR_MAP = {
    'Energy': 1, 'Materials': 2, 'Industrials': 3,
    'Consumer Discretionary': 4, 'Consumer Staples': 5,
    'Health Care': 6, 'Financials': 7, 'Information Technology': 8,
    'Communication Services': 9, 'Utilities': 10, 'Real Estate': 11,
    'Unknown': 0,
}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _detect_symbol_format(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Returns True if symbols in this table need '.US' appended for join."""
    try:
        row = con.execute(f"""
            SELECT symbol FROM {table}
            WHERE month_date = (SELECT MAX(month_date) FROM {table})
            LIMIT 1
        """).fetchone()
        if row:
            return not str(row[0]).endswith('.US')
    except Exception:
        pass
    return False


def _get_rebalance_dates(
    con: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
    rebal_freq: str = 'semi-annual',
    custom_months: Optional[list] = None,  # e.g. [1,7] for Jan+Jul semi-annual
) -> list[str]:
    """
    Return rebalance date strings for the requested frequency.

    custom_months: if provided, overrides rebal_freq — uses exactly these calendar months.
    rebal_freq presets:
      'monthly'     → all months
      'quarterly'   → 3,6,9,12
      'semi-annual' → 6,12  (baseline)
      'annual'      → 12
      'annual-q1'   → 3
      'annual-q2'   → 6
      'annual-q3'   → 9
      'annual-q4'   → 12
    """
    if custom_months:
        month_list = ','.join(str(m) for m in custom_months)
        freq_filter = f'MONTH(month_date) IN ({month_list})'
    else:
        freq_filter = {
            'monthly':     'TRUE',
            'quarterly':   'MONTH(month_date) IN (3,6,9,12)',
            'semi-annual': 'MONTH(month_date) IN (6,12)',
            'annual':      'MONTH(month_date) = 12',
            'annual-q1':   'MONTH(month_date) = 3',
            'annual-q2':   'MONTH(month_date) = 6',
            'annual-q3':   'MONTH(month_date) = 9',
            'annual-q4':   'MONTH(month_date) = 12',
        }.get(rebal_freq, 'MONTH(month_date) IN (6,12)')

    rows = con.execute(f"""
        SELECT DISTINCT month_date::VARCHAR AS d
        FROM v_backtest_scores
        WHERE month_date >= '{start_date}'::DATE
          AND month_date <= '{end_date}'::DATE
          AND jcn_full_composite IS NOT NULL
          AND {freq_filter}
        ORDER BY d
    """).fetchall()
    return [r[0] for r in rows]


def _load_base_universe(
    con: duckdb.DuckDBPyConnection,
    dates: list,
    start_date: str,
    end_date: str,
    hold_months: int = 6,
    min_price: float = 5.0,
    min_adv_usd: float = 1_000_000.0,
) -> dict:
    """
    Load universe (symbols per date) + forward returns + market cap + sector.

    Uses the same approach as the CPU engine (factor_backtest_cpu_legacy.py):
      - monthly_prices CTE keyed by (symbol, month_start)
      - entry = last close of the rebalance month
      - exit  = last close of month + hold_months
      - sector from v_backtest_scores LEFT JOIN
      - No ADV filter (volume is NULL for most rows in this DB)

    Returns: {date_str: {symbol: {fwd_return, market_cap, sector}}}
    """
    # Build month-start dates (DATE_TRUNC results) for the entry filter
    # Rebal dates are month-end (e.g. '1995-06-30'), DATE_TRUNC → '1995-06-01'
    entry_month_starts = list({d[:7] + '-01' for d in dates})  # deduplicated
    entry_months_sql = ",".join(f"'{m}'::DATE" for m in sorted(entry_month_starts))

    # Build a date lookup: month_start_str -> rebal_date_str
    month_start_to_rebal: dict = {}
    for d in dates:
        ms = d[:7] + '-01'
        month_start_to_rebal[ms] = d

    sql = f"""
    WITH monthly_prices AS (
        SELECT
            symbol,
            DATE_TRUNC('month', price_date)             AS month_start,
            LAST(adjusted_close ORDER BY price_date)    AS close_price,
            LAST(market_cap     ORDER BY price_date)    AS market_cap
        FROM v_backtest_prices
        WHERE price_date >= '{start_date}'::DATE - INTERVAL '2 months'
          AND price_date <= '{end_date}'::DATE   + INTERVAL '{hold_months + 2} months'
          AND adjusted_close IS NOT NULL AND adjusted_close > 0
          AND market_cap IS NOT NULL
        GROUP BY symbol, DATE_TRUNC('month', price_date)
    ),
    entry_prices AS (
        SELECT
            symbol,
            month_start                     AS entry_month,
            close_price                     AS entry_close,
            market_cap
        FROM monthly_prices
        WHERE month_start IN ({entry_months_sql})
          AND close_price >= {min_price}
    ),
    joined AS (
        SELECT
            e.symbol,
            e.entry_month::VARCHAR          AS entry_month_str,
            e.market_cap,
            (x.close_price / NULLIF(e.entry_close, 0) - 1) AS fwd_return  -- gross return, no embedded cost
        FROM entry_prices e
        JOIN monthly_prices x
            ON  e.symbol     = x.symbol
            AND x.month_start = e.entry_month + INTERVAL '{hold_months} months'
        WHERE x.close_price IS NOT NULL
          AND e.entry_close  > 0
          AND (x.close_price / e.entry_close - 1) BETWEEN -0.95 AND 3.0
    )
    SELECT
        j.symbol,
        j.entry_month_str,
        j.fwd_return,
        j.market_cap,
        COALESCE(vs.gic_sector, 'Unknown') AS sector
    FROM joined j
    LEFT JOIN v_backtest_scores vs
        ON  j.symbol = vs.symbol
        AND DATE_TRUNC('month', vs.month_date) = j.entry_month_str::DATE
    ORDER BY j.entry_month_str, j.symbol
    """

    t0 = time.time()
    rows = con.execute(sql).fetchall()
    log.info(f"Base universe query: {len(rows):,} rows in {time.time()-t0:.1f}s")

    # Build dict: rebal_date -> {symbol: {fwd_return, market_cap, sector}}
    universe: dict = {}
    for symbol, month_str, fwd_ret, mktcap, sector in rows:
        # month_str is '2015-06-01' (month_start from DATE_TRUNC)
        # Map back to rebalance date ('2015-06-30')
        date_key = month_start_to_rebal.get(month_str)
        if date_key is None:
            # Fallback: search by year-month prefix
            ym = month_str[:7]
            for d in dates:
                if d[:7] == ym:
                    date_key = d
                    break
        if date_key is None:
            continue

        if date_key not in universe:
            universe[date_key] = {}
        universe[date_key][symbol] = {
            'fwd_return': float(fwd_ret) if fwd_ret is not None else None,
            'market_cap': float(mktcap)  if mktcap is not None else 0.0,
            'sector':     str(sector)    if sector  else 'Unknown',
        }

    return universe


def _load_cyc001_scores(
    con: duckdb.DuckDBPyConnection,
    dates: list[str],
    score_cols: list[tuple],
) -> dict:
    """
    Load CYC-001 composites from v_backtest_scores in one query.
    Returns: {score_col: {date: {symbol: float}}}
    """
    col_names = [sc for sc, _, _ in score_cols]
    cols_sql = ", ".join(col_names)
    dates_sql = ",".join(f"'{d}'" for d in dates)

    rows = con.execute(f"""
        SELECT symbol, month_date::VARCHAR, {cols_sql}
        FROM v_backtest_scores
        WHERE month_date IN ({dates_sql})
          AND ({" OR ".join(f"{c} IS NOT NULL" for c in col_names)})
        ORDER BY month_date, symbol
    """).fetchall()

    # Build nested dict
    result = {sc: {} for sc in col_names}
    for row in rows:
        sym = row[0]
        date_str = row[1]
        for i, sc in enumerate(col_names):
            val = row[2 + i]
            if val is not None:
                if date_str not in result[sc]:
                    result[sc][date_str] = {}
                result[sc][date_str][sym] = float(val)

    return result


def _load_separate_table_scores(
    con: duckdb.DuckDBPyConnection,
    dates: list[str],
    col_defs: list[tuple],  # (factor_id, direction, table, col_in_table, needs_check)
) -> dict:
    """
    Load each separate-table score column into {date: {symbol: float}}.
    Groups by source table for efficiency.
    Returns: {factor_id: {date: {symbol: float}}}
    """
    # Group by source table
    by_table: dict = {}
    for factor_id, direction, table, col_name, _ in col_defs:
        if table not in by_table:
            by_table[table] = []
        by_table[table].append((factor_id, col_name))

    result = {}
    dates_sql = ",".join(f"'{d}'" for d in dates)

    for table, cols in by_table.items():
        col_names_sql = ", ".join(cn for _, cn in cols)

        # Detect if symbols need .US suffix
        needs_suffix = _detect_symbol_format(con, table)
        sym_expr = "CONCAT(symbol, '.US')" if needs_suffix else "symbol"

        try:
            rows = con.execute(f"""
                SELECT {sym_expr} AS sym, month_date::VARCHAR,
                       {col_names_sql}
                FROM {table}
                WHERE month_date IN ({dates_sql})
                  AND ({" OR ".join(f"{cn} IS NOT NULL" for _, cn in cols)})
                ORDER BY month_date, symbol
            """).fetchall()

            for row in rows:
                sym = row[0]
                date_str = row[1]
                for i, (factor_id, _) in enumerate(cols):
                    val = row[2 + i]
                    if val is not None:
                        if factor_id not in result:
                            result[factor_id] = {}
                        if date_str not in result[factor_id]:
                            result[factor_id][date_str] = {}
                        result[factor_id][date_str][sym] = float(val)

        except Exception as e:
            log.warning(f"Failed to load {table}: {e}")

    return result


def _load_cyc002_single_scores(
    con: duckdb.DuckDBPyConnection,
    dates: list[str],
    col_defs: list[tuple],  # (factor_id, table, col_in_table, direction)
) -> dict:
    """
    Load CYC-002 singles from PROD_* tables.
    Groups by source table for efficiency.
    Returns: {factor_id: {date: {symbol: float}}}
    """
    # Group by source table
    by_table: dict = {}
    for factor_id, table, col_name, direction in col_defs:
        if table not in by_table:
            by_table[table] = []
        by_table[table].append((factor_id, col_name))

    result = {}
    dates_sql = ",".join(f"'{d}'" for d in dates)

    for table, cols in by_table.items():
        col_names_sql = ", ".join(cn for _, cn in cols)
        needs_suffix = _detect_symbol_format(con, table)
        sym_expr = "CONCAT(symbol, '.US')" if needs_suffix else "symbol"

        try:
            rows = con.execute(f"""
                SELECT {sym_expr} AS sym, month_date::VARCHAR,
                       {col_names_sql}
                FROM {table}
                WHERE month_date IN ({dates_sql})
                  AND ({" OR ".join(f"{cn} IS NOT NULL" for _, cn in cols)})
                ORDER BY month_date, symbol
            """).fetchall()

            for row in rows:
                sym = row[0]
                date_str = row[1]
                for i, (factor_id, _) in enumerate(cols):
                    val = row[2 + i]
                    if val is not None:
                        if factor_id not in result:
                            result[factor_id] = {}
                        if date_str not in result[factor_id]:
                            result[factor_id][date_str] = {}
                        result[factor_id][date_str][sym] = float(val)

        except Exception as e:
            log.warning(f"Failed to load {table}: {e}")

    return result


def _pack_to_matrix(
    dates: list[str],
    universe: dict,
    score_data: dict,
    all_factor_ids: list[str],
    directions: dict,
) -> GPUDataPack:
    """
    Convert dicts to padded 2D numpy arrays, then upload to GPU VRAM.

    universe: {date: {symbol: {fwd_return, market_cap, sector}}}
    score_data: {factor_id: {date: {symbol: float}}}

    Returns: GPUDataPack with everything in VRAM.
    """
    t0 = time.time()

    # Build global symbol list per period & max_stocks
    symbols_per_period: list[list[str]] = []
    for d in dates:
        syms = sorted(universe.get(d, {}).keys())
        symbols_per_period.append(syms)

    max_stocks = max((len(s) for s in symbols_per_period), default=0)
    n_periods = len(dates)

    if max_stocks == 0:
        raise ValueError("No universe data found — check DB connection and date range")

    log.info(f"Packing: {n_periods} periods × {max_stocks} max_stocks")

    # Allocate CPU arrays
    returns_np    = np.full((n_periods, max_stocks), np.nan, dtype=np.float64)
    market_cap_np = np.full((n_periods, max_stocks), np.nan, dtype=np.float64)
    sector_np     = np.zeros((n_periods, max_stocks), dtype=np.int8)
    valid_np      = np.zeros((n_periods, max_stocks), dtype=bool)
    n_valid_np    = np.zeros(n_periods, dtype=np.int32)

    symbol_index = {}  # date -> {symbol: col_idx}

    for i, d in enumerate(dates):
        syms = symbols_per_period[i]
        idx_map = {sym: j for j, sym in enumerate(syms)}
        symbol_index[d] = idx_map

        udata = universe.get(d, {})
        for j, sym in enumerate(syms):
            sdata = udata.get(sym, {})
            fwd = sdata.get('fwd_return')
            mc  = sdata.get('market_cap', 0.0)
            sec = sdata.get('sector', 'Unknown')

            if fwd is not None and np.isfinite(fwd):
                returns_np[i, j] = fwd
                market_cap_np[i, j] = float(mc) if mc else 0.0
                sector_np[i, j] = _SECTOR_MAP.get(sec, 0)
                valid_np[i, j] = True

        n_valid_np[i] = int(valid_np[i].sum())

    log.info(f"CPU arrays built in {time.time()-t0:.1f}s. Avg valid/period: {n_valid_np.mean():.0f}")

    # Build score matrices — all NaN-filled, fill where data exists
    score_matrices: dict = {}
    for factor_id in all_factor_ids:
        mat = np.full((n_periods, max_stocks), np.nan, dtype=np.float64)
        fdata = score_data.get(factor_id, {})
        for i, d in enumerate(dates):
            idx_map = symbol_index[d]
            date_scores = fdata.get(d, {})
            for sym, val in date_scores.items():
                j = idx_map.get(sym)
                if j is not None and np.isfinite(val):
                    mat[i, j] = val
        score_matrices[factor_id] = mat

    log.info(f"Score matrices built in {time.time()-t0:.1f}s. Uploading to GPU...")

    # Upload to GPU VRAM
    t_upload = time.time()
    returns_gpu    = cp.asarray(returns_np)
    market_cap_gpu = cp.asarray(market_cap_np)
    sector_gpu     = cp.asarray(sector_np)
    valid_gpu      = cp.asarray(valid_np)
    n_valid_gpu    = cp.asarray(n_valid_np)

    score_cols_gpu: dict = {}
    for factor_id, mat in score_matrices.items():
        score_cols_gpu[factor_id] = cp.asarray(mat)

    cp.cuda.Stream.null.synchronize()

    # Compute VRAM usage
    total_bytes = (
        returns_gpu.nbytes + market_cap_gpu.nbytes + sector_gpu.nbytes +
        valid_gpu.nbytes + n_valid_gpu.nbytes +
        sum(v.nbytes for v in score_cols_gpu.values())
    )
    vram_mb = total_bytes / 1e6

    elapsed = time.time() - t0
    log.info(f"GPU upload complete: {vram_mb:.0f}MB in {time.time()-t_upload:.1f}s | Total load: {elapsed:.1f}s")

    # Build direction registry
    dir_registry = {}
    for sc, direction, _ in CYC001_SCORE_COLS:
        dir_registry[sc] = direction
    for factor_id, direction, table, col_name, _ in SEPARATE_TABLE_COLS:
        dir_registry[factor_id] = direction
    for factor_id, table, col_name, direction in CYC002_TABLE_COLS:
        dir_registry[factor_id] = direction

    return GPUDataPack(
        returns_gpu=returns_gpu,
        market_cap_gpu=market_cap_gpu,
        sector_gpu=sector_gpu,
        valid_gpu=valid_gpu,
        n_valid=n_valid_gpu,
        score_columns=score_cols_gpu,
        directions=dir_registry,
        symbols=symbols_per_period,
        dates=dates,
        symbol_index=symbol_index,
        n_periods=n_periods,
        max_stocks=max_stocks,
        load_seconds=elapsed,
        vram_mb=vram_mb,
    )


# ── Public API ────────────────────────────────────────────────────────────────

def load_all_data(
    mirror_db: str = MIRROR_DB,
    fund_db: Optional[str] = None,
    start_date: str = '1995-03-31',
    end_date: str = '2024-12-31',
    rebal_freq: str = 'semi-annual',
    hold_months: int = 6,
    min_price: float = 5.0,
    min_adv_usd: float = 1_000_000.0,
    custom_months: Optional[list] = None,  # override rebal_freq with exact month list
) -> GPUDataPack:
    """
    ONE DuckDB connection loads ALL data for ALL 326 factor runs into VRAM.

    Steps:
      1. Get semi-annual rebalance dates
      2. Load base universe (returns + market_cap + sector) for all dates
      3. Load CYC-001 composite scores from v_backtest_scores (one query)
      4. Load separate-table scores (moat, fundsmith, longeq, rulebreaker) (grouped by table)
      5. Load CYC-002 singles from PROD_* tables (grouped by table)
      6. Pack all data into (n_periods, max_stocks) numpy arrays
      7. Upload to GPU VRAM in one shot

    Returns: GPUDataPack with everything in VRAM, ready for gpu_batch_runner.
    """
    t0 = time.time()
    log.info(f"[DataLoader] Loading all data: {start_date} → {end_date} ({rebal_freq})")
    log.info(f"  DB: {mirror_db}")

    con = duckdb.connect(mirror_db, read_only=True)

    try:
        # Step 1: Rebalance dates
        dates = _get_rebalance_dates(con, start_date, end_date, rebal_freq,
                                     custom_months=custom_months)
        if len(dates) < 3:
            raise ValueError(f"Only {len(dates)} rebalance dates found — check DB and date range")
        log.info(f"  Rebalance dates: {len(dates)} ({dates[0]} → {dates[-1]})")

        # Step 2: Base universe
        log.info("  Loading base universe (returns + market_cap + sector)...")
        universe = _load_base_universe(
            con, dates, start_date, end_date,
            hold_months=hold_months,
            min_price=min_price,
            min_adv_usd=min_adv_usd,
        )
        log.info(f"  Universe: {sum(len(v) for v in universe.values()):,} stock×period records")

        # Step 3: CYC-001 composites
        log.info("  Loading CYC-001 composites from v_backtest_scores...")
        cyc001_scores = _load_cyc001_scores(con, dates, CYC001_SCORE_COLS)
        log.info(f"  CYC-001: {len(cyc001_scores)} score columns loaded")

        # Step 4: Separate-table scores
        log.info("  Loading separate-table scores (moat, fundsmith, longeq, rulebreaker)...")
        sep_scores = _load_separate_table_scores(con, dates, SEPARATE_TABLE_COLS)
        log.info(f"  Separate tables: {len(sep_scores)} score columns loaded")

        # Step 5: CYC-002 singles
        log.info("  Loading CYC-002 singles from PROD_* tables...")
        cyc002_scores = _load_cyc002_single_scores(con, dates, CYC002_TABLE_COLS)
        log.info(f"  CYC-002 singles: {len(cyc002_scores)} score columns loaded")

    finally:
        con.close()

    # Step 6 (optional): CYC-004 scores from fundamentals DB
    cyc004_scores: dict = {}
    fund_db_path = fund_db or os.environ.get("OBQ_FUND_DB", r"D:/OBQ_AI/obq_fundamentals.duckdb")
    try:
        fund_con = duckdb.connect(fund_db_path, read_only=True)
        # Check if CYC-004 table exists
        tbl_exists = fund_con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='scores' AND table_name='obq_cyc004_scores'
        """).fetchone()[0] > 0

        # Also check CYC-005 in the same DB
        tbl5_exists = fund_con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='scores' AND table_name='obq_cyc005_scores'
        """).fetchone()[0] > 0

        if tbl_exists:
            log.info("  Loading CYC-004 scores from obq_fundamentals.duckdb...")
            dates_sql = ",".join(f"'{d}'" for d in dates)

            # Get all factor columns (exclude meta)
            cyc4_cols_raw = fund_con.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='scores' AND table_name='obq_cyc004_scores'
                  AND column_name NOT IN ('symbol','month_date','as_of_filing_date','gic_sector')
                ORDER BY ordinal_position
            """).fetchall()
            cyc4_cols = [r[0] for r in cyc4_cols_raw]

            if cyc4_cols:
                cols_sql = ", ".join(cyc4_cols)
                # Direct IN query — CYC-004 stored at exact SA dates; append .US for VRAM key
                rows = fund_con.execute(f"""
                    SELECT
                        CASE WHEN symbol LIKE '%.US' THEN symbol
                             ELSE CONCAT(symbol, '.US') END AS symbol,
                        month_date::VARCHAR AS date_str,
                        {cols_sql}
                    FROM scores.obq_cyc004_scores
                    WHERE month_date IN ({dates_sql})
                    ORDER BY month_date, symbol
                """).fetchall()

                for row in rows:
                    sym = row[0]
                    date_str = row[1]
                    for i, fc in enumerate(cyc4_cols):
                        val = row[2 + i]
                        if val is not None and not (isinstance(val, float) and math.isnan(val)):
                            if fc not in cyc004_scores:
                                cyc004_scores[fc] = {}
                            if date_str not in cyc004_scores[fc]:
                                cyc004_scores[fc][date_str] = {}
                            cyc004_scores[fc][date_str][sym] = float(val)

                log.info(f"  CYC-004: {len(cyc004_scores)} score columns loaded")
            else:
                log.warning("  CYC-004 table exists but has no factor columns")
        else:
            log.info("  CYC-004 scores not yet computed — skipping")

        # CYC-005 scores (same load pattern)
        if tbl5_exists:
            log.info("  Loading CYC-005 sector scores from obq_fundamentals.duckdb...")
            cyc5_cols_raw = fund_con.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='scores' AND table_name='obq_cyc005_scores'
                  AND column_name NOT IN ('symbol','month_date','as_of_filing_date','gic_sector')
                ORDER BY ordinal_position
            """).fetchall()
            cyc5_cols = [r[0] for r in cyc5_cols_raw]
            if cyc5_cols:
                cols5_sql = ", ".join(cyc5_cols)
                # Direct IN query — CYC-005 stored at exact SA dates; append .US for VRAM key
                rows5 = fund_con.execute(f"""
                    SELECT
                        CASE WHEN symbol LIKE '%.US' THEN symbol
                             ELSE CONCAT(symbol, '.US') END AS symbol,
                        month_date::VARCHAR AS date_str,
                        {cols5_sql}
                    FROM scores.obq_cyc005_scores
                    WHERE month_date IN ({dates_sql})
                    ORDER BY month_date, symbol
                """).fetchall()
                for row in rows5:
                    sym = row[0]; date_str = row[1]
                    for i, fc in enumerate(cyc5_cols):
                        val = row[2 + i]
                        if val is not None and not (isinstance(val, float) and math.isnan(val)):
                            if fc not in cyc004_scores:
                                cyc004_scores[fc] = {}
                            if date_str not in cyc004_scores[fc]:
                                cyc004_scores[fc][date_str] = {}
                            cyc004_scores[fc][date_str][sym] = float(val)
                n5 = sum(1 for k in cyc004_scores if k.startswith('cyc5_'))
                log.info(f"  CYC-005: {n5} score columns loaded")
        else:
            log.info("  CYC-005 scores not yet computed — skipping")

        # CYC-007 composite scores (same load pattern)
        tbl7_exists = fund_con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='scores' AND table_name='obq_cyc007_scores'
        """).fetchone()[0] > 0

        if tbl7_exists:
            log.info("  Loading CYC-007 composite scores from obq_fundamentals.duckdb...")
            cyc7_cols_raw = fund_con.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='scores' AND table_name='obq_cyc007_scores'
                  AND column_name NOT IN ('symbol','month_date','gic_sector')
                ORDER BY ordinal_position
            """).fetchall()
            cyc7_cols = [r[0] for r in cyc7_cols_raw]
            if cyc7_cols:
                cols7_sql = ", ".join(cyc7_cols)
                # Direct IN query — CYC-007 stored at exact SA dates; append .US for VRAM key
                rows7 = fund_con.execute(f"""
                    SELECT
                        CASE WHEN symbol LIKE '%.US' THEN symbol
                             ELSE CONCAT(symbol, '.US') END AS symbol,
                        month_date::VARCHAR AS date_str,
                        {cols7_sql}
                    FROM scores.obq_cyc007_scores
                    WHERE month_date IN ({dates_sql})
                    ORDER BY month_date, symbol
                """).fetchall()
                for row in rows7:
                    sym = row[0]; date_str = row[1]
                    for i, fc in enumerate(cyc7_cols):
                        val = row[2 + i]
                        if val is not None and not (isinstance(val, float) and math.isnan(val)):
                            if fc not in cyc004_scores:
                                cyc004_scores[fc] = {}
                            if date_str not in cyc004_scores[fc]:
                                cyc004_scores[fc][date_str] = {}
                            cyc004_scores[fc][date_str][sym] = float(val)
                n7 = sum(1 for k in cyc004_scores if k.startswith('cyc7_'))
                log.info(f"  CYC-007: {n7} composite columns loaded")
        else:
            log.info("  CYC-007 scores not yet computed — skipping")

        fund_con.close()
    except Exception as e:
        log.warning(f"  CYC-004 load failed (non-fatal): {e}")

    # Merge all score dicts
    all_scores: dict = {}
    all_scores.update(cyc001_scores)
    all_scores.update(sep_scores)
    all_scores.update(cyc002_scores)
    all_scores.update(cyc004_scores)

    all_factor_ids = list(all_scores.keys())
    log.info(f"  Total score columns to pack: {len(all_factor_ids)}")

    # Step 6+7: Pack and upload to GPU
    pack = _pack_to_matrix(dates, universe, all_scores, all_factor_ids, {})

    elapsed = time.time() - t0
    pack.load_seconds = elapsed
    log.info(f"[DataLoader] COMPLETE: {elapsed:.1f}s | {pack.gpu_status()}")

    return pack


# ── Self-test / smoke test ────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    db = sys.argv[1] if len(sys.argv) > 1 else MIRROR_DB
    print(f"\n[DataLoader smoke test] DB: {db}")

    dev = cp.cuda.Device()
    free, total = dev.mem_info
    print(f"GPU: cc={dev.compute_capability}  VRAM {free/1e9:.1f}/{total/1e9:.1f}GB free")

    pack = load_all_data(
        mirror_db=db,
        start_date='1995-03-31',
        end_date='2024-12-31',
        rebal_freq='semi-annual',
    )

    print(f"\n{'='*60}")
    print(f"DataPack: {pack.n_periods} periods × {pack.max_stocks} max_stocks")
    print(f"Score columns in VRAM: {len(pack.score_columns)}")
    print(f"VRAM used: {pack.vram_mb:.0f}MB")
    print(f"Load time: {pack.load_seconds:.1f}s")

    # Verify a few columns
    for col in ['jcn_full_composite', 'cyc2_roic', 'cyc2_ps']:
        if col in pack.score_columns:
            arr = pack.score_columns[col]
            nans = float(cp.isnan(arr).mean()) * 100
            print(f"  {col}: shape={arr.shape} NaN={nans:.1f}%")

    # Cap mask test
    mask = pack.cap_mask('large')
    print(f"  large-cap mask: {float(mask.mean())*100:.1f}% of slots")

    print(pack.gpu_status())
