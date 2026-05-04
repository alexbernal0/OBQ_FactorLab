"""
engine/factor_backtest.py
=========================
OBQ FactorLab — Quintile Factor Backtest Engine

Pipeline:
  Universe → Prefilter → Score/Rank → NTILE buckets → Forward returns per bucket
  → IC, Monotonicity, Quintile CAGR, Sharpe per bucket → Factor Tearsheet data

Data sources (MotherDuck / local DuckDB mirror):
  v_backtest_scores   — point-in-time OBQ scores, monthly
  v_backtest_prices   — daily adjusted prices with market_cap, sector
  v_backtest_universe — OBQ investable universe membership
  v_backtest_filings  — PIT fundamentals (for raw factor values)

All date arithmetic is monthly-close to monthly-close.
"""

from __future__ import annotations

import os
import time
import math
import traceback
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
from dotenv import load_dotenv

load_dotenv()

# ── Database connection ────────────────────────────────────────────────────────
MIRROR_DB = os.environ.get(
    "OBQ_EODHD_MIRROR_DB",
    r"D:/OBQ_AI/obq_eodhd_mirror.duckdb"
)


def _get_con() -> duckdb.DuckDBPyConnection:
    """Open read-only connection to the EODHD mirror."""
    return duckdb.connect(MIRROR_DB, read_only=True)


# ── Configuration dataclass ────────────────────────────────────────────────────
@dataclass
class FactorBacktestConfig:
    # Score / factor selection
    score_column: str = "jcn_full_composite"      # column from v_backtest_scores
    score_direction: str = "higher_better"         # "higher_better" | "lower_better"
    score_level: str = "universe"                  # universe | sector | history | composite

    # Prefilter
    min_price: float = 5.0
    max_price: float = 10_000.0
    min_market_cap: float = 0.0                    # 0 = no filter
    min_adv_usd: float = 1_000_000.0              # avg daily value traded ($1M default)
    exclude_sectors: list = field(default_factory=list)
    us_listed_only: bool = True

    # Backtest
    n_buckets: int = 5
    start_date: str = "1990-07-31"
    end_date: str = "2026-03-31"
    hold_months: int = 6                           # forward return period
    rebalance_freq: str = "monthly"                # monthly | quarterly | semi-annual | annual
    transaction_cost_bps: float = 15.0

    # Cap tier filter
    cap_tier: str = "all"   # all | micro | small | mid | large | mega
    # micro <$300M, small $300M-2B, mid $2B-10B, large $10B-200B, mega >$200B

    # Label
    run_label: str = ""


# Cap tier market cap ranges (USD)
CAP_TIERS = {
    "all":   (0, float("inf")),
    "micro": (0, 300e6),
    "small": (300e6, 2e9),
    "mid":   (2e9, 10e9),
    "large": (10e9, 200e9),
    "mega":  (200e9, float("inf")),
}

# Available score columns with display names
SCORE_COLUMNS = {
    # OBQ Composite scores
    "jcn_full_composite":         "JCN Composite",
    "jcn_qarp":                   "JCN QARP",
    "jcn_garp":                   "JCN GARP",
    "jcn_quality_momentum":       "JCN Quality-Momentum",
    "jcn_value_momentum":         "JCN Value-Momentum",
    "jcn_growth_quality_momentum":"JCN Growth-Quality-Momentum",
    "jcn_fortress":               "JCN Fortress",
    "jcn_alpha_trifecta":         "JCN Alpha Trifecta",
    # Individual factor scores
    "value_score":                "Value Score",
    "quality_score":              "Quality Score",
    "growth_score":               "Growth Score",
    "momentum_score":             "Momentum Score",
    "finstr_score":               "Financial Strength",
    "momentum_af_score":          "Momentum (Alpha Factor)",
    "momentum_fip_score":         "Momentum (FIP)",
    "momentum_sys_score":         "Momentum (Systematic)",
    # Universe-normalized variants
    "value_score_universe":       "Value Score (Universe)",
    "quality_score_universe":     "Quality Score (Universe)",
    "growth_score_universe":      "Growth Score (Universe)",
    "finstr_score_universe":      "FinStr Score (Universe)",
    "af_universe_score":          "Alpha Factor (Universe)",
    # Separate-table scores (joined via SEPARATE_SCORE_TABLES)
    "longeq_rank":                "LongEQ Rank",
    "longeq_score":               "LongEQ Score",
    "rulebreaker_rank":           "Rulebreaker Rank",
    "rulebreaker_score":          "Rulebreaker Score",
    "fundsmith_rank":             "Fundsmith Rank",
    "fundsmith_score":            "Fundsmith Score",
    "moat_score":                 "Moat Score",
    "moat_rank":                  "Moat Rank",
}

# Scores that live in separate tables (not in v_backtest_scores)
# Map: score_column -> (source_table, score_direction)
SEPARATE_SCORE_TABLES = {
    "longeq_rank":      ("PROD_LONGEQ_SCORES",      "lower_better"),   # rank 1 = best
    "longeq_score":     ("PROD_LONGEQ_SCORES",       "higher_better"),  # 0-7 gate count
    "rulebreaker_rank": ("PROD_RULEBREAKER_SCORES",  "lower_better"),
    "rulebreaker_score":("PROD_RULEBREAKER_SCORES",  "higher_better"),
    "fundsmith_rank":   ("PROD_FUNDSMITH_SCORES",    "lower_better"),
    "fundsmith_score":  ("PROD_FUNDSMITH_SCORES",    "higher_better"),
    "moat_score":       ("PROD_MOAT_SCORES",         "higher_better"),
    "moat_rank":        ("PROD_MOAT_SCORES",         "lower_better"),
}


# ── Main backtest function ─────────────────────────────────────────────────────

def run_factor_backtest(
    cfg: FactorBacktestConfig,
    cb=None,
) -> dict:
    """
    Run a quintile factor backtest.

    Returns dict with:
      status, run_label, score_column, config
      dates               — list of rebalance dates (str)
      buckets             — list[int] 1..n_buckets
      bucket_returns      — {bucket: [monthly_rets]}  (decimal)
      bucket_equity       — {bucket: [cum equity starting 1.0]}
      bucket_metrics      — {bucket: {cagr, sharpe, max_dd, ...}}
      ic_data             — [{date, ic_value}]
      period_data         — [{date, bucket_returns dict}]
      quintile_bar        — [{bucket, cagr_excess}]
      factor_metrics      — full metric dict
      annual_ret_by_bucket— {bucket: [{year, ret}]}
      n_obs, n_stocks_avg, elapsed_s
    """
    t0 = time.time()

    def _cb(msg):
        if cb:
            try: cb("info", msg)
            except: pass

    try:
        _cb(f"Loading score data for {cfg.score_column}...")
        con = _get_con()

        # ── 1. Get rebalance dates ─────────────────────────────────────────────
        rebal_dates = _get_rebalance_dates(con, cfg)
        if len(rebal_dates) < 3:
            return {"status": "error", "error": f"Not enough rebalance dates: {len(rebal_dates)}"}

        _cb(f"Rebalance dates: {len(rebal_dates)} periods ({rebal_dates[0]} → {rebal_dates[-1]})")

        # ── 2. Load scores + prices for all dates in one query ─────────────────
        _cb("Querying scores and prices...")
        score_col = _resolve_score_column(cfg.score_column)

        df_scores = _load_scores(con, cfg, rebal_dates, score_col)
        if df_scores.empty:
            return {"status": "error", "error": f"No score data for {cfg.score_column}"}

        _cb(f"Loaded {len(df_scores):,} score records across {df_scores['month_date'].nunique()} months")

        # ── 3. Load forward returns ─────────────────────────────────────────────
        _cb("Computing forward returns...")
        df_fwd = _compute_forward_returns(con, cfg, rebal_dates)

        # ── 3b. Load Russell 3000 universe returns (point-in-time from Norgate) ──
        # This replaces the naive EW-all-scored-stocks benchmark with a proper
        # investable universe benchmark matching Tortoriello's S&P 1500 methodology.
        _cb("Loading Russell 3000 universe returns (Norgate, point-in-time)...")
        df_r3000 = _load_russell3000_universe_returns(con, cfg, rebal_dates)
        if df_r3000 is not None and not df_r3000.empty:
            _cb(f"  Russell 3000 universe: {len(df_r3000)} periods ({df_r3000['month_date'].min()} → {df_r3000['month_date'].max()})")
        else:
            _cb("  Russell 3000 data not available — falling back to scored-stock universe")
            df_r3000 = None

        con.close()

        # ── 4. Join scores → prefilter → rank → buckets ────────────────────────
        _cb("Ranking and bucketing...")
        results = _run_quintile_backtest(df_scores, df_fwd, cfg, rebal_dates, _cb,
                                          df_r3000=df_r3000)

        elapsed = round(time.time() - t0, 1)
        _cb(f"Factor backtest complete in {elapsed}s")
        results["elapsed_s"] = elapsed
        results["status"] = "complete"
        results["run_label"] = cfg.run_label or f"{cfg.score_column} | {cfg.n_buckets}Q | {cfg.hold_months}mo"
        results["config"] = {
            "score_column":   cfg.score_column,
            "n_buckets":      cfg.n_buckets,
            "hold_months":    cfg.hold_months,
            "start_date":     cfg.start_date,
            "end_date":       cfg.end_date,
            "min_price":      cfg.min_price,
            "min_adv_usd":    cfg.min_adv_usd,
            "cap_tier":       cfg.cap_tier,
            "rebalance_freq": cfg.rebalance_freq,
            "cost_bps":       cfg.transaction_cost_bps,
        }
        return results

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


# ── Internal helpers ────────────────────────────────────────────────────────────

def _get_rebalance_dates(con, cfg: FactorBacktestConfig) -> list[str]:
    """Get all score dates within the backtest window at the configured frequency."""
    freq_filter = {
        "monthly":     "TRUE",
        "quarterly":   "MONTH(month_date) IN (3,6,9,12)",
        "semi-annual": "MONTH(month_date) IN (6,12)",
        "annual":      "MONTH(month_date) = 12",
    }.get(cfg.rebalance_freq, "TRUE")

    # Use the appropriate source table
    if _is_separate_table(cfg.score_column):
        src_table = SEPARATE_SCORE_TABLES[cfg.score_column][0]
        score_col = cfg.score_column
    else:
        src_table = "v_backtest_scores"
        score_col = cfg.score_column

    rows = con.execute(f"""
        SELECT DISTINCT month_date::VARCHAR as d
        FROM {src_table}
        WHERE month_date >= '{cfg.start_date}'::DATE
          AND month_date <= '{cfg.end_date}'::DATE
          AND {score_col} IS NOT NULL
          AND {freq_filter}
        ORDER BY d
    """).fetchall()
    return [r[0] for r in rows]


def _resolve_score_column(score_col: str) -> str:
    """Map score column name to the actual column — just pass through."""
    return score_col


def _load_russell3000_universe_returns(
    con, cfg: FactorBacktestConfig, rebal_dates: list[str]
) -> pd.DataFrame | None:
    """
    Load equal-weighted Russell 3000 universe returns for each rebalance period.
    Uses PROD_Sector_Index_Membership (Norgate, point-in-time) for constituent membership
    and v_backtest_prices for actual price returns.

    Returns DataFrame with columns: [month_date, r3000_ret, n_members]
    or None if insufficient data.

    Russell 3000 is the closest available match to Tortoriello's S&P 1500 universe —
    it covers the top 3000 US stocks by market cap, point-in-time, survivorship-bias free.
    """
    if not rebal_dates:
        return None

    hold = cfg.hold_months
    dates_sql = ",".join(f"'{d}'" for d in rebal_dates)

    try:
        df = con.execute(f"""
            WITH r3000_members AS (
                -- Point-in-time Russell 3000 membership at each rebalance date
                SELECT
                    m.date::VARCHAR       AS month_date,
                    m.symbol
                FROM PROD_Sector_Index_Membership m
                WHERE m.in_russell_3000 = TRUE
                  AND m.date IN ({dates_sql})
            ),
            monthly_prices AS (
                -- Last price of each month for each symbol
                SELECT
                    symbol,
                    DATE_TRUNC('month', price_date) AS month_start,
                    LAST(adjusted_close ORDER BY price_date) AS close_price
                FROM v_backtest_prices
                WHERE price_date >= '{cfg.start_date}'::DATE - INTERVAL '2 months'
                  AND price_date <= '{cfg.end_date}'::DATE + INTERVAL '{hold + 1} months'
                  AND adjusted_close IS NOT NULL AND adjusted_close > 0
                GROUP BY symbol, DATE_TRUNC('month', price_date)
            ),
            period_returns AS (
                -- Forward return over hold_months for each Russell 3000 member
                SELECT
                    r.month_date,
                    r.symbol,
                    (t1.close_price / NULLIF(t0.close_price, 0) - 1) AS fwd_return
                FROM r3000_members r
                JOIN monthly_prices t0
                    ON r.symbol = t0.symbol
                    AND t0.month_start = DATE_TRUNC('month', r.month_date::DATE)
                JOIN monthly_prices t1
                    ON r.symbol = t1.symbol
                    AND t1.month_start = t0.month_start + INTERVAL '{hold} months'
                WHERE t0.close_price > 0 AND t1.close_price > 0
                  -- Sanity cap: exclude extreme data errors
                  AND (t1.close_price / t0.close_price - 1) BETWEEN -0.95 AND 3.0
            )
            SELECT
                month_date,
                AVG(fwd_return)  AS r3000_ret,
                COUNT(*)         AS n_members
            FROM period_returns
            GROUP BY month_date
            ORDER BY month_date
        """).fetchdf()

        if df.empty or len(df) < 3:
            return None

        return df

    except Exception as e:
        return None


def _is_separate_table(score_col: str) -> bool:
    """True if score lives in a separate table (not v_backtest_scores)."""
    return score_col in SEPARATE_SCORE_TABLES


def _load_scores(con, cfg: FactorBacktestConfig, rebal_dates: list[str], score_col: str) -> pd.DataFrame:
    """
    Load scores with prefilter applied. Handles both v_backtest_scores
    and separate factor tables (LongEQ, Rulebreaker, Fundsmith, Moat).
    """
    cap_lo, cap_hi = CAP_TIERS.get(cfg.cap_tier, (0, float("inf")))
    cap_hi_sql = f"{cap_hi}" if cap_hi != float("inf") else "9e18"
    sector_excl = ""
    if cfg.exclude_sectors:
        slist = ", ".join(f"'{s}'" for s in cfg.exclude_sectors)
        sector_excl = f"AND vs.gic_sector NOT IN ({slist})"

    effective_mktcap_min = max(cap_lo, cfg.min_adv_usd * 50, cfg.min_market_cap)
    dates_sql = ",".join(f"'{d}'" for d in rebal_dates)

    # Separate-table scores need a special JOIN
    if _is_separate_table(score_col):
        src_table = SEPARATE_SCORE_TABLES[score_col][0]
        sql = f"""
        WITH monthly_prices AS (
            SELECT
                symbol,
                DATE_TRUNC('month', price_date) AS month_start,
                LAST(adjusted_close ORDER BY price_date) AS close_price,
                LAST(market_cap     ORDER BY price_date) AS market_cap
            FROM v_backtest_prices
            WHERE price_date >= '{cfg.start_date}'::DATE - INTERVAL '2 months'
              AND price_date <= '{cfg.end_date}'::DATE   + INTERVAL '1 month'
              AND adjusted_close IS NOT NULL AND market_cap IS NOT NULL
            GROUP BY symbol, DATE_TRUNC('month', price_date)
        ),
        score_data AS (
            SELECT
                sc.symbol,
                sc.month_date::VARCHAR              AS month_date,
                COALESCE(vs.gic_sector, 'Unknown')  AS gic_sector,
                sc.{score_col}                      AS score_raw,
                p.close_price                       AS price,
                p.market_cap
            FROM {src_table} sc
            -- join price data
            JOIN monthly_prices p
                ON sc.symbol = p.symbol
                AND p.month_start = DATE_TRUNC('month', sc.month_date)
            -- join v_backtest_scores for sector info (LEFT JOIN — sector optional)
            LEFT JOIN v_backtest_scores vs
                ON sc.symbol = vs.symbol
                AND sc.month_date = vs.month_date
            WHERE sc.month_date IN ({dates_sql})
              AND sc.{score_col} IS NOT NULL
              AND p.close_price  >= {cfg.min_price}
              AND p.close_price  <= {cfg.max_price}
              AND p.market_cap   >= {effective_mktcap_min}
              AND p.market_cap   <= {cap_hi_sql}
              {sector_excl}
        )
        SELECT * FROM score_data
        ORDER BY month_date, symbol
        """
    else:
        # Standard v_backtest_scores join
        sql = f"""
        WITH monthly_prices AS (
            SELECT
                symbol,
                DATE_TRUNC('month', price_date) AS month_start,
                LAST(adjusted_close ORDER BY price_date) AS close_price,
                LAST(market_cap    ORDER BY price_date) AS market_cap
            FROM v_backtest_prices
            WHERE price_date >= '{cfg.start_date}'::DATE - INTERVAL '2 months'
              AND price_date <= '{cfg.end_date}'::DATE   + INTERVAL '1 month'
              AND adjusted_close IS NOT NULL
              AND market_cap IS NOT NULL
            GROUP BY symbol, DATE_TRUNC('month', price_date)
        ),
        score_data AS (
            SELECT
                s.symbol,
                s.month_date::VARCHAR                   AS month_date,
                s.gic_sector,
                s.{score_col}                           AS score_raw,
                p.close_price                           AS price,
                p.market_cap
            FROM v_backtest_scores s
            JOIN monthly_prices p
                ON s.symbol = p.symbol
                AND p.month_start = DATE_TRUNC('month', s.month_date)
            WHERE s.month_date IN ({dates_sql})
              AND s.{score_col} IS NOT NULL
              AND p.close_price  >= {cfg.min_price}
              AND p.close_price  <= {cfg.max_price}
              AND p.market_cap   >= {effective_mktcap_min}
              AND p.market_cap   <= {cap_hi_sql}
              {sector_excl}
        )
        SELECT * FROM score_data
        ORDER BY month_date, symbol
        """
    return con.execute(sql).fetchdf()


def _compute_forward_returns(con, cfg: FactorBacktestConfig, rebal_dates: list[str]) -> pd.DataFrame:
    """
    Compute hold_months forward return for each stock from each rebalance date.
    entry = last close of the score month
    exit  = last close of month+hold_months
    """
    hold = cfg.hold_months
    cost = cfg.transaction_cost_bps / 10000.0  # one-way, applied round-trip

    # monthly_prices keyed by (symbol, month_start)
    # rebal_dates are month-end strings like '2015-01-31'
    # DATE_TRUNC converts them to '2015-01-01' (month_start)
    date_list = ",".join(f"DATE_TRUNC('month', '{d}'::DATE)" for d in rebal_dates)

    sql = f"""
    WITH monthly_prices AS (
        SELECT
            symbol,
            DATE_TRUNC('month', price_date) AS month_start,
            LAST(adjusted_close ORDER BY price_date) AS close_price
        FROM v_backtest_prices
        WHERE price_date >= '{cfg.start_date}'::DATE - INTERVAL '2 months'
          AND price_date <= '{cfg.end_date}'::DATE   + INTERVAL '{hold + 2} months'
        GROUP BY symbol, DATE_TRUNC('month', price_date)
    )
    SELECT
        t0.symbol,
        t0.month_start::VARCHAR                           AS entry_month,
        t1.month_start::VARCHAR                           AS exit_month,
        (t1.close_price / NULLIF(t0.close_price, 0) - 1
         - {cost * 2})                                   AS fwd_return
    FROM monthly_prices t0
    JOIN monthly_prices t1
        ON  t0.symbol = t1.symbol
        AND t1.month_start = t0.month_start + INTERVAL '{hold} months'
    WHERE t0.month_start IN ({date_list})
      AND t1.close_price IS NOT NULL
      AND t0.close_price > 0
    """
    df = con.execute(sql).fetchdf()
    # entry_month is month_start string ('2015-01-01')
    # scores use month_end ('2015-01-31')
    # We'll join on the month_start derived from score month_date
    return df


def _run_quintile_backtest(
    df_scores: pd.DataFrame,
    df_fwd: pd.DataFrame,
    cfg: FactorBacktestConfig,
    rebal_dates: list[str],
    _cb,
    df_r3000: pd.DataFrame | None = None,
) -> dict:
    """Core backtest loop — ranks stocks, assigns buckets, computes returns."""
    n = cfg.n_buckets
    cost = cfg.transaction_cost_bps / 10000.0

    # df_fwd.entry_month is '2015-01-01' (month_start)
    # df_scores.month_date is '2015-01-31' (month_end)
    # Derive month_start from score month_date for joining
    df_scores["month_date"] = df_scores["month_date"].astype(str)
    df_scores["month_start"] = pd.to_datetime(df_scores["month_date"]).dt.to_period("M").dt.to_timestamp().dt.strftime("%Y-%m-%d")
    df_fwd["entry_month"]    = df_fwd["entry_month"].astype(str)

    df = df_scores.merge(
        df_fwd[["symbol", "entry_month", "fwd_return"]],
        left_on=["symbol", "month_start"],
        right_on=["symbol", "entry_month"],
        how="inner"
    ).dropna(subset=["score_raw", "fwd_return"])

    if df.empty:
        return {"status": "error", "error": "No matched score+return data"}

    # Rank within each period
    def assign_bucket(grp):
        if len(grp) < n:
            return grp.assign(bucket=np.nan)
        try:
            if cfg.score_direction == "higher_better":
                grp = grp.assign(bucket=pd.qcut(-grp["score_raw"], n, labels=False, duplicates="drop") + 1)
            else:
                grp = grp.assign(bucket=pd.qcut(grp["score_raw"], n, labels=False, duplicates="drop") + 1)
        except Exception:
            # Fallback: rank-based assignment when qcut fails (e.g. gate-count scores with many ties)
            if cfg.score_direction == "higher_better":
                ranks = (-grp["score_raw"]).rank(method="first")
            else:
                ranks = grp["score_raw"].rank(method="first")
            grp = grp.assign(bucket=pd.cut(ranks, n, labels=False) + 1)
        return grp

    df = df.groupby("month_date", group_keys=False).apply(assign_bucket)
    df = df.dropna(subset=["bucket"])
    df["bucket"] = df["bucket"].astype(int)

    # Per-period per-bucket average return
    period_stats = df.groupby(["month_date", "bucket"])["fwd_return"].agg(["mean", "count"]).reset_index()
    period_stats.columns = ["month_date", "bucket", "avg_ret", "n_stocks"]
    period_stats = period_stats.sort_values("month_date")

    dates = sorted(period_stats["month_date"].unique().tolist())
    n_stocks_avg = df.groupby("month_date")["bucket"].count().mean()
    _cb(f"  {len(dates)} periods, avg {n_stocks_avg:.0f} stocks/period")

    # Equity curves per bucket (cumulative product)
    bucket_returns = {}
    bucket_equity = {}
    bucket_dates = {}
    for b in range(1, n + 1):
        bdata = period_stats[period_stats["bucket"] == b].sort_values("month_date")
        rets = bdata["avg_ret"].tolist()
        bucket_returns[str(b)] = [round(r, 6) for r in rets]
        eq = [1.0]
        for r in rets:
            eq.append(round(eq[-1] * (1 + r), 6))
        bucket_equity[str(b)] = eq
        bucket_dates[str(b)] = bdata["month_date"].tolist()

    # ── Universe returns: Russell 3000 EW (Norgate PIT) or fallback ──────────────
    # Priority:
    #   1. Russell 3000 EW from PROD_Sector_Index_Membership (point-in-time, Norgate)
    #      → matches Tortoriello's S&P 1500 EW methodology
    #   2. Fallback: EW average of all scored stocks in our filtered universe
    if df_r3000 is not None and not df_r3000.empty:
        # Align R3000 returns to our backtest dates
        r3000_map = dict(zip(df_r3000["month_date"].astype(str), df_r3000["r3000_ret"]))
        universe_rets_list = []
        for d in dates:
            ret = r3000_map.get(str(d))
            if ret is None:
                # Fallback to scored-stock EW for this period
                ret = float(df[df["month_date"] == d]["fwd_return"].mean()) if len(df[df["month_date"] == d]) > 0 else 0.0
            universe_rets_list.append(float(ret))
        universe_rets = np.array(universe_rets_list)
        n_r3000_periods = sum(1 for d in dates if str(d) in r3000_map)
        _cb(f"  Universe benchmark: Russell 3000 EW ({n_r3000_periods}/{len(dates)} periods from Norgate, rest fallback)")
    else:
        # Pure fallback: EW all scored stocks (our pre-existing approach)
        universe_rets_by_period = df.groupby("month_date")["fwd_return"].mean()
        universe_rets = universe_rets_by_period.reindex(dates).values
        _cb("  Universe benchmark: EW all scored stocks (R3000 unavailable)")

    universe_eq = np.concatenate([[1.0], np.cumprod(1 + universe_rets)])

    # Compute metrics per bucket
    from engine.metrics import compute_all
    bucket_metrics = {}
    for b in range(1, n + 1):
        rets = np.array(bucket_returns[str(b)])
        eq   = np.array(bucket_equity[str(b)])
        if len(rets) < 6:
            bucket_metrics[str(b)] = {}
            continue
        try:
            m = compute_all(equity=eq, monthly_ret=rets, periods_per_year=12/cfg.hold_months)
            bucket_metrics[str(b)] = m
        except Exception:
            bucket_metrics[str(b)] = {}

    # Build a period→universe_ret dict for _count_vs_universe closure
    # universe_rets is now aligned to `dates` array
    universe_rets_by_period = {d: float(universe_rets[i]) for i, d in enumerate(dates) if i < len(universe_rets)}

    # ── Tortoriello metrics per bucket ─────────────────────────────────────────
    ppy = 12 / cfg.hold_months   # periods per year (e.g. 2 for 6-month hold)
    tortoriello = {}
    for b in range(1, n + 1):
        rets_b = np.array(bucket_returns.get(str(b), []))
        if len(rets_b) < 4 or len(universe_rets) < 4:
            tortoriello[str(b)] = {}
            continue

        min_len = min(len(rets_b), len(universe_rets))
        rb = rets_b[:min_len]
        ru = universe_rets[:min_len]
        excess = rb - ru

        # Terminal wealth: $10,000 → $X
        eq_b = np.array(bucket_equity.get(str(b), [1.0]))
        terminal_wealth = float(10000 * eq_b[-1]) if len(eq_b) > 1 else 10000.0

        # Average excess return vs universe (arithmetic)
        avg_excess = float(excess.mean())

        # % of 1-period windows outperforming
        pct_1y = float((excess > 0).mean())

        # % of rolling 3-year windows outperforming (3yr = 6 periods at 6mo hold)
        window_3y = max(2, int(round(3 * ppy)))
        roll_3y = []
        for i in range(window_3y, len(rb) + 1):
            cum_b = float(np.prod(1 + rb[i-window_3y:i]) - 1)
            cum_u = float(np.prod(1 + ru[i-window_3y:i]) - 1)
            roll_3y.append(cum_b > cum_u)
        pct_3y = float(np.mean(roll_3y)) if roll_3y else 0.0

        # Maximum gain / loss (worst single period)
        max_gain = float(rb.max())
        max_loss = float(rb.min())

        # Standard deviation (annualized)
        std_dev = float(rb.std() * np.sqrt(ppy))

        # Beta and Alpha vs universe
        if np.var(ru) > 0:
            beta  = float(np.cov(rb, ru)[0, 1] / np.var(ru))
            alpha = float(rb.mean() * ppy - beta * ru.mean() * ppy)
        else:
            beta, alpha = 1.0, 0.0

        # Average portfolio size (stocks in bucket per period)
        avg_size = float(
            period_stats[period_stats["bucket"] == b]["n_stocks"].mean()
        )

        # Avg companies outperforming / underperforming universe per period
        def _count_vs_universe(grp):
            period_universe_ret = universe_rets_by_period.get(grp.name, 0)
            return pd.Series({
                "n_beat": (grp["fwd_return"] > period_universe_ret).sum(),
                "n_lag":  (grp["fwd_return"] <= period_universe_ret).sum(),
            })
        bucket_df = df[df["bucket"] == b]
        beat_lag  = bucket_df.groupby("month_date").apply(_count_vs_universe)
        avg_beat  = float(beat_lag["n_beat"].mean()) if len(beat_lag) > 0 else 0.0
        avg_lag   = float(beat_lag["n_lag"].mean())  if len(beat_lag) > 0 else 0.0

        # Median factor score (raw) per bucket — tells you what factor value is in each bucket
        med_score = float(bucket_df["score_raw"].median()) if len(bucket_df) > 0 else 0.0

        # Average market cap per bucket (if available)
        avg_mktcap = float(bucket_df["market_cap"].mean()) if "market_cap" in bucket_df.columns and not bucket_df["market_cap"].isna().all() else None

        # Rolling 3-year excess returns series (for the rolling chart)
        roll_3y_series = []
        roll_dates_3y  = []
        d_list = sorted(period_stats["month_date"].unique().tolist())
        for i in range(window_3y, len(rb) + 1):
            cum_b = float(np.prod(1 + rb[i-window_3y:i]) ** (1/3) - 1)  # annualized
            cum_u = float(np.prod(1 + ru[i-window_3y:i]) ** (1/3) - 1)
            roll_3y_series.append(round(cum_b - cum_u, 4))
            if i <= len(d_list):
                roll_dates_3y.append(d_list[i-1])

        tortoriello[str(b)] = {
            "terminal_wealth":  round(terminal_wealth, 0),
            "avg_excess_vs_univ": round(avg_excess, 4),
            "pct_1y_beats_univ":  round(pct_1y, 3),
            "pct_3y_beats_univ":  round(pct_3y, 3),
            "max_gain":           round(max_gain, 4),
            "max_loss":           round(max_loss, 4),
            "std_dev_ann":        round(std_dev, 4),
            "beta_vs_univ":       round(beta, 3),
            "alpha_vs_univ":      round(alpha, 4),
            "avg_portfolio_size": round(avg_size, 1),
            "avg_beat_universe":  round(avg_beat, 1),
            "avg_lag_universe":   round(avg_lag, 1),
            "median_factor_score":round(med_score, 2),
            "avg_market_cap":     round(avg_mktcap, 0) if avg_mktcap else None,
            "roll_3y_excess":     roll_3y_series,
            "roll_3y_dates":      roll_dates_3y,
        }

    # Universe metrics for comparison column
    from engine.metrics import compute_all as _ca
    try:
        univ_metrics = _ca(equity=universe_eq, monthly_ret=universe_rets, periods_per_year=ppy)
    except Exception:
        univ_metrics = {}
    universe_terminal = float(10000 * universe_eq[-1]) if len(universe_eq) > 1 else 10000.0

    # IC (Information Coefficient) — Spearman ρ between rank and fwd return per period
    ic_data = []
    for d in dates:
        grp = df[df["month_date"] == d].dropna(subset=["score_raw", "fwd_return"])
        if len(grp) < 10:
            continue
        try:
            from scipy.stats import spearmanr
            rho, pval = spearmanr(grp["score_raw"], grp["fwd_return"])
            if cfg.score_direction == "lower_better":
                rho = -rho
            ic_data.append({"date": d, "ic_value": round(float(rho), 4)})
        except Exception:
            pass

    # IC metrics
    ic_vals = [x["ic_value"] for x in ic_data if x["ic_value"] is not None]
    ic_mean  = float(np.mean(ic_vals)) if ic_vals else 0.0
    ic_std   = float(np.std(ic_vals))  if ic_vals else 1.0
    icir     = float(ic_mean / ic_std * np.sqrt(12 / cfg.hold_months)) if ic_std > 0 else 0.0
    ic_hit   = float(np.mean([v > 0 for v in ic_vals])) if ic_vals else 0.0

    # Quintile spread: Q1 CAGR - Q5 CAGR (or Q1 - Qn)
    q1m = bucket_metrics.get("1", {})
    qnm = bucket_metrics.get(str(n), {})
    q1_cagr = q1m.get("cagr", 0) or 0
    qn_cagr = qnm.get("cagr", 0) or 0
    spread_cagr = q1_cagr - qn_cagr

    # Monotonicity score — what fraction of periods Q1 > Q2 > ... > Qn (cumulative)
    mono_score = _compute_monotonicity(bucket_metrics, n)

    # Spearman ρ across buckets: bucket_number vs avg_CAGR
    try:
        from scipy.stats import spearmanr
        b_nums  = list(range(1, n + 1))
        b_cagrs = [bucket_metrics.get(str(b), {}).get("cagr", 0) or 0 for b in b_nums]
        spearman_rho, _ = spearmanr(b_nums, b_cagrs)
        spearman_rho = float(spearman_rho) * (-1)  # Q1=best → negative correlation expected
    except Exception:
        spearman_rho = 0.0

    # Annual returns by bucket
    annual_ret_by_bucket = {}
    for b in range(1, n + 1):
        bdates = bucket_dates.get(str(b), [])
        brets  = bucket_returns.get(str(b), [])
        by_year = {}
        for d, r in zip(bdates, brets):
            yr = d[:4]
            if yr not in by_year:
                by_year[yr] = []
            by_year[yr].append(r)
        annual_ret_by_bucket[str(b)] = [
            {"year": int(yr), "ret": round(float(np.prod([1+r for r in rs])) - 1, 4)}
            for yr, rs in sorted(by_year.items())
        ]

    # Sector attribution: avg return per sector per bucket
    sector_attr = _compute_sector_attribution(df, n)

    # Period data for table display
    period_data = []
    for d in dates:
        pd_row = {"date": d}
        for b in range(1, n + 1):
            b_row = period_stats[(period_stats["month_date"] == d) & (period_stats["bucket"] == b)]
            pd_row[f"q{b}_ret"] = round(float(b_row["avg_ret"].values[0]), 4) if len(b_row) > 0 else None
        period_data.append(pd_row)

    # Fitness metrics (staircase, alpha win rate, bear/bull, OBQ Fund Score)
    # Must run BEFORE factor_metrics dict is built
    fitness = {}
    try:
        fitness = _compute_fitness_metrics(
            dates=dates,
            bucket_returns=bucket_returns,
            universe_rets=universe_rets,
            bucket_metrics=bucket_metrics,
            annual_ret_by_bucket=annual_ret_by_bucket,
            n=n,
        )
    except Exception as _fit_err:
        _cb(f"Fitness metrics skipped: {_fit_err}")

    # Composite factor metrics (includes all fitness scores)
    factor_metrics = {
        "ic_mean":             round(ic_mean, 4),
        "ic_std":              round(ic_std, 4),
        "icir":                round(icir, 3),
        "ic_hit_rate":         round(ic_hit, 3),
        "spearman_rho":        round(spearman_rho, 3),
        "monotonicity_score":  round(mono_score, 3),
        "quintile_spread_cagr":round(spread_cagr, 4),
        "n_obs":               len(dates),
        "n_stocks_avg":        round(n_stocks_avg, 0),
        "q1_cagr":             round(q1_cagr, 4),
        "q1_sharpe":           round(q1m.get("sharpe", 0) or 0, 3),
        "q1_max_dd":           round(q1m.get("max_dd", 0) or 0, 4),
        "q1_calmar":           round(q1m.get("calmar", 0) or 0, 3),
        "q1_surefire":         round(q1m.get("surefire_ratio", 0) or 0, 3),
        "qn_cagr":             round(qn_cagr, 4),
        "qn_sharpe":           round(qnm.get("sharpe", 0) or 0, 3),
        "hold_months":         cfg.hold_months,
        "n_buckets":           n,
        # Fitness scores
        "staircase_score":     fitness.get("staircase_score", 0.0),
        "alpha_win_rate":      fitness.get("alpha_win_rate", 0.0),
        "avg_annual_alpha":    fitness.get("avg_annual_alpha", 0.0),
        "bear_score":          fitness.get("bear_score", 0.0),
        "bull_score":          fitness.get("bull_score", 0.0),
        "downside_capture":    fitness.get("downside_capture", 1.0),
        "alpha_sharpe":        fitness.get("alpha_sharpe", 0.0),
        "obq_fund_score":      fitness.get("obq_fund_score", 0.0),
    }

    # SPY benchmark — fetch metrics sliced to same date range
    spy_metrics = {}
    try:
        from engine.spy_backtest import run_spy_backtest
        spy_result = run_spy_backtest(
            start_date=dates[0] if dates else cfg.start_date,
            end_date=dates[-1] if dates else cfg.end_date,
        )
        if spy_result.get("status") == "complete":
            spy_metrics = spy_result.get("portfolio_metrics", {})
            spy_eq = spy_result.get("portfolio_equity", [1.0])
            spy_metrics["terminal_wealth"] = round(10000 * spy_eq[-1], 0) if spy_eq else 10000.0

            # ── Recompute fitness metrics vs SPY (honest benchmark) ──────────
            # Replace the alpha_win_rate and avg_annual_alpha that were computed
            # vs the EW universe with versions computed vs SPY annual returns.
            try:
                spy_annual_ret = spy_result.get("annual_ret_by_year", [])
                spy_by_year_map = {a["year"]: a["ret"] for a in spy_annual_ret if a.get("ret") is not None}
                q1_annual_map   = {d["year"]: d["ret"] for d in (annual_ret_by_bucket.get("1") or [])}

                common_spy = set(q1_annual_map.keys()) & set(spy_by_year_map.keys())
                if len(common_spy) >= 3:
                    spy_alpha_win = float(sum(
                        1 for yr in common_spy if q1_annual_map[yr] > spy_by_year_map[yr]
                    ) / len(common_spy))
                    spy_alpha_series = [q1_annual_map[yr] - spy_by_year_map[yr]
                                        for yr in sorted(common_spy)]
                    spy_avg_alpha = float(np.mean(spy_alpha_series))

                    # Recompute OBQ Fund Score vs SPY
                    q1_max_dd_v   = abs(q1m.get("max_dd", 0) or 0)
                    spy_max_dd_v  = abs(spy_metrics.get("max_dd", 0.01) or 0.01)
                    dd_protection = float(np.clip(1.0 - (q1_max_dd_v / max(spy_max_dd_v, 0.001)), 0.0, 1.0))

                    spy_down_years = [yr for yr in common_spy if spy_by_year_map.get(yr, 0) < 0]
                    if spy_down_years:
                        q1_bear_avg  = np.mean([q1_annual_map.get(yr, 0) for yr in spy_down_years])
                        spy_bear_avg = np.mean([spy_by_year_map.get(yr, 0) for yr in spy_down_years])
                        dn_cap_vs_spy = float(q1_bear_avg / spy_bear_avg) if spy_bear_avg != 0 else 1.0
                        dn_score_spy  = float(np.clip(1.0 - dn_cap_vs_spy, -1.0, 1.0))
                    else:
                        dn_score_spy = 0.0

                    if len(spy_alpha_series) >= 3:
                        a_arr = np.array(spy_alpha_series)
                        alpha_sharpe_spy = float(a_arr.mean() / a_arr.std()) if a_arr.std() > 0 else 0.0
                    else:
                        alpha_sharpe_spy = 0.0

                    obq_fund_vs_spy = float(
                        0.30 * spy_alpha_win
                      + 0.25 * float(np.tanh(spy_avg_alpha / 0.05))
                      + 0.20 * dd_protection
                      + 0.15 * dn_score_spy
                      + 0.10 * float(np.tanh(alpha_sharpe_spy))
                    )
                    obq_fund_vs_spy = round(float(np.clip(obq_fund_vs_spy, -1.0, 1.0)), 4)

                    # Update factor_metrics with SPY-based fitness (overrides universe-based)
                    factor_metrics["alpha_win_rate"]   = round(spy_alpha_win, 3)
                    factor_metrics["avg_annual_alpha"] = round(spy_avg_alpha, 4)
                    factor_metrics["obq_fund_score"]   = obq_fund_vs_spy
                    factor_metrics["alpha_sharpe"]     = round(alpha_sharpe_spy, 3)
                    fitness["alpha_win_rate"]           = round(spy_alpha_win, 3)
                    fitness["avg_annual_alpha"]         = round(spy_avg_alpha, 4)
                    fitness["obq_fund_score"]           = obq_fund_vs_spy
                    _cb(f"OBQ Fund Score (vs SPY): {obq_fund_vs_spy:.4f} | Alpha Win Rate: {spy_alpha_win*100:.1f}%/yr | Avg Alpha: {spy_avg_alpha*100:.2f}%/yr")
            except Exception as _spy_fit_err:
                _cb(f"SPY fitness recompute skipped: {_spy_fit_err}")

    except Exception as _spy_err:
        _cb(f"SPY metrics skipped: {_spy_err}")

    # Trade log: per-stock entry/exit records for Q1 bucket
    # entry = rebalance date, exit = next rebalance date
    # Provides full auditability for every position held
    # ── Build Q1 Trade Log ────────────────────────────────────────────────────
    # Detect actual symbol column name (may be 'symbol' or 'Symbol' depending on source)
    sym_col = "symbol" if "symbol" in df.columns else "Symbol"
    trade_log = []
    try:
        available_cols = list(df.columns)
        for i, d_entry in enumerate(dates[:-1]):
            d_exit = dates[i + 1]
            mask = (df["month_date"] == d_entry) & (df["bucket"] == 1)
            q1_stocks = df[mask].copy()
            if q1_stocks.empty:
                continue

            for _, row in q1_stocks.iterrows():
                fwd_ret = row.get("fwd_return")
                if fwd_ret is not None and not pd.isna(fwd_ret):
                    fwd_ret = float(np.clip(fwd_ret, -0.95, 3.0))
                mc  = row.get("market_cap")
                sym = str(row.get(sym_col, "") or "").replace(".US", "")
                trade_log.append({
                    "entry_date":  d_entry,
                    "symbol":      sym,
                    "entry_price": None,   # factor engine uses bucket-avg, no individual prices
                    "exit_date":   d_exit,
                    "exit_price":  None,
                    "return_pct":  round(fwd_ret * 100, 2) if fwd_ret is not None else None,
                    "sector":      str(row.get("gic_sector") or "Unknown"),
                    "score":       round(float(row["score_raw"]), 4) if row.get("score_raw") is not None else None,
                    "bucket":      1,
                    "market_cap_B":round(float(mc)/1e9, 3) if mc and not pd.isna(mc) else None,
                })
        _cb(f"Trade log built: {len(trade_log)} Q1 entries across {len(dates)-1} periods")
    except Exception as _tl_err:
        _cb(f"Trade log error: {_tl_err}")
        import traceback; traceback.print_exc()

    return {
        "dates":                dates,
        "buckets":              list(range(1, n + 1)),
        "bucket_returns":       bucket_returns,
        "bucket_equity":        bucket_equity,
        "bucket_metrics":       bucket_metrics,
        "tortoriello":          tortoriello,
        "universe_metrics":     univ_metrics,
        "universe_equity":      [round(v,6) for v in universe_eq.tolist()],
        "universe_terminal":    round(universe_terminal, 0),
        "spy_metrics":          spy_metrics,
        "fitness":              fitness,
        "ic_data":              ic_data,
        "period_data":          period_data,
        "factor_metrics":       factor_metrics,
        "annual_ret_by_bucket": annual_ret_by_bucket,
        "sector_attribution":   sector_attr,
        "trade_log":            trade_log,       # Q1 per-stock trade audit log
        "n_obs":                len(dates),
        "n_stocks_avg":         round(n_stocks_avg, 0),
    }


def _compute_monotonicity(bucket_metrics: dict, n: int) -> float:
    """
    Monotonicity: fraction of adjacent bucket pairs where Q_i CAGR > Q_{i+1} CAGR.
    Perfect = 1.0 (Q1 > Q2 > ... > Qn for CAGR).
    """
    cagrs = []
    for b in range(1, n + 1):
        c = bucket_metrics.get(str(b), {}).get("cagr", None)
        cagrs.append(c if c is not None else 0.0)
    pairs = [(cagrs[i] > cagrs[i + 1]) for i in range(len(cagrs) - 1)]
    return float(np.mean(pairs)) if pairs else 0.0


def _compute_sector_attribution(df: pd.DataFrame, n: int) -> list[dict]:
    """Average return per GIC sector per bucket."""
    if "gic_sector" not in df.columns:
        return []
    grp = df.groupby(["gic_sector", "bucket"])["fwd_return"].mean().unstack(fill_value=0)
    result = []
    for sector in grp.index:
        row = {"sector": sector}
        for b in range(1, n + 1):
            row[f"q{b}"] = round(float(grp.loc[sector, b]) if b in grp.columns else 0.0, 4)
        if 1 in grp.columns and n in grp.columns:
            row["spread"] = round(float(grp.loc[sector, 1]) - float(grp.loc[sector, n]), 4)
        result.append(row)
    return sorted(result, key=lambda x: x.get("spread", 0), reverse=True)


# ── Fitness metrics ────────────────────────────────────────────────────────────

# Bear and bull market date windows (extended history since we have data to 1990)
BEAR_WINDOWS = [
    ("1990 Recession",    "1990-01-01", "1990-10-31"),
    ("1994 Bond Crash",   "1994-01-01", "1994-12-31"),
    ("1997-98 Crisis",    "1997-07-01", "1998-10-31"),
    ("2000-02 Dot-Com",   "2000-03-01", "2002-09-30"),
    ("2007-09 GFC",       "2007-10-01", "2009-03-31"),
    ("2020 COVID",        "2020-02-01", "2020-04-30"),
    ("2022 Rate Shock",   "2022-01-01", "2022-12-31"),
]

BULL_WINDOWS = [
    ("1991-94 Recovery",  "1991-01-01", "1994-01-31"),
    ("1995-99 Tech Boom", "1995-01-01", "1999-12-31"),
    ("2003-07 Recovery",  "2003-01-01", "2007-09-30"),
    ("2009-19 Bull",      "2009-04-01", "2019-12-31"),
    ("2020-21 Rebound",   "2020-05-01", "2021-12-31"),
    ("2023-24 AI Bull",   "2023-01-01", "2024-12-31"),
]


def _compute_fitness_metrics(
    dates: list,
    bucket_returns: dict,
    universe_rets: np.ndarray,
    bucket_metrics: dict,
    annual_ret_by_bucket: dict,
    n: int,
) -> dict:
    """
    Compute all fitness / ranking metrics for the strategy log.

    Returns dict with:
      staircase_score     — Q1-Qn spread × monotonicity × step_uniformity
                            Rewards wide spread + clean descent + even steps.
                            High = Q1 clearly best, linear decline to Qn.
      alpha_win_rate      — % calendar years Q1 CAGR beats universe CAGR
      bear_score          — Q1 avg excess return vs universe during all bear windows
      bull_score          — Q1 avg excess return vs universe during all bull windows
      obq_fund_score      — composite fund fitness (0-1 scale)
      bear_detail         — [{period, q1_ret, univ_ret, excess}] for each bear window
      bull_detail         — [{period, q1_ret, univ_ret, excess}] for each bull window
    """
    ppy = 12.0  # periods per year base (dates are already at hold_month frequency)

    # ── 1. Staircase Score ────────────────────────────────────────────────────
    # Measures how cleanly Q1 > Q2 > Q3 > Q4 > Q5, rewarding:
    #   (a) Largest total slope  Q1_CAGR - Q5_CAGR   (wide spread)
    #   (b) Perfect monotonicity Q_i > Q_{i+1} every step (no inversions)
    #   (c) Even/uniform steps   (not all alpha in one jump)
    #
    # Formula:
    #   total_spread    = Q1_CAGR - Qn_CAGR
    #   monotonicity    = fraction of adjacent pairs where Q_i > Q_{i+1}  (0→1)
    #   step_uniformity = 1 - (std(steps) / mean_abs_step)  capped 0→1
    #                     measures how evenly distributed the decline is
    #
    #   staircase_score = total_spread × monotonicity × step_uniformity
    #
    # Interpretation: high score requires BOTH wide spread AND clean linear descent.
    # A factor where Q4 beats Q1 (top chart) scores near 0 despite large spread.
    cagrs = [bucket_metrics.get(str(b), {}).get("cagr", 0.0) or 0.0 for b in range(1, n+1)]
    steps = [cagrs[i] - cagrs[i+1] for i in range(len(cagrs)-1)]  # positive = good

    total_spread = cagrs[0] - cagrs[-1]                             # Q1 - Qn

    n_steps = len(steps)
    monotonicity = float(sum(1 for s in steps if s > 0) / n_steps) if n_steps > 0 else 0.0

    # Step uniformity: lightly penalize lumpiness (soft penalty, not a kill switch)
    # CV = coefficient of variation = std/mean. Map to uniformity: 1/(1 + CV*0.5)
    # This keeps high spread + mostly-monotonic factors in the game even if lumpy.
    if n_steps > 1 and any(abs(s) > 0 for s in steps):
        step_std  = float(np.std(steps))
        step_mean = float(np.mean([abs(s) for s in steps]))
        cv = step_std / max(step_mean, 1e-6)
        step_uniformity = float(1.0 / (1.0 + cv * 0.5))   # 0.4→1.0 range, never zeros out
    else:
        step_uniformity = 1.0

    # Additional penalty: if Q1 is not actually the best bucket, apply a hard penalty.
    # A factor where Q4 or Q5 outperforms Q1 is not just weak — it's inverted.
    q1_is_best = cagrs[0] >= max(cagrs)          # True only if Q1 is the top bucket
    inversion_penalty = 1.0 if q1_is_best else max(0.1, cagrs[0] / max(cagrs) if max(cagrs) > 0 else 0.1)

    staircase_score = float(total_spread * monotonicity * step_uniformity * inversion_penalty)

    # ── 2. Alpha Win Rate ─────────────────────────────────────────────────────
    # % calendar years where Q1 annual return > universe annual return.
    q1_annual = {d["year"]: d["ret"] for d in (annual_ret_by_bucket.get("1") or [])}
    # Build universe annual returns from universe_rets + dates alignment
    # universe_rets[i] corresponds to dates[i]
    univ_by_year: dict = {}
    for i, d in enumerate(dates):
        if i >= len(universe_rets):
            break
        yr = int(str(d)[:4])
        univ_by_year.setdefault(yr, []).append(float(universe_rets[i]))
    univ_annual = {yr: float(np.prod([1+r for r in rs])-1) for yr, rs in univ_by_year.items()}

    common_years = set(q1_annual.keys()) & set(univ_annual.keys())
    if common_years:
        alpha_win_rate = float(sum(
            1 for yr in common_years if q1_annual[yr] > univ_annual[yr]
        ) / len(common_years))
    else:
        alpha_win_rate = 0.0

    # Alpha series (Q1 - universe per year)
    alpha_series = [q1_annual[yr] - univ_annual[yr] for yr in sorted(common_years)]
    avg_annual_alpha = float(np.mean(alpha_series)) if alpha_series else 0.0

    # ── 3. Bear / Bull Window Scores ─────────────────────────────────────────
    def _window_score(windows, dates, q1_returns, univ_rets):
        """Avg Q1 excess return vs universe across all date-range windows that overlap data."""
        details = []
        for name, w_start, w_end in windows:
            # Find periods within this window
            idxs = [
                i for i, d in enumerate(dates)
                if w_start <= str(d) <= w_end and i < len(q1_returns) and i < len(univ_rets)
            ]
            if len(idxs) < 1:
                continue
            q1_r  = float(np.prod([1 + q1_returns[i]  for i in idxs]) - 1)
            un_r  = float(np.prod([1 + univ_rets[i]   for i in idxs]) - 1)
            excess = round(q1_r - un_r, 4)
            details.append({
                "period": name, "q1_ret": round(q1_r, 4),
                "univ_ret": round(un_r, 4), "excess": excess,
            })
        if not details:
            return 0.0, details
        score = float(np.mean([d["excess"] for d in details]))
        return round(score, 4), details

    q1_rets_list  = list(bucket_returns.get("1", []))
    univ_rets_list = list(universe_rets)

    bear_score, bear_detail = _window_score(BEAR_WINDOWS, dates, q1_rets_list, univ_rets_list)
    bull_score, bull_detail = _window_score(BULL_WINDOWS, dates, q1_rets_list, univ_rets_list)

    # ── 4. OBQ Fund Score ─────────────────────────────────────────────────────
    # Designed for 2-and-20 fund economics — maximize performance fee consistency.
    #
    #   30% × Alpha Win Rate        (% years you beat benchmark = fee trigger)
    #   25% × tanh(avg_alpha/0.05)  (magnitude of alpha, normalized)
    #   20% × DD Protection         (1 - Q1_MaxDD / Universe_MaxDD, capped 0-1)
    #   15% × Downside Capture      (1 - downside_capture, so lower capture = higher score)
    #   10% × Alpha Sharpe          (Sharpe of annual alpha series)

    q1_max_dd   = abs(bucket_metrics.get("1", {}).get("max_dd", 0.0) or 0.0)
    univ_max_dd = abs(bucket_metrics.get(str(n), {}).get("max_dd", 0.01) or 0.01)
    # Use universe_rets drawdown if available
    if len(universe_rets) > 1:
        univ_eq = np.cumprod(1 + universe_rets)
        roll_max = np.maximum.accumulate(univ_eq)
        dd = univ_eq / roll_max - 1
        univ_max_dd_real = abs(float(dd.min()))
        if univ_max_dd_real > 0:
            univ_max_dd = univ_max_dd_real

    dd_protection = float(np.clip(1.0 - (q1_max_dd / max(univ_max_dd, 0.001)), 0.0, 1.0))

    # Downside capture: in bear years, how much of universe loss does Q1 capture?
    bear_years = [yr for yr in common_years if univ_annual.get(yr, 0) < 0]
    if bear_years:
        q1_bear  = np.mean([q1_annual.get(yr, 0)   for yr in bear_years])
        un_bear  = np.mean([univ_annual.get(yr, 0)  for yr in bear_years])
        dn_cap   = float(q1_bear / un_bear) if un_bear != 0 else 1.0
        dn_cap   = float(np.clip(dn_cap, 0.0, 2.0))
        dn_score = float(np.clip(1.0 - dn_cap, -1.0, 1.0))  # low capture = good
    else:
        dn_cap   = 1.0
        dn_score = 0.0

    # Sharpe of alpha series
    if len(alpha_series) >= 3:
        a_arr = np.array(alpha_series)
        alpha_sharpe = float(a_arr.mean() / a_arr.std()) if a_arr.std() > 0 else 0.0
    else:
        alpha_sharpe = 0.0

    alpha_norm = float(np.tanh(avg_annual_alpha / 0.05))

    obq_fund_score = float(
        0.30 * alpha_win_rate
      + 0.25 * alpha_norm
      + 0.20 * dd_protection
      + 0.15 * dn_score
      + 0.10 * np.tanh(alpha_sharpe)  # normalize sharpe to -1..1
    )
    obq_fund_score = round(float(np.clip(obq_fund_score, -1.0, 1.0)), 4)

    return {
        "staircase_score":  round(staircase_score, 4),
        "alpha_win_rate":   round(alpha_win_rate, 3),
        "avg_annual_alpha": round(avg_annual_alpha, 4),
        "bear_score":       bear_score,
        "bull_score":       bull_score,
        "downside_capture": round(dn_cap, 3),
        "alpha_sharpe":     round(alpha_sharpe, 3),
        "obq_fund_score":   obq_fund_score,
        "bear_detail":      bear_detail,
        "bull_detail":      bull_detail,
    }


# ── Config validation ──────────────────────────────────────────────────────────

def get_available_scores() -> dict:
    """Return all available score columns with metadata."""
    return SCORE_COLUMNS


def get_score_date_range(score_col: str) -> dict:
    """Return min/max date and symbol count for a given score column."""
    try:
        con = _get_con()
        if score_col in {"moat_score", "moat_rank"}:
            table = "PROD_MOAT_SCORES"
            col   = score_col
            date_col = "month_date"
        else:
            table = "v_backtest_scores"
            col   = score_col
            date_col = "month_date"

        row = con.execute(f"""
            SELECT MIN({date_col})::VARCHAR, MAX({date_col})::VARCHAR, COUNT(DISTINCT symbol)
            FROM {table}
            WHERE {col} IS NOT NULL
        """).fetchone()
        con.close()
        return {"min_date": str(row[0]), "max_date": str(row[1]), "symbols": int(row[2])}
    except Exception as e:
        return {"error": str(e)}
