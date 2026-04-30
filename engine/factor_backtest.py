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
    start_date: str = "2005-01-31"
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
    "jcn_full_composite":        "JCN Composite",
    "jcn_qarp":                  "JCN QARP",
    "jcn_garp":                  "JCN GARP",
    "jcn_quality_momentum":      "JCN Quality-Momentum",
    "jcn_value_momentum":        "JCN Value-Momentum",
    "jcn_growth_quality_momentum":"JCN Growth-Quality-Momentum",
    "jcn_fortress":              "JCN Fortress",
    # Individual factor scores
    "value_score":               "Value Score",
    "quality_score":             "Quality Score",
    "growth_score":              "Growth Score",
    "momentum_score":            "Momentum Score",
    "finstr_score":              "Financial Strength",
    "momentum_af_score":         "Momentum (Alpha Factor)",
    "momentum_fip_score":        "Momentum (FIP)",
    # Moat / LongEQ (separate tables)
    "moat_score":                "Moat Score",
    "moat_rank":                 "Moat Rank",
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

        con.close()

        # ── 4. Join scores → prefilter → rank → buckets ────────────────────────
        _cb("Ranking and bucketing...")
        results = _run_quintile_backtest(df_scores, df_fwd, cfg, rebal_dates, _cb)

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
    """Get all monthly score dates within the backtest window."""
    freq_filter = {
        "monthly":     "TRUE",
        "quarterly":   "MONTH(month_date) IN (1,4,7,10)",
        "semi-annual": "MONTH(month_date) IN (1,7)",
        "annual":      "MONTH(month_date) = 1",
    }.get(cfg.rebalance_freq, "TRUE")

    rows = con.execute(f"""
        SELECT DISTINCT month_date::VARCHAR as d
        FROM v_backtest_scores
        WHERE month_date >= '{cfg.start_date}'::DATE
          AND month_date <= '{cfg.end_date}'::DATE
          AND {freq_filter}
        ORDER BY d
    """).fetchall()
    return [r[0] for r in rows]


def _resolve_score_column(score_col: str) -> str:
    """Map score column name to the actual v_backtest_scores column."""
    # Moat columns live in a separate table — handled specially
    direct = {
        "value_score", "quality_score", "growth_score", "finstr_score",
        "momentum_score", "momentum_af_score", "momentum_fip_score",
        "momentum_sys_score",
        "jcn_full_composite", "jcn_qarp", "jcn_garp",
        "jcn_quality_momentum", "jcn_value_momentum",
        "jcn_growth_quality_momentum", "jcn_fortress",
    }
    if score_col in direct:
        return score_col
    return score_col  # pass through, let SQL fail informatively


def _load_scores(con, cfg: FactorBacktestConfig, rebal_dates: list[str], score_col: str) -> pd.DataFrame:
    """
    Load scores with prefilter applied.
    Score dates are month-end (e.g. 2015-01-31). Prices are looked up by
    DATE_TRUNC('month', score_date) to get the monthly last price.
    """
    cap_lo, cap_hi = CAP_TIERS.get(cfg.cap_tier, (0, float("inf")))
    cap_hi_sql = f"{cap_hi}" if cap_hi != float("inf") else "9e18"
    sector_excl = ""
    if cfg.exclude_sectors:
        slist = ", ".join(f"'{s}'" for s in cfg.exclude_sectors)
        sector_excl = f"AND s.gic_sector NOT IN ({slist})"

    # Build monthly price snapshot (last close of each calendar month)
    # Then join to scores by matching DATE_TRUNC of score month_date
    # Note: volume is NULL in v_backtest_prices — use market_cap as liquidity proxy
    # min_adv_usd maps to minimum market_cap threshold (scaled: $1M ADV ≈ $50M mktcap)
    # Users set min_adv_usd; we convert: mktcap_min = max(cap_lo, min_adv_usd * 50)
    effective_mktcap_min = max(cap_lo, cfg.min_adv_usd * 50)

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
        WHERE s.month_date IN ({",".join(f"'{d}'" for d in rebal_dates)})
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
        if cfg.score_direction == "higher_better":
            grp = grp.assign(bucket=pd.qcut(-grp["score_raw"], n, labels=False) + 1)
        else:
            grp = grp.assign(bucket=pd.qcut(grp["score_raw"], n, labels=False) + 1)
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

    # ── Compute universe-level returns (equal-weighted all stocks per period) ──
    universe_rets_by_period = df.groupby("month_date")["fwd_return"].mean()
    universe_rets = universe_rets_by_period.values
    universe_eq   = np.concatenate([[1.0], np.cumprod(1 + universe_rets)])

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

    # Composite factor metrics
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
        "qn_cagr":             round(qn_cagr, 4),
        "qn_sharpe":           round(qnm.get("sharpe", 0) or 0, 3),
        "hold_months":         cfg.hold_months,
        "n_buckets":           n,
    }

    return {
        "dates":                dates,
        "buckets":              list(range(1, n + 1)),
        "bucket_returns":       bucket_returns,
        "bucket_equity":        bucket_equity,
        "bucket_metrics":       bucket_metrics,
        "tortoriello":          tortoriello,         # Tortoriello table metrics per bucket
        "universe_metrics":     univ_metrics,         # Universe column
        "universe_equity":      [round(v,6) for v in universe_eq.tolist()],
        "universe_terminal":    round(universe_terminal, 0),
        "ic_data":              ic_data,
        "period_data":          period_data,
        "factor_metrics":       factor_metrics,
        "annual_ret_by_bucket": annual_ret_by_bucket,
        "sector_attribution":   sector_attr,
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
