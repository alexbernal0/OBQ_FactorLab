"""
Core backtest engine — two modes:
  1. quintile: classic quintile-bin factor analysis
  2. topn:     long-only top-N portfolio vs benchmark

GPU-accelerated via engine.gpu where available.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone, date
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from engine import gpu
from engine.metrics import compute_all
from engine.data import UniverseConfig, load_scores, load_prices, build_monthly_price_matrix, compute_forward_returns

log = logging.getLogger("factorlab.backtest")

CRISIS_PERIODS = {
    "Dot-Com Crash":           ("2000-03-24", "2002-10-09"),
    "Global Financial Crisis": ("2007-10-09", "2009-03-09"),
    "European Debt Crisis":    ("2011-05-02", "2011-10-04"),
    "2015-16 Correction":      ("2015-08-10", "2016-02-11"),
    "Volmageddon":             ("2018-01-26", "2018-02-09"),
    "COVID Crash":             ("2020-02-20", "2020-03-23"),
    "2022 Rate Selloff":       ("2022-01-03", "2022-10-12"),
}


def _cb(cb, level, msg):
    if cb:
        try: cb(level, msg)
        except: pass


def run_backtest(cfg: UniverseConfig, cb: Optional[Callable] = None) -> dict:
    """
    Main entry point. cfg.model_type selects 'quintile' or 'topn'.
    Returns full results dict (JSON-serialisable).
    """
    t0 = time.time()
    run_id = str(uuid.uuid4())
    _cb(cb, "info", f"Run {run_id[:8]} starting  mode={cfg.model_type}  factor={cfg.factor}")

    # ── 1. Load scores ──────────────────────────────────────────────────────
    scores_df = load_scores(cfg, cb=lambda l, m: _cb(cb, l, m))
    if scores_df.empty:
        return {"status": "error", "error": "No score data returned for this config"}

    # ── 2. Get rebalance dates ──────────────────────────────────────────────
    rebal_dates = _get_rebal_dates(scores_df, cfg)
    _cb(cb, "info", f"Rebalance dates: {len(rebal_dates)} ({rebal_dates[0]} -> {rebal_dates[-1]})")

    # ── 3. Load prices for all symbols ─────────────────────────────────────
    symbols = scores_df["symbol"].unique().tolist()
    start_dt = str(rebal_dates[0])
    end_dt   = str(rebal_dates[-1])
    prices_df = load_prices(symbols, start_dt, end_dt, cb=lambda l, m: _cb(cb, l, m))
    price_matrix = build_monthly_price_matrix(prices_df)
    fwd_ret = compute_forward_returns(price_matrix, periods=1)

    # ── 4. Route to mode ────────────────────────────────────────────────────
    if cfg.model_type == "quintile":
        result = _run_quintile(cfg, scores_df, price_matrix, fwd_ret, rebal_dates, cb=cb)
    else:
        result = _run_topn(cfg, scores_df, price_matrix, fwd_ret, rebal_dates, cb=cb)

    result["run_id"] = run_id
    result["model_type"] = cfg.model_type
    result["factor"] = cfg.factor
    result["elapsed_s"] = round(time.time() - t0, 1)
    result["gpu_used"] = gpu.available()
    result["status"] = "complete"
    _cb(cb, "ok", f"Run complete in {result['elapsed_s']}s  GPU={'yes' if gpu.available() else 'no'}")
    return result


# ── Quintile mode ────────────────────────────────────────────────────────────

def _run_quintile(cfg, scores_df, price_matrix, fwd_ret, rebal_dates, cb=None):
    _cb(cb, "info", "Mode: Quintile Analysis")
    n_q = cfg.n_quintiles

    period_records = []
    q_equity = {f"Q{q}": [1.0] for q in range(1, n_q + 1)}
    q_equity["Universe"] = [1.0]
    equity_dates = [rebal_dates[0]]
    ic_records = []
    all_holdings = {q: None for q in range(1, n_q + 1)}
    turnover_records = []

    _cb(cb, "info", f"Processing {len(rebal_dates)-1} rebalance periods...")

    for i in range(len(rebal_dates) - 1):
        rd = rebal_dates[i]
        rd_next = rebal_dates[i + 1]

        # Scores at this rebalance date
        month_scores = scores_df[scores_df["month_date"] == rd].copy()
        if month_scores.empty:
            continue

        # Apply score transformations
        month_scores = _apply_score_transforms(month_scores, cfg)
        if len(month_scores) < n_q * 5:
            continue

        # Assign quintiles
        month_scores["quintile"] = _assign_quintile_labels(month_scores["score"], n_q)
        month_scores = month_scores.dropna(subset=["quintile"])

        # Forward returns — Timestamp-safe index lookup
        rd_next_ts = pd.Timestamp(rd_next)
        avail = fwd_ret.index[fwd_ret.index <= rd_next_ts]
        if len(avail) == 0: continue
        period_rets = fwd_ret.loc[avail[-1]].dropna()

        month_scores["fwd_ret"] = month_scores["symbol"].map(period_rets)
        month_scores = month_scores.dropna(subset=["fwd_ret"])

        if len(month_scores) < n_q:
            continue

        # IC
        ic_val = float(np.corrcoef(month_scores["score"], month_scores["fwd_ret"])[0, 1])
        ic_records.append({"date": str(rd), "ic": ic_val, "n": len(month_scores)})

        # Quintile returns
        q_rets = {}
        q_n = {}
        q_scores = {}
        q_turnover = {}
        for q in range(1, n_q + 1):
            q_stocks = month_scores[month_scores["quintile"] == q]
            if len(q_stocks) == 0: continue
            q_rets[q] = float(q_stocks["fwd_ret"].mean())
            q_n[q] = len(q_stocks)
            q_scores[q] = float(q_stocks["score"].mean())
            # Turnover vs prior period
            prior = all_holdings[q] if all_holdings[q] is not None else set()
            current = set(q_stocks["symbol"].tolist())
            to_val = len(current.symmetric_difference(prior)) / max(len(current | prior), 1)
            q_turnover[q] = float(to_val * 100)
            all_holdings[q] = current

        univ_ret = float(month_scores["fwd_ret"].mean())
        q_rets[0] = univ_ret  # 0 = universe

        period_records.append({
            "date": str(rd), "next_date": str(rd_next),
            "quintile_returns": q_rets,
            "quintile_n_stocks": q_n,
            "quintile_avg_score": q_scores,
            "quintile_turnover": q_turnover,
            "ic": ic_val, "n_stocks": len(month_scores),
        })

        # Compound equity curves
        for q in range(1, n_q + 1):
            r = q_rets.get(q, 0.0)
            q_equity[f"Q{q}"].append(q_equity[f"Q{q}"][-1] * (1 + r))
        q_equity["Universe"].append(q_equity["Universe"][-1] * (1 + univ_ret))
        equity_dates.append(rd_next)

    if not period_records:
        return {"status": "error", "error": "No valid periods computed"}

    _cb(cb, "info", f"Computed {len(period_records)} periods")

    # ── Annual returns ───────────────────────────────────────────────────────
    ann_df = _aggregate_annual(period_records, n_q)

    # ── Sector analysis ──────────────────────────────────────────────────────
    sector_df = _sector_analysis(scores_df, rebal_dates, fwd_ret, price_matrix, n_q)

    # ── GPU-accelerated IC computation ───────────────────────────────────────
    ic_series = np.array([r["ic"] for r in ic_records], dtype=np.float32)

    # ── Full metrics per quintile ────────────────────────────────────────────
    _cb(cb, "info", "Computing tearsheet metrics (GPU)...")
    equity_idx = pd.DatetimeIndex([pd.Timestamp(d) for d in equity_dates])
    quintile_metrics = {}
    for q in range(1, n_q + 1):
        eq = np.array(q_equity[f"Q{q}"], dtype=np.float64)
        mr = np.diff(eq) / eq[:-1]
        bm_eq = np.array(q_equity["Universe"], dtype=np.float64)
        bm_mr = np.diff(bm_eq) / bm_eq[:-1]
        ann_r = np.array([row.get(f"Q{q}", np.nan) for row in [_dummy_ann_row(ann_df, y) for y in ann_df.index]], dtype=float)
        q1q5 = None
        if q == 1 and "Q5" in q_equity:
            q1_cagr = (q_equity["Q1"][-1] / q_equity["Q1"][0]) ** (12/max(len(q_equity["Q1"])-1,1)) - 1
            q5_cagr = (q_equity["Q5"][-1] / q_equity["Q5"][0]) ** (12/max(len(q_equity["Q5"])-1,1)) - 1
            q1q5 = float(q1_cagr - q5_cagr)
        quintile_metrics[f"Q{q}"] = compute_all(
            equity=eq, monthly_ret=mr,
            bm_equity=bm_eq, bm_monthly=bm_mr,
            ic_series=ic_series if q == 1 else None,
            q1q5_spread_cagr=q1q5,
            label=f"Q{q}",
        )

    # Universe metrics
    bm_eq = np.array(q_equity["Universe"], dtype=np.float64)
    bm_mr = np.diff(bm_eq) / bm_eq[:-1]
    quintile_metrics["Universe"] = compute_all(equity=bm_eq, monthly_ret=bm_mr, label="Universe")

    # ── Obsidian Score ────────────────────────────────────────────────────────
    obsidian = _obsidian_score(period_records, sector_df, ann_df, n_q)

    return {
        "mode": "quintile",
        "equity_curves": {k: [round(v, 6) for v in vals] for k, vals in q_equity.items()},
        "equity_dates": [str(d) for d in equity_dates],
        "period_data": period_records,
        "annual_returns": ann_df.to_dict() if hasattr(ann_df, "to_dict") else {},
        "sector_analysis": sector_df.to_dict(orient="records") if sector_df is not None else [],
        "ic_data": [{"date": r["date"], "ic_value": r["ic"], "n_stocks": r["n"]} for r in ic_records],
        "quintile_metrics": quintile_metrics,
        "obsidian_score": obsidian,
        "n_periods": len(period_records),
        "start_date": str(rebal_dates[0]),
        "end_date": str(rebal_dates[-1]),
    }


# ── Top-N mode ───────────────────────────────────────────────────────────────

def _run_topn(cfg, scores_df, price_matrix, fwd_ret, rebal_dates, cb=None):
    _cb(cb, "info", f"Mode: Top-{cfg.top_n} Portfolio")
    N = cfg.top_n
    comm = cfg.commission_bps / 10000
    slip = cfg.slippage_bps / 10000
    cost_per_trade = comm + slip

    portfolio_equity = [cfg.initial_capital]
    bm_equity = [cfg.initial_capital]
    equity_dates = [rebal_dates[0]]
    period_records = []
    holdings = pd.Series(dtype=float)

    for i in range(len(rebal_dates) - 1):
        rd, rd_next = rebal_dates[i], rebal_dates[i+1]
        month_scores = scores_df[scores_df["month_date"] == rd].copy()
        if month_scores.empty: continue
        month_scores = _apply_score_transforms(month_scores, cfg)
        if len(month_scores) < N: continue

        # Select top N
        top_n_df = month_scores.nlargest(N, "score")
        new_holdings = pd.Series(
            1.0 / N, index=top_n_df["symbol"].tolist()
        )

        # Transaction costs
        if len(holdings) > 0:
            all_syms = set(new_holdings.index) | set(holdings.index)
            turnover = sum(abs(new_holdings.get(s, 0) - holdings.get(s, 0)) for s in all_syms) / 2
            cost_drag = turnover * cost_per_trade
        else:
            cost_drag = cost_per_trade  # initial purchase

        # Forward returns — normalize to Timestamp for reliable index comparison
        rd_next_ts = pd.Timestamp(rd_next)
        avail = fwd_ret.index[fwd_ret.index <= rd_next_ts]
        if len(avail) == 0: continue
        ret_col = avail[-1]
        period_rets = fwd_ret.loc[ret_col].dropna()

        # Map holdings to returns using index alignment
        holding_rets = period_rets.reindex(new_holdings.index).dropna()
        if len(holding_rets) == 0: continue
        portfolio_ret = float((holding_rets * new_holdings.reindex(holding_rets.index)).sum() - cost_drag)
        univ_ret = float(period_rets.mean())

        portfolio_equity.append(portfolio_equity[-1] * (1 + portfolio_ret))
        bm_equity.append(bm_equity[-1] * (1 + univ_ret))
        equity_dates.append(rd_next)
        holdings = new_holdings

        period_records.append({
            "date": str(rd), "next_date": str(rd_next),
            "portfolio_return": portfolio_ret,
            "universe_return": univ_ret,
            "n_stocks": N,
            "cost_drag": cost_drag,
            "top_holdings": top_n_df[["symbol","score"]].head(10).to_dict(orient="records"),
        })

    if not period_records:
        return {"status": "error", "error": "No valid periods for Top-N"}

    # Metrics
    port_eq = np.array(portfolio_equity, dtype=np.float64)
    port_mr = np.diff(port_eq) / port_eq[:-1]
    bm_eq_arr = np.array(bm_equity, dtype=np.float64)
    bm_mr_arr = np.diff(bm_eq_arr) / bm_eq_arr[:-1]

    portfolio_metrics = compute_all(
        equity=port_eq, monthly_ret=port_mr,
        bm_equity=bm_eq_arr, bm_monthly=bm_mr_arr,
        label=f"Top-{N} {cfg.factor}",
    )
    bm_metrics = compute_all(equity=bm_eq_arr, monthly_ret=bm_mr_arr, label="EW Universe")

    # Monthly return heatmap data
    heatmap = _monthly_heatmap(equity_dates, port_mr.tolist())

    return {
        "mode": "topn",
        "portfolio_equity": [round(v, 4) for v in portfolio_equity],
        "bm_equity": [round(v, 4) for v in bm_equity],
        "equity_dates": [str(d) for d in equity_dates],
        "period_data": period_records,
        "portfolio_metrics": portfolio_metrics,
        "bm_metrics": bm_metrics,
        "monthly_heatmap": heatmap,
        "n_periods": len(period_records),
        "start_date": str(rebal_dates[0]),
        "end_date": str(rebal_dates[-1]),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_rebal_dates(scores_df: pd.DataFrame, cfg: UniverseConfig) -> List[date]:
    # Normalize to Python date objects
    raw = sorted(scores_df["month_date"].unique())
    all_dates = [d.date() if hasattr(d, 'date') else d for d in raw]
    sched = cfg.rebalance
    def _ok(d):
        if sched == "Monthly": return True
        if sched in ("Quarterly", "Quarterly Std"): return d.month in (3,6,9,12)
        if sched == "Semi-Annual": return d.month in (6,12)
        if sched == "Annual": return d.month == 12
        return True
    dates = [d for d in all_dates if _ok(d)]
    # Extend by 1 for forward return period boundary
    if dates and all_dates[-1] > dates[-1]:
        next_avail = [d for d in all_dates if d > dates[-1]]
        if next_avail: dates.append(next_avail[0])
    return dates


def _apply_score_transforms(df: pd.DataFrame, cfg: UniverseConfig) -> pd.DataFrame:
    df = df.copy()
    df["score"] = df["score"].astype(float)
    if cfg.na_handling == "Exclude":
        df = df.dropna(subset=["score"])
    elif cfg.na_handling == "Worst":
        df["score"] = df["score"].fillna(0.0)
    elif cfg.na_handling == "Neutral":
        df["score"] = df["score"].fillna(50.0)
    if cfg.winsorize:
        lo = np.percentile(df["score"].dropna(), 1)
        hi = np.percentile(df["score"].dropna(), 99)
        df["score"] = df["score"].clip(lo, hi)
    if cfg.sector_neutral and "gic_sector" in df.columns:
        df["score"] = df.groupby("gic_sector")["score"].transform(
            lambda x: (x - x.mean()) / (x.std() + 1e-8) * 15 + 50
        )
    return df.dropna(subset=["score"])


def _assign_quintile_labels(scores: pd.Series, n_q: int) -> pd.Series:
    n = len(scores)
    ranked = scores.rank(ascending=False, method="first")
    per_q = n / n_q
    q_labels = ((ranked - 1) // per_q + 1).clip(1, n_q).astype(int)
    return q_labels


def _aggregate_annual(period_records, n_q):
    rows = []
    for p in period_records:
        yr = int(str(p["date"])[:4])
        row = {"year": yr}
        row["Universe"] = p["quintile_returns"].get(0, np.nan)
        for q in range(1, n_q+1):
            row[f"Q{q}"] = p["quintile_returns"].get(q, np.nan)
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty: return df
    agg = df.groupby("year").apply(lambda g: (1 + g.drop("year",axis=1).fillna(0)).prod() - 1)
    return agg


def _dummy_ann_row(ann_df, yr):
    if ann_df.empty or yr not in ann_df.index: return {}
    return ann_df.loc[yr].to_dict()


def _sector_analysis(scores_df, rebal_dates, fwd_ret, price_matrix, n_q):
    try:
        rows = []
        for rd in rebal_dates[:-1]:
            ms = scores_df[scores_df["month_date"] == rd].copy()
            if ms.empty: continue
            ms["quintile"] = _assign_quintile_labels(ms["score"].astype(float), n_q)
            avail = fwd_ret.index[fwd_ret.index <= rebal_dates[rebal_dates.index(rd)+1]]
            if len(avail) == 0: continue
            period_rets = fwd_ret.loc[avail[-1]]
            ms["fwd_ret"] = ms["symbol"].map(period_rets)
            ms = ms.dropna(subset=["fwd_ret","gic_sector"])
            rows.append(ms)
        if not rows: return None
        all_data = pd.concat(rows)
        q1 = all_data[all_data["quintile"]==1].groupby("gic_sector")["fwd_ret"].mean().rename("Q1")
        q5 = all_data[all_data["quintile"]==n_q].groupby("gic_sector")["fwd_ret"].mean().rename("Q5")
        sec = pd.concat([q1, q5], axis=1).dropna()
        sec["spread"] = sec["Q1"] - sec["Q5"]
        sec["q1_beats"] = sec["spread"] > 0
        return sec.reset_index().sort_values("spread", ascending=False)
    except Exception as e:
        log.warning(f"Sector analysis failed: {e}")
        return None


def _obsidian_score(period_records, sector_df, ann_df, n_q):
    """5-star Obsidian Backtest Score."""
    THRESHOLDS = {
        "benchmark_win_rate":    [0.50, 0.65, 0.75, 0.85],
        "quintile_progression":  [0.25, 0.50, 0.75, 1.00],
        "sector_consistency":    [0.50, 0.65, 0.75, 0.85],
        "absolute_performance":  [0.03, 0.05, 0.08, 0.12],
        "yoy_consistency":       [0.50, 0.65, 0.75, 0.85],
    }
    def stars(val, thresholds):
        for i, t in enumerate(reversed(thresholds)):
            if val >= t:
                return (4 - i) * 0.25
        return 0.0

    scores = {}
    details = {}

    # Annual returns pivoted
    if not ann_df.empty:
        yr_q1 = ann_df.get("Q1", pd.Series())
        yr_univ = ann_df.get("Universe", pd.Series())
        yr_q5 = ann_df.get(f"Q{n_q}", pd.Series())

        bwr = float((yr_q1 > yr_univ).mean()) if len(yr_q1) > 0 else 0.0
        scores["benchmark_win_rate"] = stars(bwr, THRESHOLDS["benchmark_win_rate"])
        details["benchmark_win_rate"] = {"value": bwr, "years_won": int((yr_q1>yr_univ).sum()), "total": len(yr_q1)}

        yoy = float((yr_q1 > yr_q5).mean()) if len(yr_q1) > 0 else 0.0
        scores["yoy_consistency"] = stars(yoy, THRESHOLDS["yoy_consistency"])
        details["yoy_consistency"] = {"value": yoy}

        q_means = {f"Q{q}": float(ann_df.get(f"Q{q}", pd.Series()).mean()) for q in range(1, n_q+1)}
        pairs_ok = sum(1 for q in range(1, n_q) if q_means.get(f"Q{q}",0) > q_means.get(f"Q{q+1}",0))
        prog = pairs_ok / (n_q - 1)
        scores["quintile_progression"] = stars(prog, THRESHOLDS["quintile_progression"])
        details["quintile_progression"] = {"pairs_correct": pairs_ok, "pairs_total": n_q-1, "q_means": q_means}

        q1_cagr = float((1 + yr_q1).prod() ** (1/max(len(yr_q1),1)) - 1)
        q5_cagr = float((1 + yr_q5).prod() ** (1/max(len(yr_q5),1)) - 1)
        spread = q1_cagr - q5_cagr
        scores["absolute_performance"] = stars(spread, THRESHOLDS["absolute_performance"])
        details["absolute_performance"] = {"spread": spread, "q1_cagr": q1_cagr, "q5_cagr": q5_cagr}
    else:
        for k in ["benchmark_win_rate","yoy_consistency","quintile_progression","absolute_performance"]:
            scores[k] = 0.0; details[k] = {}

    if sector_df is not None and len(sector_df) > 0:
        sec_cons = float((sector_df["spread"] > 0).mean())
        scores["sector_consistency"] = stars(sec_cons, THRESHOLDS["sector_consistency"])
        details["sector_consistency"] = {"value": sec_cons, "n_sectors": len(sector_df)}
    else:
        scores["sector_consistency"] = 0.0
        details["sector_consistency"] = {}

    total = sum(scores.values())
    rating_map = [(4.5,"EXCEPTIONAL"),(4.0,"EXCELLENT"),(3.5,"STRONG"),(2.5,"MODERATE"),(1.5,"WEAK"),(0,"POOR")]
    rating = next(r for t, r in rating_map if total >= t)
    return {"total": round(total, 2), "max": 5.0, "rating": rating,
            "dimension_scores": scores, "dimension_details": details}


def _monthly_heatmap(dates, monthly_rets):
    rows = {}
    for d, r in zip(dates[1:], monthly_rets):
        dt = pd.Timestamp(d)
        rows.setdefault(dt.year, {})[dt.month] = round(float(r) * 100, 2)
    years = sorted(rows.keys())
    months = list(range(1, 13))
    data = [[rows.get(yr, {}).get(mo, None) for mo in months] for yr in years]
    return {"years": years, "months": months, "data": data}
