"""
engine/gpu_portfolio_compute.py
================================
OBQ FactorLab CYC-008 — GPU-Accelerated Portfolio Model Backtest Engine

Architecture (aligned with factor backtest for future GPU optimization):
  GPU:  Batch-rank all stocks by factor score each period → top-N indices
        (n_factors, n_periods, top_n) integer tensor in VRAM.
        This is the expensive step — O(n_factors × n_periods × n_stocks log n_stocks)
  CPU:  Per-factor sequential portfolio simulation using pre-ranked indices:
        equity curve, turnover, cost drag, holdings log.
        Sequential by design — holdings carry over between periods.

Staggered tranche structure (CYC-008 config):
  - total_stocks = 28   (top 28 by factor score)
  - n_tranches   = 4    (4 groups of 7)
  - hold_months  = 12   (each tranche holds 1 year)
  - stagger      = quarterly (tranche 1→Mar, 2→Jun, 3→Sep, 4→Dec)
  - No sector cap — pure factor signal
  - Equal weight within tranche at rebalance

Benchmark: SPX (from pwb_data.duckdb PWB_indices) via spx_backtest.py
OBQ Port Score: computed post-batch (percentile drawdown component requires full batch)
"""

from __future__ import annotations

import os
import time
import math
import logging
from typing import Optional

import numpy as np
import pandas as pd

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import cupy as cp

log = logging.getLogger(__name__)


# ── OBQ Port Score ─────────────────────────────────────────────────────────────

def compute_obq_port_score(pm: dict, dd_pct_rank: float) -> tuple[float, dict]:
    """
    Compute OBQ Port Score and all 5 sub-components.

    Args:
        pm:           portfolio_metrics dict from compute_all()
        dd_pct_rank:  percentile rank of this strategy's drawdown control
                      within the full batch (0=worst DD, 1=best DD)
                      computed AFTER the full batch is run.

    Returns:
        (obq_port_score, components_dict)
    """
    def _tanh(x): return float(np.tanh(x))
    def _safe(v, default=0.0): return float(v) if v is not None and np.isfinite(float(v)) else default

    calmar   = _safe(pm.get("calmar_gips") or pm.get("calmar"))
    sortino  = _safe(pm.get("sortino"))
    win_rate = _safe(pm.get("win_rate_monthly"))
    iudr     = _safe(pm.get("iudr"))
    eq_r2    = _safe(pm.get("equity_r2"))
    surefire = _safe(pm.get("surefire_ratio"))
    alpha    = _safe(pm.get("alpha"))
    up_cap   = _safe(pm.get("up_capture"), 1.0)
    dn_cap   = _safe(pm.get("down_capture"), 1.0)

    # 1. Return — Calmar (CAGR/MaxDD) + Sortino (downside-only risk)
    #    No Sharpe — it penalizes upside vol
    ret_comp = (0.60 * _tanh(calmar / 0.50)
              + 0.40 * _tanh(sortino / 1.50))

    # 2. Consistency — Win rate + IUDR curve shape
    #    Win rate: >60% good, 50% neutral; IUDR: >3 excellent shape
    cons_comp = (0.50 * _tanh((win_rate - 0.50) / 0.10)
               + 0.50 * _tanh(iudr / 3.0))

    # 3. Smoothness — R² (how close to straight line) + Surefire
    #    R² is already 0-1; Surefire scale: ~5 = strong shape×magnitude
    smooth_comp = (0.60 * float(np.clip(eq_r2, 0, 1))
                 + 0.40 * _tanh(surefire / 5.0))

    # 4. Alpha + Capture asymmetry
    #    Up capture: >110% = getting more than SPX on rallies (scale: 110% → midpoint)
    #    Down capture: <80% = shedding 20% of SPX drops (scale: (2-0.8)/0.4 = 3.0 → midpoint)
    alpha_comp = (0.40 * _tanh(alpha / 0.06)
                + 0.35 * _tanh(up_cap / 1.10)
                + 0.25 * _tanh((2.0 - dn_cap) / 0.40))

    # 5. Drawdown — percentile rank within batch (0=worst, 1=best)
    #    Purely relative — no threshold needed
    dd_comp = float(np.clip(dd_pct_rank, 0.0, 1.0))

    # Weighted sum — equal 20% each
    score = (0.20 * ret_comp
           + 0.20 * cons_comp
           + 0.20 * smooth_comp
           + 0.20 * alpha_comp
           + 0.20 * dd_comp)

    score = float(np.clip(score, -1.0, 1.0))

    components = {
        "return_comp":       round(ret_comp,    4),
        "consistency_comp":  round(cons_comp,   4),
        "smoothness_comp":   round(smooth_comp, 4),
        "alpha_capture_comp":round(alpha_comp,  4),
        "drawdown_comp":     round(dd_comp,     4),
        "obq_port_score":    round(score,       4),
    }
    return score, components


def compute_dd_percentile_ranks(results: list[dict]) -> list[float]:
    """
    Given a list of completed portfolio results, compute the drawdown
    percentile rank for each (used in OBQ Port Score drawdown component).
    Lower combined drawdown = higher rank (closer to 1.0).

    dd_score = 0.55 × (-max_dd) + 0.45 × (-avg_ann_dd)
    Both are negative numbers; negating makes lower drawdown = higher value.
    """
    scores = []
    for r in results:
        pm = r.get("portfolio_metrics", {})
        max_dd   = float(pm.get("max_dd",     0) or 0)
        avg_andd = float(pm.get("avg_ann_dd", 0) or 0)
        # Combine: higher value = better drawdown control
        scores.append(0.55 * (-max_dd) + 0.45 * (-avg_andd))

    scores_arr = np.array(scores)
    n = len(scores_arr)
    if n == 0:
        return []
    if n == 1:
        return [0.5]

    # Percentile rank: fraction of others this score beats
    ranks = []
    for i, s in enumerate(scores_arr):
        rank = float((scores_arr < s).sum()) / (n - 1)
        ranks.append(rank)
    return ranks


# ── GPU top-N selection ────────────────────────────────────────────────────────

def gpu_topn_selection(
    scores_gpu:   "cp.ndarray",   # (n_periods, max_stocks) float64
    valid_gpu:    "cp.ndarray",   # (n_periods, max_stocks) bool
    top_n:        int,
    lower_better: bool = False,
    cap_mask_gpu: Optional["cp.ndarray"] = None,
) -> "cp.ndarray":
    """
    GPU vectorized top-N stock selection per period.

    Returns: (n_periods, top_n) int32 — column indices of top-N stocks each period.
    -1 = not enough stocks in period.
    """
    if cap_mask_gpu is not None:
        eff_valid = valid_gpu & cap_mask_gpu
    else:
        eff_valid = valid_gpu

    # Mask invalid scores
    score_valid = eff_valid & ~cp.isnan(scores_gpu)

    # Replace invalid with -inf (or +inf if lower_better) so they sort last
    if lower_better:
        masked = cp.where(score_valid, -scores_gpu, cp.float64(cp.inf))
    else:
        masked = cp.where(score_valid, scores_gpu, cp.float64(-cp.inf))

    n_periods, max_stocks = masked.shape

    # argsort descending — top scores first
    # cupy argsort ascending; negate for descending
    sorted_idx = cp.argsort(-masked, axis=1)  # (n_periods, max_stocks)

    # Take first top_n columns
    top_idx = sorted_idx[:, :top_n].astype(cp.int32)  # (n_periods, top_n)

    # Mark positions where we didn't have enough valid stocks as -1
    nv = score_valid.sum(axis=1)  # (n_periods,)
    for t in range(n_periods):
        if int(nv[t]) < top_n:
            top_idx[t, int(nv[t]):] = cp.int32(-1)

    return top_idx


# ── CPU portfolio simulation ───────────────────────────────────────────────────

def simulate_tranche_portfolio(
    top_idx_np:     np.ndarray,    # (n_periods, top_n) int32 — GPU output, CPU copy
    returns_np:     np.ndarray,    # (n_periods, max_stocks) float64
    scores_np:      np.ndarray,    # (n_periods, max_stocks) float64 for factor score logging
    market_cap_np:  Optional[np.ndarray],  # (n_periods, max_stocks)
    dates:          list[str],
    total_stocks:   int  = 28,
    n_tranches:     int  = 4,
    hold_months:    int  = 12,
    cost_bps:       float = 15.0,
    score_column:   str  = "",
) -> dict:
    """
    CPU sequential simulation of staggered-tranche portfolio.

    Tranche structure:
      - 4 tranches of (total_stocks // 4) = 7 stocks each
      - Tranche k rebalances at periods where month ∈ {3,6,9,12}[k-1]
      - Each tranche holds for hold_months (12mo = annual rebalance)
      - Combined portfolio = equal-weight blend of 4 tranche equity curves
      - Top-N pre-computed by GPU (top_idx_np)

    Returns full result dict compatible with portfolio_bank schema.
    """
    t0 = time.time()

    n_periods  = len(dates)
    per_tranche = total_stocks // n_tranches  # 7 stocks per tranche
    cost_one_way = cost_bps / 10000.0

    # Determine rebalance period for each tranche
    # Quarterly stagger: tranche 1→Mar(3), 2→Jun(6), 3→Sep(9), 4→Dec(12)
    tranche_months = {1: 3, 2: 6, 3: 9, 4: 12}

    # ── Per-tranche simulation ────────────────────────────────────────────────
    tranche_equity_curves = {}   # t_idx -> [float] length n_periods+1
    tranche_rebal_periods = {}   # t_idx -> [period_idx]
    tranche_holdings_count = {}  # t_idx -> [int] length n_periods — actual stocks held

    for t_idx in range(1, n_tranches + 1):
        rebal_month = tranche_months[t_idx]

        # Find periods where this tranche rebalances
        rebal_periods = [i for i, d in enumerate(dates)
                         if int(d[5:7]) == rebal_month]

        tranche_rebal_periods[t_idx] = rebal_periods

        # Start equity at 1.0, simulate period-by-period
        eq = [1.0]
        prev_holdings = set()
        holdings_per_period = []

        for i in range(n_periods):
            if i in rebal_periods:
                # Rebalance: pick top per_tranche stocks for THIS tranche
                #   Tranche 1: positions 0..6 (rank 1-7)
                #   Tranche 2: positions 7..13 (rank 8-14)
                #   Tranche 3: positions 14..20 (rank 15-21)
                #   Tranche 4: positions 21..27 (rank 22-28)
                offset = (t_idx - 1) * per_tranche
                new_idx = top_idx_np[i, offset:offset + per_tranche]
                new_holdings = set(int(x) for x in new_idx if x >= 0)

                # Turnover cost
                exits   = prev_holdings - new_holdings
                entries = new_holdings - prev_holdings
                turnover = (len(exits) + len(entries)) / max(len(new_holdings), 1)
                cost_drag = turnover * cost_one_way

                prev_holdings = new_holdings
            else:
                cost_drag = 0.0

            holdings_per_period.append(len(prev_holdings))

            # Return for this period: equal-weight avg of holdings
            if prev_holdings:
                period_rets = []
                for col_idx in prev_holdings:
                    if col_idx < returns_np.shape[1]:
                        r = returns_np[i, col_idx]
                        if np.isfinite(r):
                            period_rets.append(float(np.clip(r, -0.95, 3.0)))
                if period_rets:
                    raw_ret = float(np.mean(period_rets))
                    period_ret = float(np.clip(raw_ret, -0.80, 1.50)) - cost_drag
                else:
                    period_ret = -cost_drag
            else:
                period_ret = 0.0

            eq.append(eq[-1] * (1.0 + period_ret))

        tranche_equity_curves[t_idx] = eq
        tranche_holdings_count[t_idx] = holdings_per_period

    # ── Blend tranches into combined portfolio equity ─────────────────────────
    # At each period, blend equity values (equal weight across tranches)
    # Normalize each tranche to start at 1.0 (they already do)
    portfolio_equity = []
    for i in range(n_periods + 1):
        vals = [tranche_equity_curves[t][i] for t in range(1, n_tranches + 1)
                if i < len(tranche_equity_curves[t])]
        portfolio_equity.append(float(np.mean(vals)) if vals else 1.0)

    # Re-normalize to 1.0 at start
    base = portfolio_equity[0]
    portfolio_equity = [v / base for v in portfolio_equity]

    # ── Period data for heatmap ───────────────────────────────────────────────
    period_data = []
    equity_dates = dates  # one equity value per period end

    for i in range(n_periods):
        if i + 1 < len(portfolio_equity):
            period_ret = portfolio_equity[i + 1] / portfolio_equity[i] - 1.0
        else:
            period_ret = 0.0

        # Collect all active holdings across all tranches at period i
        active_holdings = set()
        for t_idx in range(1, n_tranches + 1):
            if i in tranche_rebal_periods.get(t_idx, []) or i > 0:
                # Use the holdings active at this period
                # Find last rebal at or before i for this tranche
                rebal_ps = [p for p in tranche_rebal_periods.get(t_idx, []) if p <= i]
                if rebal_ps:
                    last_rebal = max(rebal_ps)
                    offset = (t_idx - 1) * per_tranche
                    for col_idx in top_idx_np[last_rebal, offset:offset + per_tranche]:
                        if col_idx >= 0:
                            active_holdings.add(int(col_idx))

        period_data.append({
            "date":             dates[i],
            "portfolio_return": round(period_ret, 6),
            "n_stocks":         len(active_holdings),
            "turnover_pct":     0.0,
        })

    # ── Holdings log ─────────────────────────────────────────────────────────
    # Stored as list of {date, holdings: [{symbol_idx, score, weight}]}
    holdings_log = []
    for i in range(n_periods):
        holds = []
        for t_idx in range(1, n_tranches + 1):
            rebal_ps = [p for p in tranche_rebal_periods.get(t_idx, []) if p <= i]
            if not rebal_ps:
                continue
            last_rebal = max(rebal_ps)
            offset = (t_idx - 1) * per_tranche
            for col_idx in top_idx_np[last_rebal, offset:offset + per_tranche]:
                if col_idx < 0:
                    continue
                col_idx = int(col_idx)
                score_val = float(scores_np[last_rebal, col_idx]) if col_idx < scores_np.shape[1] else None
                mc_val = None
                if market_cap_np is not None and col_idx < market_cap_np.shape[1]:
                    mc = market_cap_np[i, col_idx]
                    mc_val = round(float(mc) / 1e9, 3) if np.isfinite(mc) and mc > 0 else None
                holds.append({
                    "col_idx":    col_idx,
                    "score":      round(score_val, 4) if score_val and np.isfinite(score_val) else None,
                    "weight":     round(1.0 / per_tranche, 4),
                    "tranche":    t_idx,
                    "market_cap": mc_val,
                })
        holdings_log.append({"date": dates[i], "holdings": holds})

    elapsed = round(time.time() - t0, 2)

    # ── Holdings coverage quality metrics ─────────────────────────────────────
    # For each period, sum actual holdings across all tranches vs target
    total_holdings_per_period = []
    for i in range(n_periods):
        total_held = sum(tranche_holdings_count[t][i] for t in range(1, n_tranches + 1))
        total_holdings_per_period.append(total_held)

    target_total = total_stocks  # 28
    full_periods = sum(1 for h in total_holdings_per_period if h >= target_total)
    min_holdings = min(total_holdings_per_period) if total_holdings_per_period else 0
    avg_holdings = sum(total_holdings_per_period) / len(total_holdings_per_period) if total_holdings_per_period else 0
    coverage_pct = full_periods / n_periods if n_periods > 0 else 0.0  # fraction of periods with full holdings

    return {
        "portfolio_equity":  [round(v, 6) for v in portfolio_equity],
        "equity_dates":      equity_dates,
        "period_data":       period_data,
        "holdings_log":      holdings_log,
        "n_periods":         n_periods,
        "elapsed_s":         elapsed,
        # Quality metrics
        "coverage_pct":      round(coverage_pct, 4),   # 1.0 = full holdings every period
        "min_holdings":      min_holdings,              # lowest actual holdings in any period
        "avg_holdings":      round(avg_holdings, 1),    # average holdings across all periods
        "full_periods":      full_periods,              # periods with >= target_total stocks
    }


# ── SPX benchmark loader ───────────────────────────────────────────────────────

_SPX_CACHE: dict = {}  # cache so we only load once per run

def load_spx_benchmark(start_date: str, end_date: str, equity_dates: list[str]) -> dict:
    """
    Load SPX total return benchmark and resample to portfolio's quarterly dates.
    Uses spx_backtest which splices SPX price + SPY total return.
    Returns spy_metrics + spy_equity aligned to equity_dates.
    """
    global _SPX_CACHE
    cache_key = f"{start_date}_{end_date}"

    if cache_key not in _SPX_CACHE:
        from engine.spx_backtest import run_spx_backtest
        # Request 6 months earlier so we always have SPX value BEFORE the first portfolio date
        import datetime
        try:
            dt = datetime.date.fromisoformat(start_date)
            early_start = (dt - datetime.timedelta(days=180)).isoformat()
        except Exception:
            early_start = start_date
        result = run_spx_backtest(start_date=early_start, end_date=end_date)
        _SPX_CACHE[cache_key] = result

    spx_result = _SPX_CACHE[cache_key]
    if spx_result.get("status") != "complete":
        return {"spy_metrics": {}, "spy_equity": []}

    spx_raw_dates  = [str(d)[:10] for d in spx_result.get("equity_dates", [])]
    spx_raw_equity = spx_result.get("portfolio_equity", [])
    spx_metrics    = spx_result.get("portfolio_metrics", {})

    if not spx_raw_dates or not spx_raw_equity:
        return {"spy_metrics": spx_metrics, "spy_equity": []}

    # Build date → equity map
    spx_date_map = {d: v for d, v in zip(spx_raw_dates, spx_raw_equity)}
    sorted_spx = sorted(spx_date_map.keys())

    def _nearest_spx(date_str):
        cands = [d for d in sorted_spx if d <= date_str]
        if cands:
            return spx_date_map[cands[-1]]
        return spx_date_map[sorted_spx[0]] if sorted_spx else 1.0

    # Resample to n_periods+1 points: one START point + one per equity_date
    # START point = SPX value just before the first equity_date (one quarter prior)
    # This gives aligned period returns: spx_ret[i] = spx[i+1]/spx[i] - 1
    first_date = str(equity_dates[0])[:10]
    # Find SPX value at the quarter BEFORE the first period date
    # Use the largest SPX date that is strictly BEFORE first_date
    pre_cands = [d for d in sorted_spx if d < first_date]
    if pre_cands:
        start_val = spx_date_map[pre_cands[-1]]
    else:
        # No prior data — use the first equity_date value as both start and period 1 end
        # This means period 1 SPX return = 0, but better than wrong alignment
        start_val = _nearest_spx(first_date)

    # Build n_periods+1 equity: [start, end_p1, end_p2, ..., end_p120]
    spx_resampled = [start_val] + [_nearest_spx(str(d)[:10]) for d in equity_dates]

    # Normalize to start at 1.0
    if spx_resampled and spx_resampled[0] > 0:
        base = spx_resampled[0]
        spx_equity = [round(v / base, 6) for v in spx_resampled]
    else:
        spx_equity = spx_resampled

    return {"spy_metrics": spx_metrics, "spy_equity": spx_equity}


# ── Annual returns helper ──────────────────────────────────────────────────────

def compute_annual_returns(equity_dates: list, equity: list, spx_equity: list) -> list:
    """Annual returns by calendar year for portfolio and SPX."""
    from engine.portfolio_backtest import _compute_annual_returns
    return _compute_annual_returns(equity_dates, equity, [], spx_equity)


def compute_monthly_heatmap(equity_dates: list, period_rets: list, ppy: int = 4) -> dict:
    """Period returns heatmap — year × period."""
    from engine.portfolio_backtest import _build_monthly_heatmap
    return _build_monthly_heatmap(equity_dates, period_rets, ppy)


# ── Main batch entry point ─────────────────────────────────────────────────────

def run_portfolio_batch_gpu(
    pack,                          # GPUDataPack from gpu_data_loader
    factor_configs: list[dict],    # [{score_column, lower_better, display_name, ...}]
    total_stocks:   int   = 28,
    n_tranches:     int   = 4,
    hold_months:    int   = 12,
    cost_bps:       float = 15.0,
    start_date:     str   = "1995-03-31",
    end_date:       str   = "2024-12-31",
    cap_tier:       str   = "All_Cap",
    cap_mask_gpu:   Optional["cp.ndarray"] = None,
    cycle_tag:      str   = "CYC-008-PM",
    save:           bool  = True,
    overwrite:      bool  = True,
    cb = None,
) -> list[dict]:
    """
    Batch-run portfolio models for all factor_configs.

    Step 1 (GPU):  For each factor, compute top-N stock selection per period.
    Step 2 (CPU):  Sequential tranche simulation using pre-ranked stock indices.
    Step 3 (CPU):  Metrics, OBQ Port Score (post-batch percentile drawdown).
    Step 4 (DB):   Save to portfolio bank.

    Returns list of result dicts.
    """
    def _cb(msg):
        if cb:
            try: cb("info", msg)
            except: pass

    t_start = time.time()
    top_n       = total_stocks
    ppy         = round(12 / hold_months * n_tranches / n_tranches)  # quarterly = 4
    # For staggered 4-tranche quarterly: effective observation is quarterly (ppy=4)
    ppy = 4

    _cb(f"Portfolio batch: {len(factor_configs)} factors | Top-{top_n} | "
        f"{n_tranches}T-Qtrly | {hold_months}mo hold | {cost_bps}bps")

    # Get CPU copies of arrays we'll need for simulation
    returns_np    = cp.asnumpy(pack.returns_gpu)
    market_cap_np = cp.asnumpy(pack.market_cap_gpu) if hasattr(pack, 'market_cap_gpu') else None

    # Preload SPX benchmark (cached after first call)
    dates = pack.dates
    _cb("Loading SPX benchmark...")
    spx_data = load_spx_benchmark(dates[0], dates[-1],
                                   equity_dates=dates)
    spx_equity  = spx_data["spy_equity"]
    spx_metrics = spx_data["spy_metrics"]

    results = []

    for job_num, cfg in enumerate(factor_configs, start=1):
        score_col    = cfg["score_column"]
        lower_better = cfg.get("lower_better", False)
        display_name = cfg.get("display_name", score_col)
        # Per-factor cap mask overrides batch-level cap_mask_gpu
        job_cap_mask = cfg.get("cap_mask_gpu", None) or cap_mask_gpu

        if score_col not in pack.score_columns:
            _cb(f"  [{job_num}/{len(factor_configs)}] SKIP {score_col} — not in VRAM")
            continue

        scores_gpu = pack.score_columns[score_col]

        # ── Pre-flight: check score coverage vs top_n requirement ──────────────
        if job_cap_mask is not None:
            eff_valid = pack.valid_gpu & job_cap_mask & ~cp.isnan(scores_gpu)
        else:
            eff_valid = pack.valid_gpu & ~cp.isnan(scores_gpu)
        nv_per_period = cp.asnumpy(eff_valid.sum(axis=1))
        full_periods = int((nv_per_period >= top_n).sum())
        coverage_ratio = full_periods / len(nv_per_period) if len(nv_per_period) > 0 else 0
        min_valid = int(nv_per_period.min())

        if coverage_ratio < 0.10:
            # Hard reject: factor has almost no valid scores (like cyc4_eps_stability)
            _cb(f"  [{job_num}/{len(factor_configs)}] SKIP {score_col} — "
                f"only {full_periods}/{len(nv_per_period)} periods have >={top_n} stocks "
                f"(min={min_valid}, coverage={coverage_ratio:.0%}) — insufficient data")
            continue

        # ── Step 1: GPU top-N selection ────────────────────────────────────────
        t_gpu = time.perf_counter()
        top_idx_gpu = gpu_topn_selection(
            scores_gpu=scores_gpu,
            valid_gpu=pack.valid_gpu,
            top_n=top_n,
            lower_better=lower_better,
            cap_mask_gpu=job_cap_mask,
        )
        cp.cuda.Stream.null.synchronize()
        gpu_ms = round((time.perf_counter() - t_gpu) * 1000, 1)

        top_idx_np  = cp.asnumpy(top_idx_gpu)
        scores_np   = cp.asnumpy(scores_gpu)

        # ── Step 2: CPU tranche simulation ────────────────────────────────────
        sim = simulate_tranche_portfolio(
            top_idx_np    = top_idx_np,
            returns_np    = returns_np,
            scores_np     = scores_np,
            market_cap_np = market_cap_np,
            dates         = dates,
            total_stocks  = total_stocks,
            n_tranches    = n_tranches,
            hold_months   = hold_months,
            cost_bps      = cost_bps,
            score_column  = score_col,
        )

        port_equity  = sim["portfolio_equity"]
        equity_dates = sim["equity_dates"]
        period_data  = sim["period_data"]
        holdings_log = sim["holdings_log"]

        # ── Step 3: Metrics ────────────────────────────────────────────────────
        from engine.metrics import compute_all

        port_eq_arr  = np.array(port_equity, dtype=np.float64)
        port_ret_arr = np.diff(port_eq_arr) / port_eq_arr[:-1]

        # ── SPX alignment ─────────────────────────────────────────────────────
        # port_equity:  n_periods+1 points  [1.0, v1, v2, ..., v120]
        # port_ret_arr: n_periods   returns [r1, r2, ..., r120]
        # spx_equity:   n_periods   points  [s1, s2, ..., s120] — period-end values
        #
        # compute_all needs:  equity (n+1), monthly_ret (n), bm_equity (n+1), bm_monthly (n)
        # So bm_equity = [1.0, s1_norm, s2_norm, ...] — prepend 1.0, renormalize
        # spx_equity is now n_periods+1 points: [start, end_p1, ..., end_p120]
        # Already normalized to 1.0 at start. Directly matches port_equity structure.
        if spx_equity and len(spx_equity) >= 2:
            spx_eq_arr = np.array(spx_equity, dtype=np.float64)
            # Trim/pad to match port_eq_arr length
            n = len(port_eq_arr)
            if len(spx_eq_arr) > n:
                spx_eq_arr = spx_eq_arr[:n]
            elif len(spx_eq_arr) < n:
                spx_eq_arr = np.concatenate([spx_eq_arr,
                                             np.full(n - len(spx_eq_arr), spx_eq_arr[-1])])
            spx_ret_arr = np.diff(spx_eq_arr) / np.maximum(spx_eq_arr[:-1], 1e-10)
        else:
            spx_eq_arr  = None
            spx_ret_arr = None

        # Both return arrays must be same length — clip to shorter if needed
        if spx_ret_arr is not None and len(spx_ret_arr) != len(port_ret_arr):
            n_min = min(len(spx_ret_arr), len(port_ret_arr))
            port_ret_arr = port_ret_arr[:n_min]
            spx_ret_arr  = spx_ret_arr[:n_min]
            port_eq_arr  = np.concatenate([[1.0], np.cumprod(1.0 + port_ret_arr)])
            spx_eq_arr   = np.concatenate([[1.0], np.cumprod(1.0 + spx_ret_arr)])

        portfolio_metrics = compute_all(
            equity          = port_eq_arr,
            monthly_ret     = port_ret_arr,
            bm_equity       = spx_eq_arr,
            bm_monthly      = spx_ret_arr,
            periods_per_year= ppy,
            label           = f"{display_name} | Top-{top_n} | {n_tranches}T-Qtrly",
        )

        # Annual returns + monthly heatmap
        annual_ret_by_year = compute_annual_returns(
            equity_dates, port_equity,
            list(spx_equity[:len(port_equity)]) if spx_equity else []
        )
        monthly_heatmap = compute_monthly_heatmap(
            equity_dates,
            port_ret_arr.tolist(),
            ppy=ppy,
        )

        # Trade log (flat list from holdings_log)
        trade_log = []
        for i, hlog in enumerate(holdings_log):
            d_entry = hlog["date"]
            d_exit  = equity_dates[i + 1] if i + 1 < len(equity_dates) else d_entry
            for h in hlog["holdings"]:
                col  = h["col_idx"]
                ep   = float(returns_np[i, col]) if col < returns_np.shape[1] else None
                trade_log.append({
                    "entry_date":   d_entry,
                    "col_idx":      col,
                    "exit_date":    d_exit,
                    "score":        h.get("score"),
                    "weight":       h.get("weight"),
                    "tranche":      h.get("tranche"),
                    "market_cap_B": h.get("market_cap"),
                })

        # ── Build result dict ──────────────────────────────────────────────────
        # Accurate run_label — factor name | config | cycle tag
        run_label = (
            f"{display_name} | Top-{top_n} | {n_tranches}T-Qtrly | Equal-Wt | "
            f"{cap_tier} | {start_date[:4]}-{end_date[:4]} [{cycle_tag}]"
        )

        result = {
            "status":            "complete",
            "run_label":         run_label,
            "score_column":      score_col,
            "display_name":      display_name,
            "config": {
                "score_column":    score_col,
                "display_name":    display_name,
                "top_n":           top_n,
                "n_tranches":      n_tranches,
                "top_per_tranche": top_n // n_tranches,
                "sector_max":      0,
                "rebalance_freq":  "quarterly",
                "hold_months":     hold_months,
                "start_date":      start_date,
                "end_date":        end_date,
                "cap_tier":        cap_tier,
                "cost_bps":        cost_bps,
                "stop_loss_pct":   0.0,
                "weight_scheme":   "equal",
                "benchmark":       "SPX",
            },
            "portfolio_equity":   [round(v, 6) for v in port_equity],
            "bm_equity":          [],
            "spy_equity":         [round(v, 6) for v in (spx_equity[:len(port_equity)] if spx_equity else [])],
            "equity_dates":       equity_dates,
            "portfolio_metrics":  portfolio_metrics,
            "bm_metrics":         {},
            "spy_metrics":        spx_metrics,
            "period_data":        period_data,
            "annual_ret_by_year": annual_ret_by_year,
            "monthly_heatmap":    monthly_heatmap,
            "holdings_log":       holdings_log,
            "trade_log":          trade_log,
            "n_periods":          sim["n_periods"],
            "elapsed_s":          round(time.time() - t_start, 1),
            "elapsed_gpu_ms":     gpu_ms,
            # Holdings coverage quality
            "coverage_pct":       sim.get("coverage_pct", 0),
            "min_holdings":       sim.get("min_holdings", 0),
            "avg_holdings":       sim.get("avg_holdings", 0),
            "full_periods":       sim.get("full_periods", 0),
            # OBQ Port Score — filled in post-batch after percentile rank computed
            "obq_port_score":     None,
            "port_components":    None,
        }

        results.append(result)

        pm = portfolio_metrics
        cov = sim.get("coverage_pct", 0)
        cov_flag = "" if cov >= 1.0 else f" COV={cov:.0%}"
        _cb(
            f"  [{job_num}/{len(factor_configs)}] {score_col} | "
            f"CAGR={pm.get('cagr',0)*100:.1f}% | "
            f"Calmar={pm.get('calmar_gips',0):.2f} | "
            f"MaxDD={pm.get('max_dd',0)*100:.1f}% | "
            f"Avg={sim.get('avg_holdings',0):.0f}/{total_stocks} stocks | "
            f"GPU={gpu_ms}ms{cov_flag}"
        )

    # ── Post-batch: OBQ Port Score (needs full batch for percentile DD) ────────
    _cb(f"Computing OBQ Port Scores ({len(results)} results)...")
    dd_ranks = compute_dd_percentile_ranks(results)

    for i, result in enumerate(results):
        dd_pct = dd_ranks[i] if i < len(dd_ranks) else 0.5
        score, components = compute_obq_port_score(
            result["portfolio_metrics"], dd_pct
        )
        result["obq_port_score"]  = score
        result["port_components"] = components
        # Add individual components to portfolio_metrics for easy DB storage
        result["portfolio_metrics"].update(components)

    # ── Save to portfolio bank (batch mode — single connection) ──────────────
    if save:
        _cb(f"Saving {len(results)} portfolio models to bank...")
        from engine.portfolio_bank import save_portfolio_model, _get_bank
        saved = 0
        con = _get_bank()
        try:
            for result in results:
                try:
                    save_portfolio_model(result, overwrite=overwrite, con=con)
                    saved += 1
                except Exception as e:
                    _cb(f"  Save error for {result.get('score_column')}: {e}")
            con.commit()
        finally:
            con.close()
        _cb(f"  Saved {saved}/{len(results)}")

    total_s = round(time.time() - t_start, 1)
    _cb(f"Portfolio batch complete: {len(results)} models | {total_s}s total")
    return results
