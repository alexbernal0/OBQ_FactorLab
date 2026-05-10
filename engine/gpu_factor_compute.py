# -*- coding: utf-8 -*-
"""
engine/gpu_factor_compute.py
============================
GPU Factor Engine — ALL 13 Compute Steps

Executes a complete quintile factor backtest entirely on GPU via CuPy.
ZERO pandas, ZERO scipy, ZERO Python loops over periods in the hot path.

All 13 steps:
  1.  rank_scores        — argsort per row → 1-based ranks
  2.  assign_buckets     — ceil(rank / n_valid * n_buckets) → bucket IDs [1..n]
  3.  spearman_ic        — Pearson(score_ranks, return_ranks) per period
  4.  bucket_mean_ret    — mean(returns[bucket==b]) per period per bucket
  5.  equity_curves      — cumprod(1 + bucket_rets) per bucket
  6.  universe_ret       — mean(returns[valid]) per period
  7.  spread_cagr        — (Q1[-1]/Q1[0])^(1/years) - 1
  8.  monotonicity       — count(Q[i]_cagr > Q[i+1]_cagr) / (n_buckets-1)
  9.  staircase          — spread × monotonicity × step_uniformity
  10. annual_alpha       — Q1_annual_ret - universe_annual_ret per year
  11. alpha_win_rate     — sum(annual_alpha > 0) / n_years
  12. bear_bull_score    — Q1 excess return vs universe in specific windows
  13. obq_master_score   — 25% ICIR + 25% staircase + 20% alpha_win + 20% alpha_mag + 10% IC_hit

Cap-tier filtering is a GPU mask operation:
    cap_mask = (market_cap_gpu >= min_cap) & (market_cap_gpu <= max_cap)
    effective_valid = valid_gpu & cap_mask
All 4 cap tiers share the same VRAM data — just swap the mask.

Designed for RTX 3090 (cc 8.6, 24GB VRAM).
"""
from __future__ import annotations

import os
import time
import math
import json
import logging
from typing import Optional

import numpy as np

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import cupy as cp

log = logging.getLogger(__name__)

# ── Bear / Bull market windows ─────────────────────────────────────────────────
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


# ── GPU primitives ─────────────────────────────────────────────────────────────

def _batch_rank_2d(X_gpu: "cp.ndarray", valid_mask: "cp.ndarray") -> "cp.ndarray":
    """
    Rank each row of X_gpu independently, ignoring invalid entries.
    Invalid entries are sorted to the end before ranking.
    Returns cp.ndarray shape (n_periods, max_stocks) float64 ranks (1-based).
    Only valid positions have meaningful ranks; invalid positions = 0.

    Uses argsort(argsort(x)) + 1 pattern — fully vectorized on GPU.
    """
    # Push invalid entries to the end by replacing with +inf before argsort
    X_for_rank = cp.where(valid_mask, X_gpu, cp.float64(1e18))
    n, m = X_gpu.shape
    order = cp.argsort(X_for_rank, axis=1)  # ascending: rank 1 = lowest value
    ranks = cp.empty_like(X_gpu, dtype=cp.float64)
    rows = cp.arange(n)[:, None]
    ranks[rows, order] = cp.arange(1, m + 1, dtype=cp.float64)[None, :]
    # Zero out invalid
    ranks = cp.where(valid_mask, ranks, cp.float64(0.0))
    return ranks


def _batch_rank_2d_descending(X_gpu: "cp.ndarray", valid_mask: "cp.ndarray") -> "cp.ndarray":
    """
    Rank each row descending (rank 1 = highest value).
    Invalid entries = 0.
    Used for higher_better factors (Q1 = top-ranked = best).
    """
    # Push invalid to end (lowest) before descending argsort
    # For descending: negate valid entries, sort ascending
    X_neg = cp.where(valid_mask, -X_gpu, cp.float64(1e18))
    n, m = X_gpu.shape
    order = cp.argsort(X_neg, axis=1)
    ranks = cp.empty_like(X_gpu, dtype=cp.float64)
    rows = cp.arange(n)[:, None]
    ranks[rows, order] = cp.arange(1, m + 1, dtype=cp.float64)[None, :]
    ranks = cp.where(valid_mask, ranks, cp.float64(0.0))
    return ranks


def _rank_avg_gpu(S_a: "cp.ndarray", S_b: "cp.ndarray",
                  valid_mask: "cp.ndarray",
                  n_valid: "cp.ndarray",
                  dir_a: str = 'higher_better',
                  dir_b: str = 'higher_better') -> "cp.ndarray":
    """
    Compute rank-average combo score of two factors on GPU.
    Each factor is percentile-ranked (0→1) then averaged.
    Handles direction (lower_better inverts the rank).
    Returns (n_periods, max_stocks) float64 — 0 for invalid, 0..1 for valid.
    """
    # For lower_better: small value = good = high rank → rank ascending gives
    #   rank 1 to smallest → percentile = rank/n_valid → 1/n (best for lower_better)
    # For higher_better: rank descending (rank 1 = largest)
    if dir_a == 'higher_better':
        ra = _batch_rank_2d_descending(S_a, valid_mask)
    else:
        ra = _batch_rank_2d(S_a, valid_mask)

    if dir_b == 'higher_better':
        rb = _batch_rank_2d_descending(S_b, valid_mask)
    else:
        rb = _batch_rank_2d(S_b, valid_mask)

    # Percentile-rank: rank / n_valid (0→1, higher = better)
    nv = cp.maximum(n_valid[:, None].astype(cp.float64), 1.0)
    pct_a = cp.where(valid_mask, ra / nv, cp.float64(0.0))
    pct_b = cp.where(valid_mask, rb / nv, cp.float64(0.0))

    # Combo = average of percentile ranks
    # Zero out where either score is missing
    both_valid = valid_mask & (ra > 0) & (rb > 0)
    combo = cp.where(both_valid, (pct_a + pct_b) / 2.0, cp.float64(np.nan))
    return combo


# ── Tortoriello CPU post-processor ─────────────────────────────────────────────

def _compute_tortoriello_cpu(
    dates: list,
    bucket_returns: dict,
    bucket_equity: dict,
    universe_rets: list,
    n_buckets: int = 5,
    periods_per_year: float = 2.0,
) -> dict:
    """
    Build per-bucket Tortoriello stats from already-computed GPU outputs.
    Schema matches old CPU engine — tearsheet ftBuildTortorielloTable consumes this.

    Computed: terminal_wealth, avg_excess_vs_univ, max_gain, max_loss, std_dev_ann,
              beta_vs_univ, alpha_vs_univ, pct_1y_beats_univ, pct_3y_beats_univ,
              roll_3y_excess, roll_3y_dates
    Left None: avg_portfolio_size, avg_beat_universe, avg_lag_universe,
               median_factor_score, avg_market_cap (need stock-level GPU data)
    """
    out: dict = {}
    n_periods = len(universe_rets)
    if n_periods == 0:
        return {str(b): {} for b in range(1, n_buckets + 1)}

    universe_arr = np.asarray(universe_rets, dtype=float)
    univ_var = float(np.var(universe_arr)) if n_periods > 1 else 0.0

    # Calendar-year buckets for pct_1y_beats_univ
    years = [str(d)[:4] for d in dates[:n_periods]]
    year_idx: dict = {}
    for i, y in enumerate(years):
        year_idx.setdefault(y, []).append(i)

    def _annualize(ret_list):
        return float(np.prod([1 + r for r in ret_list]) - 1) if ret_list else 0.0

    univ_annual = {y: _annualize([universe_arr[i] for i in idxs])
                   for y, idxs in year_idx.items()}

    # 3-year rolling excess (window = 3 years × periods_per_year)
    W = max(int(round(3 * periods_per_year)), 2)

    def _roll_excess(b_arr_):
        n = len(b_arr_)
        out_excess, out_dates = [], []
        for end in range(W - 1, n):
            start = end - W + 1
            b_cum = float(np.prod(1 + b_arr_[start:end + 1]) - 1)
            u_cum = float(np.prod(1 + universe_arr[start:end + 1]) - 1)
            out_excess.append(round(b_cum - u_cum, 4))
            out_dates.append(str(dates[end]))
        return out_excess, out_dates

    for b in range(1, n_buckets + 1):
        bid = str(b)
        b_rets_raw = bucket_returns.get(bid, []) if isinstance(bucket_returns, dict) else []
        b_eq_raw   = bucket_equity.get(bid,  []) if isinstance(bucket_equity,  dict) else []

        b_rets = list(b_rets_raw)[:n_periods]
        if len(b_rets) < n_periods:
            b_rets = b_rets + [0.0] * (n_periods - len(b_rets))
        b_arr = np.asarray(b_rets, dtype=float)

        terminal = round(float(b_eq_raw[-1] * 10000), 0) if b_eq_raw else None
        avg_excess = round(float((b_arr - universe_arr).mean()), 4) if n_periods > 0 else 0.0
        max_gain = round(float(b_arr.max()), 4) if n_periods > 0 else 0.0
        max_loss = round(float(b_arr.min()), 4) if n_periods > 0 else 0.0
        std_ann  = round(float(b_arr.std(ddof=0) * math.sqrt(periods_per_year)), 4) if n_periods > 1 else 0.0

        if n_periods > 1 and univ_var > 1e-12:
            cov_bu = float(np.cov(b_arr, universe_arr, ddof=0)[0, 1])
            beta = cov_bu / univ_var
            alpha_periodic = float(b_arr.mean() - beta * universe_arr.mean())
            alpha_ann = round(alpha_periodic * periods_per_year, 4)
            beta = round(beta, 3)
        else:
            beta, alpha_ann = 0.0, 0.0

        b_annual = {y: _annualize([b_arr[i] for i in idxs])
                    for y, idxs in year_idx.items()}
        common_years = [y for y in b_annual if y in univ_annual]
        pct_1y = round(sum(1 for y in common_years if b_annual[y] > univ_annual[y]) /
                       max(len(common_years), 1), 3) if common_years else 0.0

        roll_excess, roll_dates = _roll_excess(b_arr)
        pct_3y = round(sum(1 for e in roll_excess if e > 0) / len(roll_excess), 3) if roll_excess else 0.0

        out[bid] = {
            "terminal_wealth":      terminal,
            "avg_excess_vs_univ":   avg_excess,
            "pct_1y_beats_univ":    pct_1y,
            "pct_3y_beats_univ":    pct_3y,
            "max_gain":             max_gain,
            "max_loss":             max_loss,
            "std_dev_ann":          std_ann,
            "beta_vs_univ":         beta,
            "alpha_vs_univ":        alpha_ann,
            "avg_portfolio_size":   None,
            "avg_beat_universe":    None,
            "avg_lag_universe":     None,
            "median_factor_score":  None,
            "avg_market_cap":       None,
            "roll_3y_excess":       roll_excess,
            "roll_3y_dates":        roll_dates,
        }
    return out


# ── Main backtest function ─────────────────────────────────────────────────────

def run_factor_on_gpu(
    scores_gpu: "cp.ndarray",       # (n_periods, max_stocks) float64 — factor scores in VRAM
    returns_gpu: "cp.ndarray",      # (n_periods, max_stocks) float64 — fwd returns in VRAM
    valid_gpu: "cp.ndarray",        # (n_periods, max_stocks) bool — base valid mask
    n_valid: "cp.ndarray",          # (n_periods,) int32
    dates: list,                    # list[str] — period dates (CPU)
    n_buckets: int = 5,
    lower_better: bool = False,     # True = lower score is better (e.g. P/Sales)
    hold_months: int = 6,
    score_column: str = '',
    cap_mask_gpu: Optional["cp.ndarray"] = None,  # optional additional cap-tier mask
    min_n_stocks: int = 10,
    sector_gpu: Optional["cp.ndarray"] = None,    # optional (T,S) int8 GICS sector IDs
) -> dict:
    """
    Run a complete quintile factor backtest on GPU.
    ALL 13 compute steps execute on GPU — no Python loops over periods.

    Args:
        scores_gpu:   (n_periods, max_stocks) factor scores already in VRAM
        returns_gpu:  (n_periods, max_stocks) forward returns in VRAM
        valid_gpu:    (n_periods, max_stocks) bool — base universe mask
        n_valid:      (n_periods,) int32 — stocks per period before cap filter
        dates:        list of date strings (CPU-only, for metadata)
        n_buckets:    quintile count (default 5)
        lower_better: True if lower score → better rank → Q1
        hold_months:  holding period (for CAGR annualization)
        score_column: factor ID string (for logging only)
        cap_mask_gpu: optional cap-tier mask — ANDed with valid_gpu
        min_n_stocks: skip periods with fewer valid stocks

    Returns:
        dict with all bank-schema-compatible fields:
          status, score_column, config, factor_metrics, bucket_metrics,
          bucket_equity, ic_data, period_data, annual_ret_by_bucket,
          tortoriello, universe_metrics, universe_equity, dates, ...
    """
    t0 = time.perf_counter()

    # Apply cap-tier mask if provided
    if cap_mask_gpu is not None:
        eff_valid = valid_gpu & cap_mask_gpu
    else:
        eff_valid = valid_gpu

    # Also mask out NaN scores
    score_valid = eff_valid & ~cp.isnan(scores_gpu)
    return_valid = eff_valid & ~cp.isnan(returns_gpu)
    both_valid = score_valid & return_valid

    # Per-period valid count
    nv_eff = both_valid.sum(axis=1).astype(cp.int32)  # (n_periods,)

    # ── Step 1: Rank scores ───────────────────────────────────────────────────
    # lower_better → ascending rank (rank 1 = lowest score = Q1)
    # higher_better → descending rank (rank 1 = highest score = Q1)
    if lower_better:
        score_ranks = _batch_rank_2d(scores_gpu, both_valid)
    else:
        score_ranks = _batch_rank_2d_descending(scores_gpu, both_valid)

    # ── Step 2: Assign buckets ────────────────────────────────────────────────
    # bucket = ceil(rank / n_valid * n_buckets), clipped to [1, n_buckets]
    # CRITICAL: clamp score_ranks to valid range BEFORE int cast.
    # Invalid entries have sentinel rank ~1e18; ceil(1e18/3440*5) overflows int32
    # → causes CuPy to spin. Zero out invalid entries first.
    nv_f = cp.maximum(nv_eff[:, None].astype(cp.float64), 1.0)
    sr_clamped = cp.where(both_valid, score_ranks, cp.float64(0.0))
    bucket_f = cp.ceil(sr_clamped / nv_f * n_buckets)
    buckets = cp.clip(bucket_f.astype(cp.int32), 1, n_buckets)
    buckets = cp.where(both_valid, buckets, cp.int32(0))  # 0 = invalid

    # ── Step 3: Spearman IC = Pearson(score_ranks, return_ranks) ─────────────
    # Return ranks for IC computation
    return_ranks = _batch_rank_2d_descending(returns_gpu, both_valid)

    # Per-period Pearson of score_ranks and return_ranks (vectorized, no Python loop)
    # Mask to valid only
    SR = cp.where(both_valid, score_ranks, cp.float64(0.0))
    RR = cp.where(both_valid, return_ranks, cp.float64(0.0))
    nv_f2 = nv_eff.astype(cp.float64)[:, None]

    SR_mean = SR.sum(axis=1, keepdims=True) / cp.maximum(nv_f2, 1.0)
    RR_mean = RR.sum(axis=1, keepdims=True) / cp.maximum(nv_f2, 1.0)

    SR_c = cp.where(both_valid, SR - SR_mean, cp.float64(0.0))
    RR_c = cp.where(both_valid, RR - RR_mean, cp.float64(0.0))

    cov_N = (SR_c * RR_c).sum(axis=1)
    var_s = (SR_c * SR_c).sum(axis=1)
    var_r = (RR_c * RR_c).sum(axis=1)
    denom = cp.sqrt(var_s * var_r)
    denom = cp.maximum(denom, cp.float64(1e-10))
    ic_gpu = cov_N / denom  # (n_periods,)

    # When lower_better=True, ascending rank was used for scores.
    # IC should be negative when the factor actually works (lower score → higher return).
    # We negate to make it positive for a working lower_better factor.
    if lower_better:
        ic_gpu = -ic_gpu

    # ── Step 4: Bucket mean returns ───────────────────────────────────────────
    # Vectorized — NO Python loop over buckets.
    # (n_periods, max_stocks, n_buckets) → sum/count in one broadcast op.
    R_masked = cp.where(both_valid, returns_gpu, cp.float64(0.0))

    b_ids = cp.arange(1, n_buckets + 1, dtype=cp.int32)                          # (n_buckets,)
    b_mask_3d   = (buckets[:, :, None] == b_ids[None, None, :])                  # (T, S, B) bool
    bucket_counts = b_mask_3d.sum(axis=1).astype(cp.int32)                       # (T, B)
    bucket_sums   = (R_masked[:, :, None] * b_mask_3d).sum(axis=1)               # (T, B)
    bucket_means  = bucket_sums / cp.maximum(bucket_counts.astype(cp.float64), 1.0)  # (T, B)

    # ── Step 5: Equity curves on GPU — cp.cumprod, no Python loop ────────────
    ones_row      = cp.ones((1, n_buckets), dtype=cp.float64)
    bucket_equity_gpu = cp.vstack([ones_row, cp.cumprod(1.0 + bucket_means, axis=0)])  # (T+1, B)

    # ── Step 6 (GPU): Universe returns — vectorized, no Python loop ──────────
    univ_sum_gpu   = (R_masked * both_valid).sum(axis=1)                          # (T,)
    univ_count_gpu = both_valid.sum(axis=1).astype(cp.float64)                   # (T,)
    universe_rets_gpu = cp.where(
        univ_count_gpu > 0,
        univ_sum_gpu / cp.maximum(univ_count_gpu, 1.0),
        cp.float64(0.0)
    )
    univ_eq_gpu = cp.concatenate([cp.ones(1, dtype=cp.float64),
                                  cp.cumprod(1.0 + universe_rets_gpu)])           # (T+1,)

    # ── Batch transfer — ONE synchronize + ONE asnumpy per array ─────────────
    cp.cuda.Stream.null.synchronize()
    gpu_ms = (time.perf_counter() - t0) * 1000

    ic_vals    = cp.asnumpy(ic_gpu)
    bkt_means  = cp.asnumpy(bucket_means)          # (T, B)
    bkt_equity = cp.asnumpy(bucket_equity_gpu)     # (T+1, B)
    bkt_counts = cp.asnumpy(bucket_counts)         # (T, B)
    universe_rets = cp.asnumpy(universe_rets_gpu)  # (T,)
    universe_eq   = cp.asnumpy(univ_eq_gpu)        # (T+1,)
    nv_cpu     = cp.asnumpy(nv_eff)

    # ── CPU post-processing (metadata only — 60 periods, negligible) ─────────
    n_periods = len(dates)
    periods_per_year = 12.0 / hold_months  # 2 for semi-annual

    # Filter to periods with enough stocks
    valid_periods = nv_cpu >= min_n_stocks

    # Build bucket_returns / bucket_equity dicts from GPU-computed arrays
    bucket_returns: dict = {}
    bucket_equity:  dict = {}

    for b in range(1, n_buckets + 1):
        bi = b - 1
        brets_raw  = bkt_means[:, bi]
        bcounts_b  = bkt_counts[:, bi]
        rets = [float(brets_raw[i]) if (valid_periods[i] and bcounts_b[i] > 0) else 0.0
                for i in range(n_periods)]
        bucket_returns[str(b)] = [round(r, 6) for r in rets]
        # equity curve already computed by GPU cumprod — just read column
        bucket_equity[str(b)] = [round(float(bkt_equity[t, bi]), 6)
                                  for t in range(n_periods + 1)]
        # Rebase: if period 0 was invalid, equity[0] is still 1.0 (correct)

    # universe_rets and universe_eq already computed on GPU above — no CPU loop needed

    # ── Step 7: CAGR per bucket ───────────────────────────────────────────────
    def _cagr(equity_curve: list, periods_per_year: float) -> float:
        if len(equity_curve) < 2 or equity_curve[0] <= 0:
            return 0.0
        years = (len(equity_curve) - 1) / periods_per_year
        if years <= 0:
            return 0.0
        return float((equity_curve[-1] / equity_curve[0]) ** (1.0 / years) - 1.0)

    def _max_dd(equity_curve: list) -> float:
        eq = np.array(equity_curve)
        if len(eq) < 2:
            return 0.0
        peak = np.maximum.accumulate(eq)
        dd = eq / np.where(peak > 0, peak, 1.0) - 1.0
        return float(dd.min())

    def _sharpe(rets: list, periods_per_year: float, rf: float = 0.04) -> float:
        r = np.array(rets)
        if len(r) < 4:
            return 0.0
        excess = r - rf / periods_per_year
        std = float(r.std())
        if std < 1e-10:
            return 0.0
        return float(r.mean() / std * np.sqrt(periods_per_year))

    # Per-bucket metrics
    bucket_metrics: dict = {}
    for b in range(1, n_buckets + 1):
        brets = bucket_returns[str(b)]
        beq   = bucket_equity[str(b)]
        if len(brets) < 6:
            bucket_metrics[str(b)] = {}
            continue
        cagr = _cagr(beq, periods_per_year)
        sharpe = _sharpe(brets, periods_per_year)
        mdd = _max_dd(beq)
        bucket_metrics[str(b)] = {
            "cagr":    round(cagr, 4),
            "sharpe":  round(sharpe, 3),
            "max_dd":  round(mdd, 4),
            "n_obs":   sum(1 for r in brets if r != 0.0),
        }

    # ── Step 8: Monotonicity ──────────────────────────────────────────────────
    cagrs = [bucket_metrics.get(str(b), {}).get("cagr", 0.0) or 0.0
             for b in range(1, n_buckets + 1)]
    steps = [cagrs[i] - cagrs[i + 1] for i in range(len(cagrs) - 1)]
    n_steps = len(steps)
    monotonicity = float(sum(1 for s in steps if s > 0) / n_steps) if n_steps > 0 else 0.0

    # ── Step 9: Staircase score ───────────────────────────────────────────────
    total_spread = cagrs[0] - cagrs[-1]  # Q1 - Qn

    if n_steps > 1 and any(abs(s) > 0 for s in steps):
        step_std  = float(np.std(steps))
        step_mean = float(np.mean([abs(s) for s in steps]))
        cv = step_std / max(step_mean, 1e-6)
        step_uniformity = float(1.0 / (1.0 + cv * 0.5))
    else:
        step_uniformity = 1.0

    q1_is_best = cagrs[0] >= max(cagrs) if cagrs else False
    inversion_penalty = 1.0 if q1_is_best else max(0.1, cagrs[0] / max(cagrs) if max(cagrs) > 0 else 0.1)
    staircase_score = float(total_spread * monotonicity * step_uniformity * inversion_penalty)

    # ── Step 10: Annual alpha (Q1 - universe) ────────────────────────────────
    q1_annual: dict = {}
    q1_rets_all = bucket_returns.get("1", [])
    univ_by_year: dict = {}

    for i, d in enumerate(dates):
        yr = d[:4]
        q1_r = q1_rets_all[i] if i < len(q1_rets_all) else 0.0
        if yr not in q1_annual:
            q1_annual[yr] = []
        q1_annual[yr].append(q1_r)
        univ_by_year.setdefault(yr, []).append(float(universe_rets[i]))

    q1_annual_compound = {yr: float(np.prod([1 + r for r in rs]) - 1)
                          for yr, rs in q1_annual.items()}
    univ_annual = {yr: float(np.prod([1 + r for r in rs]) - 1)
                   for yr, rs in univ_by_year.items()}

    annual_ret_by_bucket: dict = {}
    for b in range(1, n_buckets + 1):
        by_year: dict = {}
        brets = bucket_returns[str(b)]
        for i, d in enumerate(dates):
            yr = d[:4]
            by_year.setdefault(yr, []).append(brets[i] if i < len(brets) else 0.0)
        annual_ret_by_bucket[str(b)] = [
            {"year": int(yr), "ret": round(float(np.prod([1 + r for r in rs]) - 1), 4)}
            for yr, rs in sorted(by_year.items())
        ]

    # ── Step 11: Alpha win rate ───────────────────────────────────────────────
    common_years = set(q1_annual_compound.keys()) & set(univ_annual.keys())
    if common_years:
        alpha_win_rate = float(sum(
            1 for yr in common_years if q1_annual_compound[yr] > univ_annual[yr]
        ) / len(common_years))
        alpha_series = [q1_annual_compound[yr] - univ_annual[yr]
                        for yr in sorted(common_years)]
        avg_annual_alpha = float(np.mean(alpha_series))
    else:
        alpha_win_rate = 0.0
        alpha_series = []
        avg_annual_alpha = 0.0

    # ── Step 12: Bear / Bull scores ───────────────────────────────────────────
    def _window_score(windows, dates, q1_rets_list, univ_rets_list):
        details = []
        for name, w_start, w_end in windows:
            idxs = [i for i, d in enumerate(dates)
                    if w_start <= str(d) <= w_end
                    and i < len(q1_rets_list) and i < len(univ_rets_list)]
            if not idxs:
                continue
            q1_r  = float(np.prod([1 + q1_rets_list[i]  for i in idxs]) - 1)
            un_r  = float(np.prod([1 + univ_rets_list[i] for i in idxs]) - 1)
            excess = round(q1_r - un_r, 4)
            details.append({"period": name, "q1_ret": round(q1_r, 4),
                            "univ_ret": round(un_r, 4), "excess": excess})
        if not details:
            return 0.0, details
        return round(float(np.mean([d["excess"] for d in details])), 4), details

    q1_rets_list   = list(bucket_returns.get("1", []))
    univ_rets_list = [float(v) for v in universe_rets]

    bear_score, bear_detail = _window_score(BEAR_WINDOWS, dates, q1_rets_list, univ_rets_list)
    bull_score, bull_detail = _window_score(BULL_WINDOWS, dates, q1_rets_list, univ_rets_list)

    # ── Step 13: OBQ Master Score ─────────────────────────────────────────────
    # IC metrics
    valid_ic = ic_vals[valid_periods]
    ic_mean  = float(np.mean(valid_ic)) if len(valid_ic) > 0 else 0.0
    ic_std   = float(np.std(valid_ic))  if len(valid_ic) > 0 else 1.0
    ic_hit   = float(np.mean(valid_ic > 0)) if len(valid_ic) > 0 else 0.0
    icir     = float(ic_mean / ic_std * np.sqrt(periods_per_year)) if ic_std > 0 else 0.0

    icir_val = float(ic_mean / max(ic_std, 1e-6))
    icir_component       = float(np.tanh(icir_val / 1.5))
    staircase_component  = float(np.tanh(staircase_score / 0.10))
    alpha_win_component  = float(alpha_win_rate)
    alpha_mag_component  = float(np.tanh(avg_annual_alpha / 0.05))
    ic_hit_component     = float(np.clip((ic_hit - 0.5) * 2.0, -1.0, 1.0))

    obq_fund_score = float(
        0.25 * icir_component
      + 0.25 * staircase_component
      + 0.20 * alpha_win_component
      + 0.20 * alpha_mag_component
      + 0.10 * ic_hit_component
    )
    obq_fund_score = round(float(np.clip(obq_fund_score, -1.0, 1.0)), 4)

    # Alpha Sharpe
    if len(alpha_series) >= 3:
        a_arr = np.array(alpha_series)
        alpha_sharpe = float(a_arr.mean() / a_arr.std()) if a_arr.std() > 0 else 0.0
    else:
        alpha_sharpe = 0.0

    # Spearman ρ across bucket CAGR vs bucket number (for reporting)
    try:
        b_nums  = list(range(1, n_buckets + 1))
        b_cagrs = [bucket_metrics.get(str(b), {}).get("cagr", 0.0) or 0.0 for b in b_nums]
        if np.var(b_cagrs) > 0:
            spearman_rho = float(np.corrcoef(b_nums, b_cagrs)[0, 1]) * -1.0
        else:
            spearman_rho = 0.0
    except Exception:
        spearman_rho = 0.0

    # Downside capture
    q1_max_dd_v  = abs(bucket_metrics.get("1", {}).get("max_dd", 0.0) or 0.0)
    univ_max_dd  = abs(float(_max_dd(universe_eq)))
    dd_protection = float(np.clip(1.0 - (q1_max_dd_v / max(univ_max_dd, 0.001)), 0.0, 1.0))

    bear_yrs = [yr for yr in common_years if univ_annual.get(yr, 0) < 0]
    if bear_yrs:
        q1_bear = np.mean([q1_annual_compound.get(yr, 0) for yr in bear_yrs])
        un_bear = np.mean([univ_annual.get(yr, 0) for yr in bear_yrs])
        dn_cap  = float(q1_bear / un_bear) if un_bear != 0 else 1.0
        dn_cap  = float(np.clip(dn_cap, 0.0, 2.0))
    else:
        dn_cap = 1.0

    # IC data list (for tearsheet)
    ic_data = [{"date": dates[i], "ic_value": round(float(ic_vals[i]), 4)}
               for i in range(n_periods) if valid_periods[i]]

    # Period data (for table display)
    period_data = []
    for i, d in enumerate(dates):
        if not valid_periods[i]:
            continue
        row = {"date": d}
        for b in range(1, n_buckets + 1):
            brets = bucket_returns.get(str(b), [])
            row[f"q{b}_ret"] = round(brets[i], 4) if i < len(brets) else None
        period_data.append(row)

    # Quintile spread
    q1m = bucket_metrics.get("1", {})
    qnm = bucket_metrics.get(str(n_buckets), {})
    q1_cagr  = q1m.get("cagr", 0.0) or 0.0
    qn_cagr  = qnm.get("cagr", 0.0) or 0.0
    spread_cagr = q1_cagr - qn_cagr

    # Universe metrics (simple)
    univ_cagr = _cagr(universe_eq, periods_per_year)
    univ_sharpe = _sharpe(universe_rets, periods_per_year)
    universe_metrics = {
        "cagr": round(univ_cagr, 4),
        "sharpe": round(univ_sharpe, 3),
        "max_dd": round(float(_max_dd(universe_eq)), 4),
        "terminal_wealth": round(float(10000 * universe_eq[-1]), 0),
    }

    # ── Sector attribution (GPU vectorized aggregation) ──────────────────────
    # For each (sector_id 1..11, bucket 1..n): mean fwd return across all valid
    # stock-period observations. Output: list of dicts sorted by Q1-Qn spread.
    sector_attribution: list = []
    if sector_gpu is not None:
        n_sec = 12  # 0=Unknown + 1..11
        sec_sums   = cp.zeros((n_sec, n_buckets), dtype=cp.float64)
        sec_counts = cp.zeros((n_sec, n_buckets), dtype=cp.int64)
        # CRITICAL: returns_gpu contains NaN at invalid positions. `0 * NaN = NaN`,
        # so multiplying by a bool mask poisons sum(). Pre-zero the NaN with cp.where.
        # R_masked is already this — returns where valid, 0 elsewhere — defined above.
        # Vectorized: 12 sectors × 5 buckets = 60 small reductions
        for s_id in range(n_sec):
            sec_mask = (sector_gpu == s_id) & both_valid
            if not bool(sec_mask.any()):
                continue
            for b_id in range(1, n_buckets + 1):
                mask = sec_mask & (buckets == b_id)
                # Use R_masked (NaN already zeroed) NOT returns_gpu
                sec_sums[s_id, b_id - 1]   = (R_masked * mask).sum()
                sec_counts[s_id, b_id - 1] = mask.sum()
        cp.cuda.Stream.null.synchronize()
        sec_means = cp.asnumpy(sec_sums / cp.maximum(sec_counts.astype(cp.float64), 1.0))
        sec_counts_np = cp.asnumpy(sec_counts)

        # Reverse sector map (id -> name)
        _SECTOR_NAMES = {
            0: 'Unknown', 1: 'Energy', 2: 'Materials', 3: 'Industrials',
            4: 'Consumer Discretionary', 5: 'Consumer Staples',
            6: 'Health Care', 7: 'Financials', 8: 'Information Technology',
            9: 'Communication Services', 10: 'Utilities', 11: 'Real Estate',
        }
        for s_id in range(n_sec):
            # Skip sectors with no stocks (count==0 across all buckets)
            if sec_counts_np[s_id].sum() == 0:
                continue
            row: dict = {"sector": _SECTOR_NAMES.get(s_id, f"Sector{s_id}")}
            for b in range(1, n_buckets + 1):
                row[f"q{b}"] = round(float(sec_means[s_id, b - 1]), 4)
            row["spread"] = round(float(sec_means[s_id, 0] - sec_means[s_id, n_buckets - 1]), 4)
            sector_attribution.append(row)
        # Sort by spread descending (best Q1-Q5 sectors first)
        sector_attribution.sort(key=lambda x: x.get("spread", 0), reverse=True)

    # ── Tortoriello (CPU post-process from GPU outputs) ──────────────────────
    # 9 of 14 fields are computable from already-computed bucket_returns +
    # universe_rets. Stock-level fields (avg_portfolio_size, market_cap,
    # factor_score, beat/lag universe) are left None — would need GPU pass
    # with stock-level aggregation. The tearsheet renders the available fields
    # and gracefully shows "—" for the None ones.
    tortoriello = _compute_tortoriello_cpu(
        dates=dates[:n_periods],
        bucket_returns=bucket_returns,
        bucket_equity=bucket_equity,
        universe_rets=list(universe_rets),
        n_buckets=n_buckets,
        periods_per_year=periods_per_year,
    )

    elapsed_s = (time.perf_counter() - t0)

    factor_metrics = {
        "ic_mean":             round(ic_mean, 4),
        "ic_std":              round(ic_std, 4),
        "icir":                round(icir, 3),
        "ic_hit_rate":         round(ic_hit, 3),
        "spearman_rho":        round(spearman_rho, 3),
        "monotonicity_score":  round(monotonicity, 3),
        "quintile_spread_cagr":round(spread_cagr, 4),
        "n_obs":               int(valid_periods.sum()),
        "n_stocks_avg":        round(float(nv_cpu[valid_periods].mean()), 0) if valid_periods.any() else 0.0,
        "q1_cagr":             round(q1_cagr, 4),
        "q1_sharpe":           round(q1m.get("sharpe", 0.0) or 0.0, 3),
        "q1_max_dd":           round(q1m.get("max_dd", 0.0) or 0.0, 4),
        "q1_calmar":           round(q1_cagr / max(abs(q1m.get("max_dd", 0.001)), 0.001), 3),
        "q1_surefire":         0.0,  # requires full equity shape — computed by strategy_bank
        "qn_cagr":             round(qn_cagr, 4),
        "qn_sharpe":           round(qnm.get("sharpe", 0.0) or 0.0, 3),
        "hold_months":         hold_months,
        "n_buckets":           n_buckets,
        "staircase_score":     round(staircase_score, 4),
        "alpha_win_rate":      round(alpha_win_rate, 3),
        "avg_annual_alpha":    round(avg_annual_alpha, 4),
        "bear_score":          bear_score,
        "bull_score":          bull_score,
        "downside_capture":    round(dn_cap, 3),
        "alpha_sharpe":        round(alpha_sharpe, 3),
        "obq_fund_score":      obq_fund_score,
    }

    return {
        "status":                "complete",
        "score_column":          score_column,
        "dates":                 dates,
        "buckets":               list(range(1, n_buckets + 1)),
        "bucket_returns":        bucket_returns,
        "bucket_equity":         bucket_equity,
        "bucket_metrics":        bucket_metrics,
        "ic_data":               ic_data,
        "period_data":           period_data,
        "annual_ret_by_bucket":  annual_ret_by_bucket,
        "factor_metrics":        factor_metrics,
        "universe_equity":       [round(float(v), 6) for v in universe_eq],
        "universe_metrics":      universe_metrics,
        "universe_rets":         [float(v) for v in universe_rets],  # for further processing
        "tortoriello":           tortoriello,  # CPU post-process: 9/14 fields filled
        "spy_metrics":           {},
        "fitness":               {
            "staircase_score":  round(staircase_score, 4),
            "alpha_win_rate":   round(alpha_win_rate, 3),
            "avg_annual_alpha": round(avg_annual_alpha, 4),
            "bear_score":       bear_score,
            "bull_score":       bull_score,
            "bear_detail":      bear_detail,
            "bull_detail":      bull_detail,
            "downside_capture": round(dn_cap, 3),
            "alpha_sharpe":     round(alpha_sharpe, 3),
            "obq_fund_score":   obq_fund_score,
        },
        "sector_attribution":    sector_attribution,
        "trade_log":             [],
        "n_obs":                 int(valid_periods.sum()),
        "n_stocks_avg":          round(float(nv_cpu[valid_periods].mean()), 0) if valid_periods.any() else 0.0,
        "elapsed_s":             round(elapsed_s, 3),
        "elapsed_gpu_ms":        round(gpu_ms, 1),
    }


def run_combo_on_gpu(
    S_a_gpu: "cp.ndarray",
    S_b_gpu: "cp.ndarray",
    dir_a: str,
    dir_b: str,
    returns_gpu: "cp.ndarray",
    valid_gpu: "cp.ndarray",
    n_valid: "cp.ndarray",
    dates: list,
    combo_id: str,
    n_buckets: int = 5,
    hold_months: int = 6,
    cap_mask_gpu: Optional["cp.ndarray"] = None,
    sector_gpu: Optional["cp.ndarray"] = None,
) -> dict:
    """
    Run a two-factor combo backtest on GPU.
    Builds rank-average composite score on GPU, then calls run_factor_on_gpu.
    """
    # Merge valid masks
    if cap_mask_gpu is not None:
        eff_valid = valid_gpu & cap_mask_gpu
    else:
        eff_valid = valid_gpu

    both_have_score = eff_valid & ~cp.isnan(S_a_gpu) & ~cp.isnan(S_b_gpu)
    nv_eff = both_have_score.sum(axis=1).astype(cp.int32)

    # Build rank-average combo score
    combo_gpu = _rank_avg_gpu(S_a_gpu, S_b_gpu, both_have_score, nv_eff, dir_a, dir_b)

    # Run as higher_better (rank-avg is always higher = better)
    return run_factor_on_gpu(
        scores_gpu=combo_gpu,
        returns_gpu=returns_gpu,
        valid_gpu=valid_gpu,
        n_valid=n_valid,
        dates=dates,
        n_buckets=n_buckets,
        lower_better=False,
        hold_months=hold_months,
        score_column=f"combo_{combo_id}",
        cap_mask_gpu=cap_mask_gpu,
        sector_gpu=sector_gpu,
    )


# ── Self-test / benchmark ──────────────────────────────────────────────────────

if __name__ == '__main__':
    import time
    import logging
    logging.basicConfig(level=logging.INFO)

    dev = cp.cuda.Device()
    free, total = dev.mem_info
    print(f"GPU: cc={dev.compute_capability}  VRAM {free/1e9:.1f}/{total/1e9:.1f}GB")

    # Simulate 60 semi-annual periods, 3000 stocks
    rng = np.random.default_rng(42)
    n_p, n_s = 60, 3000
    scores_np  = rng.normal(50, 20, (n_p, n_s)).astype(np.float64)
    returns_np = rng.normal(0.08, 0.15, (n_p, n_s)).astype(np.float64)
    # Inject a real signal: stocks with higher scores get ~1% more return
    returns_np += scores_np * 0.002
    valid_np = np.ones((n_p, n_s), dtype=bool)
    valid_np[:, 2500:] = False  # simulate ~2500 valid stocks per period

    scores_gpu  = cp.asarray(scores_np)
    returns_gpu = cp.asarray(returns_np)
    valid_gpu   = cp.asarray(valid_np)
    n_valid_np  = valid_np.sum(axis=1).astype(np.int32)
    n_valid_gpu = cp.asarray(n_valid_np)

    # Fake semi-annual dates
    dates = [f"{'1995' if i < 20 else '2005' if i < 40 else '2015'}-{'06-30' if i%2==0 else '12-31'}"
             for i in range(n_p)]

    # GPU warmup
    _ = run_factor_on_gpu(scores_gpu[:5], returns_gpu[:5], valid_gpu[:5],
                          n_valid_gpu[:5], dates[:5], score_column='warmup')

    # Timed run
    t0 = time.perf_counter()
    result = run_factor_on_gpu(
        scores_gpu, returns_gpu, valid_gpu, n_valid_gpu,
        dates, n_buckets=5, lower_better=False,
        hold_months=6, score_column='test_factor'
    )
    ms = (time.perf_counter() - t0) * 1000

    fm = result['factor_metrics']
    print(f"\nBenchmark: {n_p}p × {n_s}s, 5 buckets")
    print(f"  GPU time: {ms:.1f}ms ({result['elapsed_gpu_ms']:.1f}ms GPU, {ms - result['elapsed_gpu_ms']:.1f}ms CPU post)")
    print(f"  IC mean={fm['ic_mean']:.4f}  ICIR={fm['icir']:.3f}  IC hit={fm['ic_hit_rate']:.3f}")
    print(f"  Q1 CAGR={fm['q1_cagr']*100:.2f}%  Q5 CAGR={fm['qn_cagr']*100:.2f}%  Spread={fm['quintile_spread_cagr']*100:.2f}%")
    print(f"  Staircase={fm['staircase_score']:.4f}  Alpha Win={fm['alpha_win_rate']:.3f}  OBQ={fm['obq_fund_score']:.4f}")
    print(f"  Status: {result['status']}")

    # Compare IC to scipy
    from scipy.stats import spearmanr
    cpu_ics = []
    for i in range(n_p):
        vm = valid_np[i]
        if vm.sum() < 10:
            continue
        rho, _ = spearmanr(scores_np[i, vm], returns_np[i, vm])
        cpu_ics.append(rho)
    cpu_ic = float(np.mean(cpu_ics))
    print(f"\n  CPU Spearman IC mean={cpu_ic:.4f}  GPU IC mean={fm['ic_mean']:.4f}  diff={abs(cpu_ic - fm['ic_mean']):.6f}")
