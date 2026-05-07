# -*- coding: utf-8 -*-
"""
engine/gpu_factor_engine.py
GPU-accelerated factor backtest engine using CuPy (CUDA).
Follows ARCHITECTURE.md GPU-First Constitution.

Architecture:
  - CuPy vectorized argsort for ranking (fully parallel on GPU)
  - CuPy batch matrix ops for Spearman IC (Pearson on ranks)
  - CuPy for quintile bucketing and return aggregation
  - NO Python loops over periods for numerical work
  - NO scipy, NO pandas groupby for compute paths

RTX 3090: 10,496 CUDA cores, 24GB VRAM, compute capability 8.6
"""
from __future__ import annotations

import os
import time
import numpy as np

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import cupy as cp


def gpu_status():
    dev = cp.cuda.Device()
    free, total = dev.mem_info
    return f"GPU Device {dev.id} cc={dev.compute_capability} VRAM {free/1e9:.1f}/{total/1e9:.1f}GB"


def _batch_rank_2d(X_gpu):
    """
    Rank each row of X_gpu independently. GPU-parallel via CuPy argsort.
    Uses argsort(argsort(x)) + 1 = rank pattern, fully vectorized on GPU.
    Input:  cp.ndarray shape (n_periods, max_stocks)
    Output: cp.ndarray shape (n_periods, max_stocks) float64 ranks (1-based)
    """
    n, m = X_gpu.shape
    order = cp.argsort(X_gpu, axis=1)
    ranks = cp.empty_like(X_gpu, dtype=cp.float64)
    rows = cp.arange(n)[:, None]
    ranks[rows, order] = cp.arange(1, m + 1, dtype=cp.float64)[None, :]
    return ranks


def gpu_factor_backtest(
    scores_by_period,
    returns_by_period,
    n_buckets=5,
    lower_better=False,
    cost_bps=15.0,
):
    """
    Run a complete factor quintile backtest on GPU.
    ALL numerical compute on GPU via CuPy -- no scipy, no pandas loops.

    Args:
        scores_by_period:  list of arrays, each (n_stocks_i,) factor scores per period
        returns_by_period: list of arrays, each (n_stocks_i,) forward returns per period
        n_buckets:         number of quintile buckets (default 5)
        lower_better:      True if lower score = better (e.g. P/Sales)
        cost_bps:          transaction cost basis points per side

    Returns:
        dict with ic_values, bucket_returns, bucket_counts, summary stats, timing
    """
    t0 = time.perf_counter()

    n_periods = len(scores_by_period)
    max_stocks = max(len(s) for s in scores_by_period)

    # CPU: Pad arrays to uniform shape (NaN-padded for invalid)
    scores_np  = np.full((n_periods, max_stocks), np.nan, dtype=np.float64)
    returns_np = np.full((n_periods, max_stocks), np.nan, dtype=np.float64)
    n_valid    = np.zeros(n_periods, dtype=np.int32)

    for i, (s, r) in enumerate(zip(scores_by_period, returns_by_period)):
        n = min(len(s), len(r))
        scores_np[i, :n] = s[:n]
        returns_np[i, :n] = r[:n]
        n_valid[i] = n

    # Transfer to GPU
    S = cp.asarray(scores_np)
    R = cp.asarray(returns_np)
    V = cp.asarray(n_valid)

    # Build valid mask (True where not NaN)
    valid_mask = ~(cp.isnan(S) | cp.isnan(R))

    # For invalid entries, set to extreme value so they sort to the end
    S_for_rank = cp.where(valid_mask, S, cp.float64(1e18))
    R_for_rank = cp.where(valid_mask, R, cp.float64(1e18))

    # GPU: Rank each row -- fully parallel via CuPy argsort
    SR = _batch_rank_2d(S_for_rank)  # score ranks (1=lowest)
    RR = _batch_rank_2d(R_for_rank)  # return ranks (1=lowest)

    # Direction correction:
    # If higher_better: invert score ranks so rank 1 = highest score = Q1
    if not lower_better:
        V_2d = V[:, None].astype(cp.float64)
        SR = cp.where(valid_mask, (V_2d + 1.0) - SR, SR)

    # Mask out invalid entries
    SR = cp.where(valid_mask, SR, cp.float64(0.0))
    RR = cp.where(valid_mask, RR, cp.float64(0.0))

    # GPU: Spearman IC = Pearson correlation of ranks per row
    # Fully vectorized -- no Python loop
    V_f = V.astype(cp.float64)[:, None]

    SR_sum = (SR * valid_mask).sum(axis=1, keepdims=True)
    RR_sum = (RR * valid_mask).sum(axis=1, keepdims=True)
    N_f    = V.astype(cp.float64)[:, None]

    SR_mean = SR_sum / cp.maximum(N_f, 1.0)
    RR_mean = RR_sum / cp.maximum(N_f, 1.0)

    SR_c = cp.where(valid_mask, SR - SR_mean, cp.float64(0.0))
    RR_c = cp.where(valid_mask, RR - RR_mean, cp.float64(0.0))

    cov_N = (SR_c * RR_c).sum(axis=1)
    var_s = (SR_c * SR_c).sum(axis=1)
    var_r = (RR_c * RR_c).sum(axis=1)
    denom = cp.sqrt(var_s * var_r)
    denom = cp.maximum(denom, cp.float64(1e-10))

    ic_gpu = cov_N / denom

    # Direction correction for IC sign:
    # When higher_better, we inverted score ranks above, which flips the correlation
    # Negate IC to restore expected sign (positive IC = factor works)
    if not lower_better:
        ic_gpu = -ic_gpu

    # GPU: Assign quintile buckets based on score rank
    bucket_float = cp.ceil(SR / cp.maximum(V_f, 1.0) * n_buckets)
    buckets = cp.clip(bucket_float.astype(cp.int32), 1, n_buckets)
    buckets = cp.where(valid_mask, buckets, cp.int32(0))

    # GPU: Mean return per bucket per period
    bucket_means  = cp.zeros((n_periods, n_buckets), dtype=cp.float64)
    bucket_counts = cp.zeros((n_periods, n_buckets), dtype=cp.int32)

    for b in range(1, n_buckets + 1):
        b_mask = (buckets == b)
        b_count = b_mask.sum(axis=1)
        b_sum   = (R_for_rank * b_mask).sum(axis=1)
        # Use original R (not R_for_rank which has 1e18 for NaN)
        b_sum_real = cp.where(valid_mask, R, cp.float64(0.0))
        b_sum_real = (b_sum_real * b_mask).sum(axis=1)
        bucket_counts[:, b-1] = b_count
        bucket_means[:, b-1]  = b_sum_real / cp.maximum(b_count.astype(cp.float64), 1.0)

    # Synchronize and transfer back
    cp.cuda.Stream.null.synchronize()
    gpu_ms = (time.perf_counter() - t0) * 1000

    ic_values      = cp.asnumpy(ic_gpu)
    bkt_returns    = cp.asnumpy(bucket_means)
    bkt_counts     = cp.asnumpy(bucket_counts)

    # CPU: Summary statistics
    valid_periods = n_valid >= 10
    valid_ic = ic_values[valid_periods]

    ic_mean = float(np.mean(valid_ic)) if len(valid_ic) > 0 else 0.0
    ic_std  = float(np.std(valid_ic))  if len(valid_ic) > 0 else 1.0
    ic_hit  = float(np.mean(valid_ic > 0)) if len(valid_ic) > 0 else 0.0
    icir    = float(ic_mean / ic_std) if ic_std > 0 else 0.0

    return {
        "ic_values":       ic_values,
        "bucket_returns":  bkt_returns,
        "bucket_counts":   bkt_counts,
        "n_valid":         n_valid,
        "ic_mean":         round(ic_mean, 4),
        "ic_std":          round(ic_std, 4),
        "icir":            round(icir, 3),
        "ic_hit":          round(ic_hit, 3),
        "n_periods":       n_periods,
        "max_stocks":      max_stocks,
        "elapsed_gpu_ms":  round(gpu_ms, 1),
    }


# --- Individual functions called by factor_backtest.py ---

def batch_spearman_ic(df, score_col="score_raw", return_col="fwd_return",
                      period_col="month_date", lower_better=False, min_stocks=10):
    """
    Compute Spearman IC for every period in df using GPU.
    Returns: (ic_data_list, stats_dict)
    """
    import pandas as _pd
    periods = sorted(df[period_col].unique())
    scores_list = []
    returns_list = []
    period_labels = []

    for d in periods:
        grp = df[df[period_col] == d].dropna(subset=[score_col, return_col])
        if len(grp) < min_stocks:
            continue
        scores_list.append(grp[score_col].values.astype(np.float64))
        returns_list.append(grp[return_col].values.astype(np.float64))
        period_labels.append(d)

    if not period_labels:
        return [], {"ic_mean": 0, "ic_std": 1, "ic_hit": 0, "used_gpu": False}

    result = gpu_factor_backtest(scores_list, returns_list, n_buckets=5,
                                 lower_better=lower_better)
    ic_vals = result["ic_values"]
    ic_data = [{"date": d, "ic_value": round(float(ic_vals[i]), 4)}
               for i, d in enumerate(period_labels)
               if i < len(ic_vals)]

    stats = {"ic_mean": result["ic_mean"], "ic_std": result["ic_std"],
             "ic_hit": result["ic_hit"], "used_gpu": True}
    return ic_data, stats


def gpu_assign_quintiles(df, score_col="score_raw", period_col="month_date",
                         n_buckets=5, higher_better=True):
    """
    Assign quintile buckets for all periods using GPU.
    Returns pd.Series aligned with df.index.
    """
    import pandas as _pd
    result = _pd.Series(np.nan, index=df.index)
    periods = sorted(df[period_col].unique())

    for d in periods:
        mask = df[period_col] == d
        grp = df[mask].dropna(subset=[score_col])
        if len(grp) < n_buckets:
            continue

        scores = grp[score_col].values.astype(np.float64)
        n = len(scores)

        # GPU rank
        s_gpu = cp.asarray(scores)
        if higher_better:
            order = cp.argsort(-s_gpu)  # descending: rank 1 = highest
        else:
            order = cp.argsort(s_gpu)   # ascending: rank 1 = lowest
        ranks = cp.empty_like(s_gpu, dtype=cp.float64)
        ranks[order] = cp.arange(1, n + 1, dtype=cp.float64)

        # NTILE bucket
        buckets = cp.ceil(ranks / n * n_buckets).astype(cp.int32)
        buckets = cp.clip(buckets, 1, n_buckets)
        buckets_np = cp.asnumpy(buckets)

        result.loc[grp.index] = buckets_np.astype(float)

    return result


# Benchmark / self-test
if __name__ == "__main__":
    print(gpu_status())

    n_periods = 374
    n_stocks  = 2500
    rng = np.random.default_rng(42)

    print(f"\nBenchmark: {n_periods} periods, {n_stocks} max stocks")

    scores_list  = []
    returns_list = []
    for i in range(n_periods):
        n = rng.integers(500, n_stocks)
        s = rng.normal(50, 20, n)
        r = rng.normal(0.01, 0.05, n) + s * 0.001
        scores_list.append(s)
        returns_list.append(r)

    # GPU warmup
    _ = gpu_factor_backtest(scores_list[:5], returns_list[:5])

    # GPU timed
    t0 = time.perf_counter()
    result = gpu_factor_backtest(scores_list, returns_list, n_buckets=5, lower_better=False)
    gpu_time = (time.perf_counter() - t0) * 1000
    print(f"  GPU total: {gpu_time:.0f}ms (kernel: {result['elapsed_gpu_ms']:.0f}ms)")
    print(f"  IC mean={result['ic_mean']:.4f}  ICIR={result['icir']:.3f}  IC hit={result['ic_hit']:.3f}")

    # CPU comparison
    from scipy.stats import spearmanr
    t0 = time.perf_counter()
    cpu_ics = [spearmanr(s, r)[0] for s, r in zip(scores_list, returns_list)]
    cpu_time = (time.perf_counter() - t0) * 1000
    cpu_ic_mean = float(np.mean(cpu_ics))
    print(f"  CPU total: {cpu_time:.0f}ms  IC mean={cpu_ic_mean:.4f}")

    speedup = cpu_time / max(gpu_time, 0.1)
    print(f"\n  Speedup: {speedup:.1f}x {'(GPU FASTER)' if speedup > 1 else '(CPU faster)'}")
    print(f"  IC parity: diff={abs(result['ic_mean']-cpu_ic_mean):.6f}")

    br = result['bucket_returns']
    vm = result['n_valid'] >= 10
    q1 = float(br[vm, 0].mean())
    q5 = float(br[vm, 4].mean())
    print(f"\n  Q1={q1*100:.3f}% Q5={q5*100:.3f}% Spread={((q1-q5)*100):.3f}%")
