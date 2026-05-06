# OBQ FactorLab — Architecture & Compute Constitution

> This document is the binding specification for all compute work in FactorLab.
> **No PR merges without conforming to these rules.**

---

## GPU-First Compute Constitution (NON-NEGOTIABLE)

### Rule 1: ALL numerical compute MUST run on GPU via CuPy RawKernel (CUDA C++)

Every backtest computation — IC calculation, quintile bucketing, forward return aggregation,
rank correlation, percentile scoring — **MUST** be implemented as CUDA C++ kernels launched
via `cupy.RawKernel`. No exceptions.

**Forbidden in compute paths:**
- `scipy.stats.spearmanr` in loops
- `pandas.DataFrame.groupby().apply()` for numerical work
- `numpy` operations that could be batched on GPU
- Any Python loop over periods/stocks for numerical computation
- `numba.cuda` (cannot target RTX 3090 compute capability 8.6)

**Required pattern:**
```python
import cupy as cp

_KERNEL_SRC = r'''
extern "C" __global__
void my_kernel(const double* input, double* output, int n) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    // ... CUDA C++ compute ...
}
'''
_KERNEL = cp.RawKernel(_KERNEL_SRC, 'my_kernel')
```

### Rule 2: CPU handles ONLY data preparation and I/O

- DuckDB queries (read data) → CPU
- Flatten DataFrames to contiguous arrays → CPU
- Transfer arrays to GPU → `cp.asarray()`
- Launch kernel → GPU
- Read results back → `cp.asnumpy()` or `.get()`
- Save to bank → CPU

### Rule 3: Target hardware

| Component | Spec |
|---|---|
| GPU | NVIDIA RTX 3090 (GA102, compute capability 8.6) |
| CUDA Cores | 10,496 |
| VRAM | 24 GB GDDR6X |
| CUDA Toolkit | 12.x (installed at `C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4`) |
| Python GPU lib | CuPy (NOT Numba — Numba's NVVM can't target cc 8.6) |
| Kernel style | `cupy.RawKernel` with inline CUDA C++ source |

### Rule 4: GPU utilization target

- Factor backtest batch runs: **>80% GPU utilization**
- Single factor run: **>50% GPU utilization**
- If GPU util < 50% during compute, the implementation is WRONG — redesign the kernel

### Rule 5: Parallelism model

Each CUDA thread = one `(period, stock)` pair for IC computation, OR one `(period, bucket)` pair
for return aggregation. With 374 periods × 3000 stocks = 1.1M threads — well within RTX 3090
capacity (10,496 cores × occupancy).

**Do NOT launch one kernel per period.** Batch ALL periods into a single kernel launch using
2D grid indexing: `grid = (ceil(n_periods/block), ceil(n_stocks/block))`.

---

## Data Architecture

### Local databases (read-only for compute)

| Database | Path | Purpose |
|---|---|---|
| Mirror DB | `D:/OBQ_AI/obq_eodhd_mirror.duckdb` | Prices, Norgate membership, EODHD fundamentals |
| Fundamentals DB | `D:/OBQ_AI/obq_fundamentals.duckdb` | TTM filings, OBQ score pipeline outputs |
| Prices DB | `D:/OBQ_AI/obq_ai.duckdb` | Norgate equity time series |

### Write targets (sequential, CPU-only)

| Database | Path | Purpose |
|---|---|---|
| Factor Bank | `D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb` | Factor model results |
| Portfolio Bank | `D:/OBQ_AI/OBQ_FactorLab_Bank/portfolio_strategy_bank.duckdb` | Portfolio model results |

### Universe tables

| Table | Content |
|---|---|
| `PROD_R3000_Universe` | R3000 PIT membership + prices + all scores (1990-2025) |
| `PROD_SP500_Universe` | SP500 subset of R3000 |
| `v_backtest_scores` | JCN composites + OBQ core scores |

---

## Backtest Engine Architecture

### GPU Factor Backtest Pipeline

```
CPU: DuckDB query → flatten to arrays → transfer to GPU
GPU: CUDA kernel (rank, correlate, bucket, aggregate) → single launch
CPU: read results → compute fitness metrics → save to bank
```

### CUDA Kernels Required

1. **`rank_and_correlate`** — For each period: rank scores, rank returns, compute Pearson correlation of ranks (= Spearman IC). Output: IC per period.

2. **`assign_quintiles`** — For each period: rank scores, assign NTILE(5) bucket. Output: bucket assignment per (period, stock).

3. **`bucket_returns`** — For each (period, bucket): compute mean forward return. Output: (n_periods × n_buckets) return matrix.

4. **`equity_curves`** — Cumulative product of bucket returns. Output: equity curve per bucket.

### Data layout for GPU

All arrays are **contiguous, row-major, float64**:
- `scores[n_periods][max_stocks_per_period]` — factor scores, NaN-padded
- `returns[n_periods][max_stocks_per_period]` — forward returns, NaN-padded
- `valid[n_periods][max_stocks_per_period]` — uint8 mask (1=valid, 0=padding)
- `n_stocks[n_periods]` — actual stock count per period (for rank normalization)

---

## OBQ Master Factor Score Formula

```
25% × tanh(ICIR / 1.5)                    — ranking consistency
25% × tanh(staircase_score / 0.10)        — quintile monotonicity
20% × alpha_win_rate                       — % years Q1 beats universe
20% × tanh(avg_annual_alpha / 0.05)       — Q1 outperformance magnitude
10% × (ic_hit_rate - 0.5) × 2             — IC reliability
```

Score interpretation:
- `0.6 → 1.0` : Exceptional — primary signal
- `0.4 → 0.6` : Good — combo ingredient
- `0.2 → 0.4` : Moderate — weak signal
- `0.0 → 0.2` : Weak — marginal
- `< 0.0`     : Negative — inverted/broken

---

## Score Pipeline (Component Scores)

### Composite weighting (current, subject to optimization)

| Dimension | Weight | What it measures |
|---|---|---|
| Universe | 40% | Percentile rank vs all stocks |
| Sector | 40% | Percentile rank within GICS sector |
| History | 20% | Percentile rank vs stock's own history |

All three dimensions stored independently per stock per month for post-hoc weight optimization.

### Score start date: 1995-03-31

- `filings_ttm` extended back to 1993-03-31 (warmup buffer)
- History scores valid from ~1997 (8-quarter warmup)
- Pipeline code: `quality_score_populate.py`, `value_score_populate.py`, etc.

---

## Research Cycles

| Cycle | Universe | Cap Filter | Status |
|---|---|---|---|
| CYC-001 | Large-cap | $10B+ | Complete (26 composites) |
| CYC-002 | Large-cap | $10B+ | Complete (46 singles + 19 combos) |
| CYC-003 | R3000 | All / Mega / Large / Mid tiers | IN PROGRESS |
| CYC-004 | R3000 mid-cap | $1B-$10B | Planned |
| CYC-005 | Tiny Titans | $25M-$200M | Planned |

---

*Last updated: 2026-05-06 | OBQ Factor Research*
