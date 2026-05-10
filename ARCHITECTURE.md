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

### GPU Batch Pipeline (MANDATORY for CYC-003+)

**The key optimization: PRE-LOAD ALL DATA INTO VRAM ONCE, then iterate factors.**

```
Phase 1 — DATA PRE-LOAD (once, ~6 seconds total):
  CPU: ONE DuckDB query loads ALL scores + ALL prices for ALL periods
  CPU: Flatten to contiguous arrays (scores_matrix, returns_matrix, valid_mask)
  GPU: cp.asarray() transfers to VRAM — stays resident for all 326 factor runs
  
  Data layout in VRAM (24GB available):
    returns[n_periods x max_stocks]    float64  ~5.6MB for 374x2500
    valid_mask[n_periods x max_stocks] uint8    ~0.9MB
    per_factor_scores[n_periods x max_stocks] float64 ~5.6MB (swapped per factor)
    dates[n_periods]                   — CPU only (metadata)
    
  Total VRAM per factor run: ~12MB. With 24GB available, can cache ALL factor 
  score columns simultaneously (91 factors x 5.6MB = ~510MB = 2% of VRAM).

Phase 2 — PER-FACTOR GPU COMPUTE (~200ms per factor, ALL on GPU):
  For each of 326 factor runs:
    1. Select score column (pointer swap in VRAM, ~0 cost)
    2. rank_scores:       CuPy argsort per period -> quintile buckets
    3. spearman_ic:       Pearson correlation of score_ranks x return_ranks
    4. bucket_returns:    mean return per quintile per period  
    5. equity_curves:     cumulative product per bucket (cp.cumprod)
    6. universe_returns:  mean return of all valid stocks per period
    7. spread:            Q1_equity[-1] vs Q5_equity[-1] -> CAGR difference
    8. monotonicity:      count(Q[i] > Q[i+1]) / n_steps per period average
    9. staircase:         spread x monotonicity x step_uniformity
    10. annual_alpha:     Q1 annual ret - universe annual ret per calendar year
    11. alpha_win_rate:   count(annual_alpha > 0) / n_years
    12. bear_bull:        alpha during specific date window subsets
    13. obq_master_score: 25% ICIR + 25% staircase + 20% alpha_win + 20% alpha_mag + 10% IC_hit
    
    ALL above are CuPy vectorized ops or RawKernel — ZERO scipy, ZERO pandas loops.

Phase 3 — CPU SAVE (~5ms per factor):
  - cp.asnumpy() transfer results back
  - Format result dict matching existing bank schema
  - save_factor_model() to DuckDB bank
  - save_portfolio_model() for Top-20 portfolio

Phase 4 — ESTIMATED TOTAL TIME:
  Pre-load:    6 seconds
  326 factors: 326 x 206ms = 67 seconds
  Save:        326 x 5ms = 1.6 seconds
  TOTAL:       ~75 seconds for ALL 326 CYC-003 runs
```

### Data Pre-load Query (ONE query, ALL data)

```sql
-- Load all scores + forward returns for the full R3000 universe
-- Joined at semi-annual dates with 6-month forward returns
-- Result: ~500K-1M rows covering all periods x all stocks
SELECT 
    s.symbol, s.month_date, s.gic_sector,
    -- ALL score columns loaded simultaneously
    s.jcn_full_composite, s.jcn_qarp, s.value_score, s.quality_score, ...
    -- CYC-002 separate table scores joined in
    q.quality_score_composite, v.value_score_composite, ...
    -- Forward 6-month return
    (p_fwd.adj_close / p_cur.adj_close - 1) AS fwd_return,
    -- Market cap for tier filtering
    p_cur.market_cap
FROM v_backtest_scores s
JOIN prices p_cur ON ...
JOIN prices p_fwd ON ... (6 months forward)
LEFT JOIN fund_db.scores.obq_quality_scores q ON ...
LEFT JOIN fund_db.scores.obq_value_scores v ON ...
WHERE s.month_date IN (semi_annual_dates)
```

### GPU Data Layout

All arrays **contiguous, row-major, float64** in VRAM:

```
scores_all[n_score_columns x n_periods x max_stocks]  -- ALL factor scores pre-loaded
returns[n_periods x max_stocks]                        -- forward returns (shared across factors)
valid_mask[n_periods x max_stocks]                     -- uint8 (1=valid, 0=padding)
n_valid[n_periods]                                     -- int32 stock count per period
market_cap[n_periods x max_stocks]                     -- for cap-tier filtering on GPU
sector_ids[n_periods x max_stocks]                     -- int8 GICS sector encoding (for sector analysis)
```

### Cap-Tier Filtering ON GPU

Instead of running separate DuckDB queries per cap tier, load ALL data once and 
filter on GPU:

```cuda
// In kernel: apply cap filter by masking
if (market_cap[pid * max_stocks + sid] < min_cap) return;  // skip this stock
if (market_cap[pid * max_stocks + sid] > max_cap) return;
```

This means 4 cap tiers x 91 factors = 364 runs share the SAME VRAM data.
Only the cap filter mask changes between tiers.

### CUDA Kernels Required

1. **`rank_and_ic`** (existing, verified) — rank scores, compute Spearman IC per period
2. **`bucket_mean_returns`** (existing, verified) — mean return per bucket per period  
3. **`equity_curve`** (NEW) — cumulative product of bucket returns -> equity per bucket
4. **`universe_benchmark`** (NEW) — EW mean return of all valid stocks per period
5. **`annual_aggregation`** (NEW) — aggregate period returns into calendar year returns
6. **`fitness_metrics`** (NEW) — staircase, alpha, monotonicity, OBQ Master Score
7. **`sector_attribution`** (NEW) — mean return per sector per bucket per period

### Implementation File Structure

```
engine/
  gpu_factor_engine.py      -- CUDA kernels + CuPy vectorized ops (EXISTS, needs expansion)
  gpu_data_loader.py        -- DuckDB -> flat arrays -> VRAM pre-loader (NEW)
  gpu_batch_runner.py        -- iterate 326 factors on pre-loaded VRAM data (NEW)  
  factor_backtest.py         -- LEGACY CPU engine (keep for reference/fallback)
  portfolio_backtest.py      -- portfolio simulation (keep CPU for now)
```

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

## Encyclopedia v2 — Universe Doctrine (CYC-003 onward)

**Default universe is R3000.** All factor analysis assumes R3000-survivorship-bias-free
PIT membership unless explicitly noted. Large-cap-only views are a SLICE of R3000,
not a separate universe.

Every factor entry in encyclopedia v2 must report:
1. **R3000 base case** — performance across the full R3000 universe (the default lens)
2. **Cap-bin slicing** — `all` / `large` ($10B+) / `mid` ($2B-$10B), sourced from
   the same `factor_models` rows differing only by `cap_tier`
3. **Sector slicing** — Q1 vs universe spread per GICS sector, per cap bin
4. **Bear / bull window decomposition** — already computed in `factor_metrics.bear_score`
   and `bull_score` — surface in encyclopedia, do not recompute

**No more "large-cap only" pages as the default.** A factor that only works in
mega-cap is a tagged characteristic, not a hidden assumption.

---

*Last updated: 2026-05-06 | OBQ Factor Research*
