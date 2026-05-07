# GPU Factor Engine — COMPLETE BUILD SPEC

> This is the exact blueprint for building the GPU factor backtest engine.
> A fresh session should read ONLY this file and build exactly what it describes.
> Do NOT patch factor_backtest.py. Build a NEW engine from scratch.

## Reference Implementation

The Options Scanner GPU engine proves the pattern works on this exact hardware:
- File: `C:\Users\admin\Desktop\OBQ_AI\OBQ_AI_OptionsApp\backtest\gpu_backtest_engine.py`
- Pattern: CPU preps flat arrays -> ONE CUDA kernel launch -> CPU reads results
- Result: 1M trade simulations in sub-second on RTX 3090
- Uses: CuPy RawKernel with CUDA C++ source (NOT Numba, NOT cuDF)

## Hardware

- GPU: RTX 3090, 10,496 CUDA cores, 24GB VRAM, compute capability 8.6
- CUDA: 12.4 at `C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4`
- Set `os.environ['CUDA_PATH']` before importing CuPy
- CuPy installed and verified working (IC parity test passed: diff=0.000016)

## What To Build

### File 1: `engine/gpu_data_loader.py`

ONE DuckDB query loads ALL data for ALL 326 factor runs into flat numpy arrays,
then transfers to GPU VRAM where it stays for the entire batch.

```python
def load_all_data(mirror_db, fund_db, start_date, end_date, rebal_freq):
    """
    Returns GPUDataPack with all data in VRAM:
      - returns_gpu:    cp.ndarray (n_periods, max_stocks) float64 — 6mo fwd returns
      - market_cap_gpu: cp.ndarray (n_periods, max_stocks) float64 — for cap filtering
      - sector_gpu:     cp.ndarray (n_periods, max_stocks) int8    — GICS sector ID
      - valid_gpu:      cp.ndarray (n_periods, max_stocks) bool    — valid mask
      - n_valid:        cp.ndarray (n_periods,) int32              — stock count per period
      - symbols:        list[list[str]] per period                 — for trade log (CPU only)
      - dates:          list[str]                                  — period dates
      - score_columns:  dict[str, cp.ndarray]                      — ALL score columns pre-loaded
                        Each is (n_periods, max_stocks) float64 in VRAM
    """
```

The query joins:
- `v_backtest_scores` (JCN composites, OBQ core scores) 
- `PROD_OBQ_Quality_Scores`, `PROD_OBQ_Value_Scores`, etc. (CYC-002 separate tables)
- `v_backtest_prices` (for forward returns + market cap)
- Semi-annual rebalance dates (June 30 + Dec 31)

Score columns to pre-load (91 total):
- 26 from v_backtest_scores (CYC-001 composites)
- 46 from separate score tables (CYC-002 singles) 
- 19 combo scores computed on GPU from pairs of singles

All data padded to uniform (n_periods, max_stocks) shape. NaN = invalid.
Estimated VRAM: 91 scores x 374 periods x 3000 stocks x 8 bytes = ~820MB = 3.4% of 24GB.

### File 2: `engine/gpu_factor_compute.py`

ALL 13 compute steps as CuPy vectorized operations. ZERO pandas, ZERO scipy, ZERO Python loops.

```python
def run_factor_on_gpu(scores_gpu, returns_gpu, valid_gpu, n_valid,
                      n_buckets=5, lower_better=False, dates=None):
    """
    Complete factor backtest on GPU. ALL compute on GPU.
    
    Input: scores_gpu (n_periods, max_stocks) already in VRAM
    Output: dict with all metrics matching existing bank schema
    
    Steps (all CuPy):
      1. rank_scores:        cp.argsort per row -> ranks
      2. assign_buckets:     ceil(rank / n_valid * n_buckets) -> bucket IDs
      3. spearman_ic:        Pearson(score_ranks, return_ranks) per period
      4. bucket_mean_ret:    mean(returns[bucket==b]) per period per bucket
      5. equity_curves:      cp.cumprod(1 + bucket_rets) per bucket
      6. universe_ret:       mean(returns[valid]) per period
      7. spread_cagr:        (Q1_equity[-1]/Q1_equity[0])^(1/years) - same for Q5
      8. monotonicity:       count(Q[i]_cagr > Q[i+1]_cagr) / (n_buckets-1)
      9. staircase:          spread * monotonicity * step_uniformity
      10. annual_alpha:      Q1_annual_ret - universe_annual_ret per year
      11. alpha_win_rate:    sum(annual_alpha > 0) / n_years
      12. bear_bull_score:   alpha during specific date windows
      13. obq_master_score:  25% ICIR + 25% staircase + 20% alpha_win + 20% alpha_mag + 10% IC_hit
    """
```

Cap-tier filtering is a GPU mask operation:
```python
cap_mask = (market_cap_gpu >= min_cap) & (market_cap_gpu <= max_cap)
effective_valid = valid_gpu & cap_mask
# Now run all 13 steps with effective_valid instead of valid_gpu
```

This means ALL 4 cap tiers share the same VRAM data. Just swap the mask.

### File 3: `engine/gpu_batch_runner.py`

Iterates all 326 factor runs on pre-loaded VRAM data.

```python
def run_cyc003_gpu(data_pack, factor_list, cap_tiers, combo_list):
    """
    Main entry point for CYC-003.
    
    1. data_pack = load_all_data(...)          # 6 seconds, ONE DuckDB query
    2. For each factor x cap_tier:
         scores_gpu = data_pack.score_columns[factor_name]   # pointer swap, ~0 cost
         cap_mask = build_cap_mask(data_pack, tier)           # GPU mask, ~0 cost
         result = run_factor_on_gpu(scores_gpu, ...)          # ~200ms ALL on GPU
         save_factor_model(result)                            # ~5ms CPU
    3. For each combo x cap_tier:
         combo_scores = rank_average_gpu(factor_a, factor_b)  # GPU rank-average
         result = run_factor_on_gpu(combo_scores, ...)        # ~200ms
         save_factor_model(result)
    
    Total: 6s + 326 x 205ms = 73 seconds
    """
```

### IMPORTANT: Rename and Replace

- Rename current `engine/factor_backtest.py` to `engine/factor_backtest_cpu_legacy.py`
- The NEW GPU engine files become the PRIMARY engine
- `engine/factor_backtest_gpu.py` is the new main entry point for ALL factor backtests
- ALL app code, batch runners, and API endpoints must import from `factor_backtest_gpu`
- The legacy CPU file is kept ONLY for parity testing reference, never called in production

### File 4: `run_cyc003_gpu.py` (entry point script)

```python
if __name__ == '__main__':
    from engine.gpu_data_loader import load_all_data
    from engine.gpu_batch_runner import run_cyc003_gpu
    
    data = load_all_data(MIRROR_DB, FUND_DB, '1995-03-31', '2024-12-31', 'semi-annual')
    results = run_cyc003_gpu(data, ALL_FACTORS, CAP_TIERS, ALL_COMBOS)
```

## What NOT To Do

- Do NOT patch factor_backtest.py — it's CPU legacy, keep for reference only
- Do NOT use ProcessPoolExecutor — single process, GPU handles parallelism
- Do NOT use pandas groupby in compute paths
- Do NOT use scipy in compute paths  
- Do NOT query DuckDB per factor — ONE query loads everything
- Do NOT use Numba (can't target cc 8.6)
- Do NOT launch one CUDA kernel per period — batch ALL periods in one launch

## Existing Code To Reuse

- `engine/gpu_factor_engine.py` — has verified `_batch_rank_2d()` and `gpu_factor_backtest()`
  IC parity diff=0.000016 vs scipy. Expand this, don't rewrite.
- `engine/cyc002_factors.py` — CYC002_FACTORS and CYC002_COMBOS registries
- `engine/strategy_bank.py` — save_factor_model() for DuckDB bank writes
- `engine/portfolio_bank.py` — save_portfolio_model()
- `engine/metrics.py` — compute_all() for portfolio metrics (can stay CPU for portfolio sim)

## Bank Schema Compatibility

The GPU engine output dict MUST match the existing bank schema exactly so the 
FactorLab app can display results. Key fields:

Factor bank: strategy_id, run_label, score_column, icir, ic_mean, ic_std, ic_hit_rate,
  quintile_spread_cagr, q1_cagr, q1_sharpe, q1_max_dd, qn_cagr, n_obs, n_stocks_avg,
  staircase_score, alpha_win_rate, avg_annual_alpha, obq_fund_score, bear_score, bull_score,
  bucket_metrics_json, bucket_equity_json, ic_data_json, tortoriello_json, ...

## Validation

After building, run parity test:
1. Pick 5 factors (jcn_full_composite, cyc2_ps, cyc2_roic, quality_score_universe, combo_T4)
2. Run each through BOTH old CPU engine AND new GPU engine
3. Compare: ICIR within 0.01, spread within 0.1%, Q1 CAGR within 0.1%
4. If parity fails, fix GPU engine until it matches

## Timeline

Fresh session with full context:
- gpu_data_loader.py:    1-2 hours
- gpu_factor_compute.py: 2-3 hours  
- gpu_batch_runner.py:   1 hour
- Parity testing:        1 hour
- Run 326 CYC-003 jobs:  ~75 seconds
- QC + verification:     30 min
- TOTAL:                 5-7 hours

---

*Created: 2026-05-06 | For next session*
