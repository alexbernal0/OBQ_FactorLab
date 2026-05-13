# FactorLab Session Handoff v6.0 — 2026-05-13

## SYSTEM STATE

### App
- FactorLab PyWebView app: `launch.bat` (project-local venv) or `.venv\Scripts\python main.py`
- Port: 5744 | venv: `.venv/` (isolated, ~400MB, no conflict with other OBQ apps)
- Bank: D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb (5GB, local only)
- GitHub backup: results/factor_bank_scalars.parquet (2,854 records, 0.2MB, scalars only)

### Key Database Paths
- Mirror: D:/OBQ_AI/obq_eodhd_mirror.duckdb (Norgate prices, R3000, EODHD fundamentals)
- Fundamentals: D:/OBQ_AI/obq_fundamentals.duckdb (TTM filings, score tables)
- Factor Bank: D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb
- Portfolio Bank: D:/OBQ_AI/OBQ_FactorLab_Bank/portfolio_strategy_bank.duckdb

## RESEARCH CYCLES COMPLETED

### CYC-003 (272 records) — R3000 Baseline
- 72 singles + 19 combos × 3 cap tiers (all/large/mid)
- Top: JCN Alpha Trifecta OBQ 0.839, JCN QARP OBQ 0.848 (large-cap)

### CYC-004 (111 records) — Pure Factor Baselines
- 37 pure factors × 3 tiers
- Key: OCF/Assets OBQ 0.777, F-Score OBQ 0.759, EBIT/Assets OBQ 0.739

### CYC-005 (187 records) — Sector Intelligence
- 15 champion factors × 11 sectors + 11 novel sector-specialist factors
- IT + JCN Alpha Trifecta OBQ 0.883 (highest in study)
- Health Care OCF/Assets OBQ 0.876, +35% Q1-Q5 spread (widest)

### CYC-006 (864 records) — Rebalance Timing Study
- 120 factors × 10 timing variants (quarterly/semi-annual/annual)
- KEY: Annual Jun 30 (A-Q2) wins for most factors (26/120)
- NOTE: 864 saved (some factors only available for certain timings)

### CYC-006b (1077 records) — Staggered Tranche Rebalancing
- 9 tranche configs × 120 factors (CPU blending of CYC-006 equity curves)
- KEY: 2T-MAR-SEP wins most (+0.221 avg OBQ gain)
- jcn_qarp reaches OBQ 0.922 with 2T-JUN-DEC

### CYC-007 (9 records) — Sector-Optimized Composites
- 9 per-sector multi-factor composites (HC, IT, FIN, CD, CS, IND, MAT)

**TOTAL BANK: 2,854 factor records**

## SESSION WORK (2026-05-13)

### App Improvements
1. **Project-local venv** — `.venv/` inside project, isolated from shared `.venv-obq`
   - Run with: `launch.bat` or `.venv\Scripts\python main.py`
   - `setup_venv.bat` for first-time setup on new machine

2. **Factor Models tab — Strategy Log preloaded**
   - Bank data embedded in HTML at serve time (`window.__FACTOR_BANK__`)
   - Zero-latency load on tab switch — no async fetch needed
   - 6 real research cycles pre-seeded in Research Cycles panel

3. **Tearsheet fully populated** — all fields for all cycles:
   - `avg_portfolio_size`, `avg_beat_universe`, `avg_lag_universe`
   - `median_factor_score`, `avg_market_cap`
   - Added to GPU engine via stock-level per-bucket aggregation
   - All 5 cycles re-run to populate new fields

4. **Universe column fully populated**
   - `ann_vol`, `calmar`, `best_month`, `worst_month` computed from universe_equity_json
   - Enriched on-the-fly in JS for existing records

5. **Period Returns Heatmap** redesigned
   - Years on Y-axis, periods on X-axis (was: dates on X, buckets on Y)
   - One chart per quintile (Q1-Q5), stacked vertically

6. **Portfolio Models tab**
   - New staggered tranche engine: `engine/tranche_portfolio_backtest.py`
   - 28-stock, 4-tranche, quarterly stagger, 1-year hold, equal-weight
   - Trial run: JCN Full Composite saved as `PM-JCNFUL-20260513-59B3`

### Code Changes
- `engine/gpu_factor_compute.py` — stock-level aggregations + `market_cap_gpu` param
- `engine/gpu_batch_runner.py` — passes `market_cap_gpu` to all GPU calls
- `engine/strategy_bank.py` — `created_at::VARCHAR AS created_at` alias fix
- `engine/tranche_portfolio_backtest.py` — NEW: staggered tranche portfolio engine
- `run_cyc005_gpu.py`, `run_cyc006_gpu.py`, `run_cyc007_gpu.py` — `market_cap_gpu` added
- `run_cyc006b_tranche.py` — full tearsheet data (bucket_metrics, tortoriello, dates, ic_data, period_data)
- `run_pm_tranche_trial.py` — NEW: trial portfolio backtest runner
- `gui/app.py` — preloaded bank data in HTML, NaN sanitizer for factor bank endpoint
- `gui/templates/index.html` — NEW: Flask template with `__FACTOR_BANK__` injection
- `gui/static/index.html` — Strategy Log columns fixed, heatmap updated
- `gui/static/js/factor_lab.js` — JS enrichment for missing fields, sync bank load
- `gui/static/js/factor_tearsheet.js` — Period heatmap redesigned (year×period grid)
- `gui/static/js/cycles_lab.js` — 6 real cycles seeded, sort state for Strategy Log
- `gui/static/js/portfolio_lab.js` — preloaded from `__PM_BANK__`
- `gui/static/js/tabs.js` — explicit `flLoadBank()` + `flLoadCycles()` call
- `launch.bat`, `setup_venv.bat`, `requirements.txt` — NEW: project venv support

## GITHUB BACKUP NOTE
The `results/` folder contains:
- `factor_bank_scalars.parquet` — all 2,854 records, scalar columns only (0.2MB)
- `portfolio_bank_scalars.parquet` — 94 portfolio models, scalar columns only

Full tearsheet JSON blobs (5GB total) are local only at:
`D:/OBQ_AI/OBQ_FactorLab_Bank/`
Back these up to external drive or cloud separately.

## NEXT PLANNED WORK
- Portfolio Model batch run: all top-performing factors through 28-stock 4-tranche model
- Tearsheet format matching paperswithbacktest style (user to locate reference folder)
- CYC-008: potential new research cycle
