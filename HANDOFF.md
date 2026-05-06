# FactorLab Session Handoff v4.0 — 2026-05-06

## IMMEDIATE NEXT STEP

Wire `engine/gpu_factor_engine.py` into `engine/factor_backtest.py` to replace scipy/pandas hot paths. Then build GPU batch runner and execute all 326 CYC-003 jobs.

### GPU Integration Points in factor_backtest.py:
1. Replace scipy.spearmanr loop (line ~778) with GPU batch IC
2. Replace pandas groupby.apply(assign_bucket) (line ~591) with GPU bucket assignment
3. Keep fitness metrics, Tortoriello metrics, save logic on CPU

### Key Context:
- GPU engine tested and verified: `engine/gpu_factor_engine.py` (IC diff=0.000016 vs CPU)
- ARCHITECTURE.md commits to CuPy-only compute (no scipy in compute paths)
- Score pipelines extended to 1995 with all 3 dimension scores stored
- OBQ Master Factor Score formula: 25% ICIR + 25% staircase + 20% alpha_win + 20% alpha_mag + 10% IC_hit
- R3000/SP500 universe tables built with full Norgate PIT membership

### CYC-003 Scope:
- 91 factors x 4 cap tiers (all/mega/large/mid) = 326 total runs
- Universe: R3000 survivorship-bias free, PIT accurate
- Period: 1995-2024 (scores) / 1990-2024 (prices)
- After completion: QC all models, rebuild encyclopedia v2 with cap-tier + sector analysis

### Database Locations:
- Mirror: `D:/OBQ_AI/obq_eodhd_mirror.duckdb` (R3000/SP500 universe tables)
- Fundamentals: `D:/OBQ_AI/obq_fundamentals.duckdb` (scores from 1995)
- Banks: `D:/OBQ_AI/OBQ_FactorLab_Bank/` (factor + portfolio)
- Encyclopedia: `D:/OBQ_AI/OBQ_Encyclopedia/` (63 files, v1)
- Backup: MotherDuck `OBQ_FUNDAMENTALS_BACKUP` (current as of 2026-05-06)

### Git HEAD: 91212bd
