# FactorLab Session Handoff v5.0 — 2026-05-12

## IMMEDIATE NEXT STEP

Start CYC-007 — Sector-Optimized Composite Scores. See below for full spec.
All research cycles CYC-003 through CYC-006b are complete and committed to GitHub.

## SYSTEM STATE

### App
- FactorLab PyWebView app: `python main.py` -> port 5744
- Bank: D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb
- Total bank records: ~2,848 (571 + 1,198 CYC-006 + 1,077 CYC-006b + extras)

### Key Database Paths
- Mirror: D:/OBQ_AI/obq_eodhd_mirror.duckdb (Norgate prices, R3000, EODHD fundamentals)
- Fundamentals: D:/OBQ_AI/obq_fundamentals.duckdb (TTM filings, score tables)
- Factor Bank: D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb
- Encyclopedia: D:/OBQ_AI/OBQ_Encyclopedia_v2/ (v2.1, 140+ chapters)

## RESEARCH CYCLES COMPLETED

### CYC-003 (273 records) - R3000 Baseline
- 91 factors x 3 cap tiers (all/large/$10B+/mid/$2B-$10B)
- Entry: python run_cyc003_gpu.py
- Top: JCN Alpha Trifecta OBQ 0.839, JCN QARP OBQ 0.848 (large)

### CYC-004 (111 records) - Pure Factor Baselines
- 37 new pure factors x 3 tiers
- Key: OCF/Assets OBQ 0.777, F-Score OBQ 0.759, EBIT/Assets OBQ 0.739
- Entry: python run_cyc004_gpu.py --full (--compute-scores builds score table)

### CYC-005 (187 records) - Sector Intelligence
- 15 champion factors x 11 sectors + 11 novel sector factors x 2 runs
- Key: IT + JCN Alpha Trifecta OBQ 0.883 (highest in study)
- Health Care OCF/Assets OBQ 0.876, +35% Q1-Q5 spread (widest in study)
- Health Care R&D Yield: OBQ 0.689 within-sector (true sector specialist)
- Entry: python run_cyc005_gpu.py --full

### CYC-006 (1,198 records) - Rebalance Timing Study
- 139 factors x 10 timing variants (quarterly/semi-annual rotations/annual)
- KEY FINDING: Annual Jun 30 (A-Q2) wins for most factors (26/120)
- Composites/Growth prefer Dec 31; Value/Quality prefer Jun 30; Momentum prefers SA-APR-OCT
- Entry: python run_cyc006_gpu.py (data already loaded; use --analyze-only for results)

### CYC-006b (1,077 records) - Staggered Tranche Rebalancing
- 9 tranche configs x 139 factors (CPU post-processing, no GPU needed)
- KEY FINDING: 2T-MAR-SEP (50/50, Mar31+Sep30) wins most with +0.221 avg OBQ gain
- 4T-MAR-JUN-SEP-DEC improves most individual factors (90/119)
- jcn_qarp reaches OBQ 0.922 with 2T-JUN-DEC (vs 0.828 SA6 baseline)
- Entry: python run_cyc006b_tranche.py (--analyze for results, takes 50 seconds)

## NEXT PLANNED CYCLES

### CYC-007 (NOT STARTED) - Sector-Optimized Composites
Build per-sector multi-factor composite scores combining the best within-sector factors:
1. Health Care Quality: OCF/Assets + EBIT/Assets + F-Score (all strong within HC)
2. IT Alpha: FCF Margin + Rule of 40 + JCN Alpha Trifecta (ICIR 2.151 for FCF Margin in IT)
3. Financials ROE: ROE + Retained Earnings/TA (flip Capital Adequacy - it inverted)
4. Energy Cycle: Mid-Cycle FCF Yield + Momentum
5. REIT FFO: FFO Proxy Yield + OCF/Assets + Dividend Growth
Use: run as CYC-005b format (sector mask GPU + within-sector quintile backtest)

## CRITICAL BUGS FIXED (know these for future work)

1. Symbol format: filings_ttm uses AAPL, v_backtest_prices uses AAPL.US
   Fix is in: engine/cyc004_score_compute.py and engine/gpu_data_loader.py
   Every new score pipeline MUST strip/add .US when joining price data

2. Bulk insert speed: save_factor_model() now accepts _shared_con parameter
   Pattern: open _get_bank() once, pass as _shared_con=con to all saves
   34 rec/sec vs 0.28 rec/sec without shared connection

3. App factor log was limiting to 500 records, raised to 2000

4. Portfolio tab was showing 93 legacy PM-* records from May 1-5 session
   Fixed in portfolio_lab.js: hides records with created_at < 2026-05-07

## ENCYCLOPEDIA v2.1 STATUS

Location: D:/OBQ_AI/OBQ_Encyclopedia_v2/
- 140+ factor chapters (Parts II-XIV) + Part XV (15 timing chapters)
- Part XV has 10 CYC-006 chapters + 5 CYC-006b chapters
- Full PDF: ~/Downloads/OBQ_Encyclopedia_v2_20260512_*.pdf (1.7 MB)
- Timing PDF: ~/Downloads/OBQ_CYC006_Rebalance_Timing_Study_*.pdf (0.1 MB)

Regenerate commands:
  cd encyclopedia_v2/generator
  python generate_all.py          # factor chapters
  python generate_cyc006.py       # CYC-006 timing Ch 1-10
  python generate_cyc006b.py      # CYC-006b tranche Ch 11-15
  python build_pdf.py             # full encyclopedia PDF
  python build_cyc006_pdf.py      # standalone timing PDF

## GITHUB
- Repo: github.com:alexbernal0/OBQ_FactorLab
- Branch: main
- All work committed and pushed
- Latest commit: see git log --oneline -1
