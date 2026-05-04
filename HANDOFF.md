# OBQ FactorLab — Session Handoff
**Date:** 2026-05-03  
**Repo:** github.com/alexbernal0/OBQ_FactorLab  
**Latest commit:** 4276c27 (v3.0)  
**App location:** `C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab\`  
**Launch:** `C:\Users\admin\Desktop\.venv-obq\Scripts\python.exe main.py`  
**Port:** 5744

---

## App State — 6-Tab GUI

| Tab | Status | Notes |
|---|---|---|
| **FACTOR MODELS** | ✅ Working | 38 models, R3000 EW universe benchmark |
| **PORTFOLIO MODELS** | ✅ Working | 32 models, SPX benchmark, full tearsheet |
| **RESULTS** | ✅ Working | SPY + QQQ/MDY/IWM benchmark buttons |
| **TRACKER** | Stub | Not yet built |
| **FINDINGS** | ✅ Working | 10+ findings incl. CYC-001 closure |
| **WIKI** | ✅ Working | 30 entries, 7 parts, searchable |

---

## Database Files (D:\OBQ_AI\)

| File | Purpose | Size |
|---|---|---|
| `obq_eodhd_mirror.duckdb` | Main data (v_backtest_scores, v_backtest_prices, PROD_*) | Large |
| `OBQ_FactorLab_Bank/factor_strategy_bank.duckdb` | Factor model bank (38 models) | Medium |
| `OBQ_FactorLab_Bank/portfolio_strategy_bank.duckdb` | Portfolio model bank (32 models) | Medium |
| `OBQ_FactorLab_Bank/trade_log.duckdb` | Dedicated trade log DB | 165K factor + 48K portfolio entries |
| `OBQ_FactorLab_Bank/findings.json` | Research findings journal | 10+ entries |

---

## CYC-001 Results Summary

**Universe:** Large-Cap ($10B+), ~Top-1000  
**Period:** 1990-07-31 → 2024-12-31 (34 years, 69 semi-annual periods)  
**Benchmark:** S&P 500 Total Return (SPX price + Shiller div pre-1993, SPY post-1993)  
**Universe benchmark:** Russell 3000 EW from Norgate PROD_Sector_Index_Membership (PIT, 12.28% CAGR)

### Top Factor Signals (ICIR)
| Rank | Score | ICIR | Staircase | Notes |
|---|---|---|---|---|
| 1 | JCN Alpha Trifecta | 1.489 | +2.6% | Value+Quality+Momentum |
| 2 | JCN QARP | 1.486 | +8.0% | Best fundable |
| 3 | Fundsmith Rank | 1.436 | +17.0% | Best staircase |
| 4 | JCN Composite | 1.432 | +1.5% | All-factor blend |
| 5 | Quality (Universe) | 1.363 | +5.2% | Best spread +17.4% |

### Top Portfolio Models (Sharpe)
| Rank | Score | CAGR | Sharpe | MaxDD |
|---|---|---|---|---|
| 1 | JCN QARP | 16.1% | **0.830** | -23.4% |
| 2 | JCN GARP | 17.2% | 0.819 | -29.4% |
| 3 | Value (Universe) | 14.7% | 0.819 | -21.2% |
| 4 | Pure Value | 13.3% | 0.751 | **-11.3%** |
| 5 | JCN Composite | 16.2% | 0.718 | -26.7% |
*SPX baseline: CAGR 10.57%, Sharpe 0.446*

---

## Key Technical Decisions Made

### Metrics (GIPS-Compliant)
- **Sharpe:** `(CAGR - rf) / ann_vol` — CAGR in numerator (was arithmetic mean, now fixed)
- **Sortino:** `(CAGR - rf) / std(neg_returns) × √ppy` — std not RMS (was wrong, now fixed)  
- **Calmar GIPS:** `CAGR / |MaxDD|`
- **Calmar OBQ:** `CAGR × WinMo% / |MaxDD|` — proprietary, labeled separately

### Benchmarks
- **SPX:** `engine/spx_backtest.py` — auto-routes from `run_spy_backtest()` when start < 1993
  - Pre-1993: SPX price return + Shiller historical dividend yields (3.67%, 2.95%, 2.81%)
  - Post-1993: SPY adjusted_close total return
- **Russell 3000 EW:** `PROD_Sector_Index_Membership.in_russell_3000` from Norgate
  - Used as universe benchmark in factor tearsheets (12.28% CAGR — harder to beat than SPX)
  - Loaded via `_load_russell3000_universe_returns()` in `factor_backtest.py`

### Staircase Score Formula
```
Staircase = (Q1-Q5 spread) × Monotonicity × (1/(1+CV×0.5)) × Q1_best_penalty
```
- Spread: Q1_CAGR - Q5_CAGR
- Monotonicity: fraction of steps where Q_i > Q_{i+1}
- Uniformity: soft CV penalty (never zeros out)
- Q1_best_penalty: = 1.0 if Q1 is top bucket, else Q1/max(Qi) — hard penalizes inversions

### OBQ Fund Score Formula
```
Fund = 0.30×AlphaWin + 0.25×tanh(AvgAlpha/5%) + 0.20×DDProtect + 0.15×DnCapture + 0.10×tanh(AlphaSharpe)
```
All components vs **SPX total return** (not EW universe)

### Trade Log Architecture
- `engine/trade_log_db.py` — dedicated `trade_log.duckdb`
- `factor_trades` table: symbol, sector, score, return%, market_cap per Q1 stock per period
- `portfolio_trades` table: same + weight, entry/exit price
- API: `/api/trades/factor/{sid}`, `/api/trades/portfolio/{sid}`, `/api/trades/count`
- Bug was fixed: `Symbol` vs `symbol` column case issue in factor engine

---

## Next Session: CYC-002 Priorities

### PLANNED (in order)
1. **CYC-002: Multi-factor blend optimization**
   - Hypothesis: QARP + Quality + Value combination outperforms any single factor
   - Target: Sharpe > 0.9, Alpha Win Rate > 60%
   - Test all pairwise + trifecta combinations of top-5 factors

2. **Portfolio Models tearsheet sidebar**
   - Add tearsheet rendering when clicking strategy in Strategy Log (like Factor Models)
   - Already built the pattern in portfolio_lab.js — just needs wiring to bank click

3. **Factor tearsheet sidebar on Strategy Bank window**
   - Click strategy in bank → full tearsheet renders in right panel
   - Same pattern as portfolio_lab.js `_showPmTearsheet()`

4. **CYC-003: Mid-cap momentum test**
   - Hypothesis: Momentum revives in mid-cap ($2B-$10B)
   - All 4 momentum variants dead in large-cap (ICIR < 0.01-0.74)

5. **Remaining Batch 3 / additional scores**
   - Some Batch 3 models had DB lock errors — may need rerun
   - LongEQ rank results TBD

### DEFERRED (post-CYC-002)
- CYC-004: Sector-neutral backtests
- CYC-005: Stop-loss optimization (15%, 20%, 25% on QARP)
- Tracker tab build-out
- Results tab: 4 benchmark buttons fully wired (HTML done, app.js wired, needs testing)

---

## Engine Architecture

```
engine/
  factor_backtest.py     — Quintile factor backtest (R3000 universe benchmark)
  portfolio_backtest.py  — Top-N portfolio backtest (SPX benchmark)
  strategy_bank.py       — Factor model bank (DuckDB, FM-xxx IDs)
  portfolio_bank.py      — Portfolio model bank (DuckDB, PM-xxx IDs)
  trade_log_db.py        — Dedicated trade log DB (factor_trades, portfolio_trades)
  spy_backtest.py        — SPX/SPY benchmark (auto-routes pre-1993 to SPX splice)
  spx_backtest.py        — Pre-1993 SPX splice with Shiller dividend yields
  metrics.py             — GIPS-compliant metrics (Sharpe, Sortino, Calmar etc.)
  data.py                — Data loading (legacy, used by Results tab)
  backtest.py            — Legacy backtest (Results tab)
```

## GUI Architecture

```
gui/
  app.py                 — Flask routes (all /api/* endpoints)
  static/
    index.html           — 6-tab layout
    css/app.css          — Theme vars (light/dark/night)
    js/
      tabs.js            — Tab switching, dividers, DOMContentLoaded
      factor_lab.js      — Factor Models tab (runs, bank, tearsheet)
      factor_tearsheet.js — Factor Plotly charts + Tortoriello tables
      portfolio_lab.js   — Portfolio Models tab (runs, bank, tearsheet)
      cycles_lab.js      — Research Cycles panel (localStorage)
      wiki_lab.js        — WIKI tab (accordion, search)
      app.js             — Results tab (legacy strategy backtests)
      tearsheet.js       — Results tearsheet charts
    data/
      wiki_data.json     — 30 wiki entries (encyclopaedia)
```

---

## Known Issues / Watch List

1. **Wiki tab overlay** — Fixed in wiki_lab.js (inner wrapper pattern). If it breaks again, ensure `view-wiki` outer div display is controlled only by tabs.js `active` class — wiki_lab.js should NEVER set `outer.style.display`.

2. **DB lock conflicts** — `obq_eodhd_mirror.duckdb` is used by OBQ_ADE pipeline (PID ~47248+48804). If backtests fail with "cannot open file", wait for ADE pipeline to finish or kill those PIDs.

3. **Factor trade logs** — 28/31 CYC-001 factor models have trade logs in `trade_log.duckdb`. The 3 without (growth_score, rulebreaker_rank, longeq_rank) will populate on next natural rerun. The engine bug (Symbol vs symbol column) is fixed.

4. **Russell 3000 coverage** — Early periods (1990-1993) have 186-200 R3000 members (sparse). The fallback (scored-stock EW) fills gaps. By 2000 the table has full ~3000 member coverage.

5. **Benchmark buttons** — QQQ/MDY/IWM HTML buttons added to Results tab + app.js `startBenchmark()` wired. Needs testing on fresh session to confirm they load correctly.

---

## Launch Commands

```powershell
# Launch FactorLab (don't kill Options Scanner on port 5001)
Start-Process "C:\Users\admin\Desktop\.venv-obq\Scripts\python.exe" -ArgumentList "main.py" -WorkingDirectory "C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab" -WindowStyle Normal

# Check trade DB
cd C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab
python check_fund_scores.py  # verify all fitness scores populated
python check_batch_results.py  # show all models ranked

# Run a new backtest manually
python seed_pm001.py  # re-seeds PM-Cycle-001

# Batch new scores
python batch_cycle001_baseline.py  # run all 14 CYC-001 factor+portfolio

# Dedup banks
python dedup_banks.py

# Check trade log DB
python -c "from engine.trade_log_db import count_trades; print(count_trades())"
```

---

## Data Layer — MotherDuck Mirror
- `v_backtest_scores` — 858K rows, 1990-2026, 22 score columns
- `v_backtest_prices` — 75K symbols, daily OHLCV + market_cap, 1962-present  
- `PROD_Sector_Index_Membership` — 2.5M rows, Norgate PIT membership, 1962-2026
  - `in_sp500` and `in_russell_3000` boolean flags per symbol per month
- `PROD_OBQ_Investable_Universe` — OBQ reconstitution table

**Data integrity confirmed:**
- ✅ `adjusted_close` throughout (split/dividend adjusted)
- ✅ 39% delisted symbols — no survivorship bias
- ✅ PIT scores — no look-ahead
- ✅ Per-stock return cap [-95%, +300%] applied
