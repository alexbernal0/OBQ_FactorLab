# OBQ Factor Lab — Session Handoff
**Date:** 2026-04-30  
**Repo:** github.com/alexbernal0/OBQ_FactorLab  
**Latest commit:** 2742bdf  
**App location:** `C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab\`  
**Launch:** Double-click `OBQ FactorLab.lnk` on Desktop

---

## What Was Built This Session

### App Architecture (5-Tab GUI)

| Tab | Purpose | Status |
|---|---|---|
| **FACTOR MODELS** | Run quintile factor backtests, assess results, view saved bank models | ✅ Working |
| **PORTFOLIO MODELS** | Top-N portfolio optimizer (factor → live strategy) | 🔨 Stub — next sprint |
| **RESULTS** | Strategy backtests (SPY B&H + factor-driven Top-N) | ✅ Working |
| **TRACKER** | Live production positions, daily rebalance report | 🔨 Stub — next sprint |
| **FINDINGS** | Research journal — log cycle intelligence | ✅ Working |

**Flow:**
```
FACTOR MODELS → [assess] → FINDINGS (log learnings)
              → [promote] → RESULTS
RESULTS       → [activate] → TRACKER
```

---

## Key Files

### Engine
| File | Purpose |
|---|---|
| `engine/factor_backtest.py` | Quintile backtest engine — 5-layer pipeline (Universe→Prefilter→Score→Rank→Metrics) |
| `engine/metrics.py` | 80+ metrics including OBQ Surefire Suite, Tortoriello metrics, DSR, MinTRL |
| `engine/spy_backtest.py` | SPY B&H benchmark (loads from PROD_EOD_ETFs) |
| `engine/strategy_bank.py` | DuckDB strategy bank — saves FM-xxx IDs |
| `engine/data.py` | Data loader for existing factor strategy backtest |
| `engine/backtest.py` | Existing strategy backtest engine (Top-N, quintile bins) |

### GUI
| File | Purpose |
|---|---|
| `gui/static/index.html` | 5-tab layout, all CSS vars for theming |
| `gui/static/js/tabs.js` | Tab switcher — owns theme, dividers, DOMContentLoaded |
| `gui/static/js/factor_lab.js` | Factor Models tab — run/poll/render, bank load |
| `gui/static/js/factor_tearsheet.js` | Quintile charts (Plotly) + Tortoriello tables |
| `gui/static/js/results_lab.js` | Results tab — saved bank models list |
| `gui/static/js/tracker_lab.js` | Tracker tab — positions, rebalance |
| `gui/static/js/findings_lab.js` | Findings journal — save/expand/filter/export |
| `gui/static/js/tearsheet.js` | Strategy tearsheet charts (Plotly, SPY etc) |
| `gui/static/js/app.js` | Strategy backtest runs table + SPY auto-load |
| `gui/static/css/app.css` | Theme vars (light/:root, dark, night) |
| `gui/app.py` | Flask backend — all API routes |
| `main.py` | PyWebView launcher, 1600×960 |

---

## Data Layer

### MotherDuck / Local DuckDB
**Mirror DB:** `D:/OBQ_AI/obq_eodhd_mirror.duckdb`

| Table | Content | Rows |
|---|---|---|
| `v_backtest_scores` | PIT OBQ scores (16 factors), monthly | 859K |
| `v_backtest_prices` | Daily OHLCV + mktcap | 73.7M |
| `v_backtest_universe` | OBQ investable universe membership | 81K |
| `PROD_JCN_Composite_Scores` | JCN + composites | 1.05M |
| `PROD_EOD_ETFs` | ETF prices (SPY etc) | — |

**Strategy Bank:** `D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb`  
**Findings:** `D:/OBQ_AI/OBQ_FactorLab_Bank/findings.json`

### Score Columns Available (16 total)
```
jcn_full_composite, jcn_qarp, jcn_garp, jcn_quality_momentum,
jcn_value_momentum, jcn_growth_quality_momentum, jcn_fortress,
value_score, quality_score, growth_score, finstr_score,
momentum_score, momentum_af_score, momentum_fip_score,
moat_score, moat_rank
```

---

## Saved Models in Bank (2 as of session end)

| Strategy ID | Description | ICIR | Q1-Q5 Spread |
|---|---|---|---|
| `FM-JCNFUL-20260430-C9BF` | JCN 5Q 6mo All-Cap 2005-2024 PROD-v1 | 2.347 | 6.30% |
| `FM-JCNFUL-20260430-96BB` | JCN 5Q 6mo All-Cap 2010-2024 | 2.347 | 12.55% |

**Best ICIR:** 2.347 | **Best Spread:** 12.55% CAGR

---

## Research Findings Logged (3)

1. **[FACTOR]** JCN Composite shows monotonic quintile separation 2005-2024 — ICIR 2.35, 100% monotonicity, Strategy FM-JCNFUL-20260430-C9BF
2. **[FACTOR]** 6mo hold outperforms 3mo — less turnover drag, similar IC — Strategy FM-JCNFUL-20260430-96BB
3. **[BIAS]** ⚠️ GOTCHA: Volume/ADV filter is NULL in v_backtest_prices — falls back to market_cap proxy. Need to join PROD_EOD_survivorship for real volume

---

## What Works Right Now

✅ Launch app → white theme, 5 tabs  
✅ Factor Models → score dropdown loads → RUN FACTOR → quintile backtest runs in ~11s  
✅ Factor tearsheet: Tortoriello main table, IC chart, rolling charts, heatmap, sector tables  
✅ Auto-save to strategy bank with FM-xxx ID  
✅ Saved models appear in bottom of Factor Models left panel  
✅ Results tab → SPY B&H auto-loads with full tearsheet  
✅ Findings tab → log research insights, tag, link to strategy ID, expandable rows  
✅ CSV / PDF / PNG exports from tearsheet buttons  
✅ Resizable dividers on all tabs  
✅ evaljs works for automation (window ref in gui.app._webview_window)  
✅ Two-pass Plotly resize (300ms draw + 400/800/1500ms resize) — all charts render

---

## Known Issues / Next Sprint

### Factor Models
- [ ] Tortoriello sector tables still need wire-up to tearsheet (code written, not fully connected)
- [ ] Rolling 3Y chart needs real benchmark (currently universe only)  
- [ ] Crisis grid: some periods show N/A (9/11 correct, but COVID only has 1 data point at 6mo hold)
- [ ] Missing: factor value distribution by quintile (what raw factor scores are in each bucket)
- [ ] Score/Factor dropdown cuts off left edge in some window sizes (config row too wide)

### Portfolio Models Tab
- [ ] Build the Top-N portfolio backtest from a factor config
- [ ] Rebalance timing optimizer (month_end vs mid_month vs week4)
- [ ] Technical filter layer (price > 200MA, RSI etc)
- [ ] Promote to Results flow

### Results Tab
- [ ] Add PROMOTE button to strategy tearsheet header → saves to bank as PS-xxx
- [ ] Side-by-side comparison of multiple runs

### Tracker Tab
- [ ] Connect to real-time score data for rank refresh
- [ ] Build rebalance diff engine (what changed since last rebalance)
- [ ] Email report generator using SMTP

### Findings Tab
- [ ] Rich text editor instead of textarea
- [ ] Auto-populate strategy_id when logging from Factor Models
- [ ] Cycle summary view (group findings by date/sprint)
- [ ] Search/filter by text content

---

## Sprint Proposed Next Session

**Priority 1 — Portfolio Models Tab**
```
Factor config → Top-N backtest → optimize rebalance + sizing + tech filter → full tearsheet → promote to Results
```

**Priority 2 — Factor Tear Sheet completeness**
```
Wire Tortoriello sector tables fully
Add factor value distribution histograms per quintile  
Add benchmark column (universe) to main table
```

**Priority 3 — Results → Tracker promote flow**

---

## API Routes Reference

```
GET  /api/factor/scores           — available score columns
GET  /api/factor/score_range?score=X — date range for a score
POST /api/factor/run              — launch factor backtest
GET  /api/factor/status/{id}      — poll status
GET  /api/factor/stream/{id}      — SSE log stream  
GET  /api/factor/result/{id}      — get result (includes strategy_id)
GET  /api/factor/bank             — all saved models
GET  /api/factor/bank/{id}        — single model full data
POST /api/factor/bank/{id}/notes  — update notes/tags

GET/POST /api/findings            — get all / save new finding
DELETE   /api/findings/{id}       — delete finding

POST /api/backtest/run            — strategy backtest
POST /api/backtest/spy            — SPY benchmark
GET  /api/backtest/spy_preloaded  — cached SPY result
POST /api/export/csv              — metrics → Downloads CSV
POST /api/export/pdf              — 2-page PDF → Downloads
POST /api/export/image_data       — PNG → Downloads (from JS canvas)
GET  /api/evaljs                  — run JS in webview (dev tool)
GET  /api/snap                    — PIL screenshot (dev tool)
```

---

## Launch Commands

```powershell
# Launch app (double-click shortcut or run directly)
C:\Users\admin\Desktop\.venv-obq\Scripts\python.exe main.py

# Run factor backtest via API (Python)
import urllib.request, json
resp = json.loads(urllib.request.urlopen(
    urllib.request.Request('http://127.0.0.1:5744/api/factor/run',
    json.dumps({'score_column':'jcn_full_composite','n_buckets':5,
                'start_date':'2005-01-31','end_date':'2024-12-31',
                'hold_months':6,'min_price':5.0,'rebalance_freq':'semi-annual'}).encode(),
    {'Content-Type':'application/json'})).read().decode())

# Query bank
bank = json.loads(urllib.request.urlopen('http://127.0.0.1:5744/api/factor/bank').read().decode())
```
