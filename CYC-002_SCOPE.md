# CYC-002 — Comprehensive Factor Validation
**Status:** READY TO RUN (waiting for GPU)  
**Date Locked:** 2026-05-04  
**Researcher:** OBQ FactorLab

---

## Objective

Two primary goals:

1. **Validate our dataset and engine** against published results from Tortoriello (2009) and O'Shaughnessy (2012). If our factors match their published ICIR / excess return rankings, we confirm data integrity and engine accuracy.

2. **Extract maximum intelligence** from our data:
   - Which factors produce alpha in large-cap US equities (1990-2024)?
   - How do factors perform by sector, industry group, and market cap tier?
   - How persistent is each factor's alpha across decades (1990s, 2000s, 2010s, 2020s)?
   - What are the core ranked factors that survive across all market regimes?
   - Which two-factor combinations produce the strongest risk-adjusted returns?

---

## Universe

- **Market cap:** $10B+ (large-cap, ~top 1,000 US stocks)
- **Period:** 1990-07-31 to 2024-12-31 (34 years, 69 semi-annual periods)
- **Price filter:** $5+ minimum
- **Liquidity:** $1M+ ADV proxy
- **Benchmark:** Russell 3000 EW (Norgate PIT) for universe returns; SPX Total Return for fund-level alpha
- **Rebalance:** Semi-annual (June + December)
- **Hold:** 6 months
- **Cost:** 15 bps one-way

---

## Factor List — 50 Single Factors

### GROUP A — PROFITABILITY (10 factors)
*Source: PROD_OBQ_Quality_Scores, PROD_LONGEQ_SCORES, PROD_FUNDSMITH_SCORES*
*Tortoriello Chapter 15 validation targets*

| # | ID | Factor | Source Column | Direction | Tortoriello Target |
|---|---|---|---|---|---|
| 1 | `roic` | Return on Invested Capital | `roic` (Quality) | higher | Sharpe 0.83, Q1 excess ~4% |
| 2 | `cash_roc` | Cash Return on Capital | `cash_roc` (LongEQ) | higher | **Best quality, 10.9% Q1-Q5 spread** |
| 3 | `roce` | Return on Capital Employed | `roce` (Fundsmith) | higher | Fundsmith #1 metric |
| 4 | `roa` | Return on Assets | `roa` (Quality) | higher | Less leverage-distorted |
| 5 | `gpa` | Gross Profit / Assets | `gpa` (Quality) | higher | Novy-Marx (2013) signal |
| 6 | `op_margin` | Operating Margin | `op_margin` (Quality) | higher | Good — 2nd tier in chart |
| 7 | `profit_margin` | Net Income Margin | `profit_margin` (filings) | higher | Good — 2nd tier in chart |
| 8 | `fcf_margin` | FCF Margin | `fcf_margin` (Quality) | higher | Strong cash quality |
| 9 | `gross_margin` | Gross Margin | `gross_margin` (Quality) | higher | **Weakest quality — chart bottom** |
| 10 | `earnings_quality` | Earnings Quality Composite | `earnings_quality` (Quality) | higher | Accruals-based quality |

### GROUP B — FINANCIAL STRENGTH (10 factors)
*Source: PROD_OBQ_FinStr_Scores, PROD_LONGEQ_SCORES, PROD_FUNDSMITH_SCORES*
*Tortoriello Chapter 15 + chart validation — Interest Coverage is #1*

| # | ID | Factor | Source Column | Direction | Target |
|---|---|---|---|---|---|
| 11 | `interest_coverage` | Interest Coverage | `interest_coverage` (FinStr) | higher | **#1 in Piper Sandler chart** |
| 12 | `fcf_debt` | FCF / Debt | `fcf_debt` (FinStr) | higher | Debt coverage — strong |
| 13 | `net_debt_ebitda` | Net Debt / EBITDA | `net_debt_ebitda` (FinStr) | lower | Leverage quality |
| 14 | `debt_assets` | Debt / Assets | `debt_assets` (FinStr) | lower | Financial risk |
| 15 | `cash_assets` | Cash / Assets | `cash_assets` (FinStr) | higher | Liquidity buffer |
| 16 | `interest_pct_op` | Interest as % of Op Income | `interest_pct_op` (LongEQ) | lower | LongEQ quality gate |
| 17 | `capex_pct_ocf` | CapEx as % of OCF | `capex_pct_ocf` (LongEQ) | lower | Capital efficiency |
| 18 | `sharecount_chg_5yr` | Share Count Change 5yr | `sharecount_chg_5yr` (LongEQ) | lower | Anti-dilution signal |
| 19 | `cash_conversion` | Cash Conversion | `cash_conversion` (Fundsmith) | higher | Fundsmith quality gate |
| 20 | `net_debt_ebit` | Net Debt / EBIT | `net_debt_ebit` (Fundsmith) | lower | Fundsmith leverage gate |

### GROUP C — VALUATION (8 factors)
*Source: PROD_OBQ_Value_Scores, PROD_LONGEQ_SCORES, v_backtest_filings*
*Tortoriello Chapter 16 + O'Shaughnessy validation*

| # | ID | Factor | Source Column | Direction | Target |
|---|---|---|---|---|---|
| 21 | `ev_ebitda_ttm` | EV/EBITDA | `ev_ebitda_ttm` (Value) | lower | **Tortoriello #1, 5.3% excess, Sharpe 0.84** |
| 22 | `pfcf_ttm` | P/FCF | `pfcf_ttm` (Value) | lower | **Sharpe 0.89 — best single value** |
| 23 | `fcf_yield` | FCF Yield | `fcf_yield` (LongEQ) | higher | High yield = undervalued |
| 24 | `ps_ttm` | P/S | `ps_ttm` (Value) | lower | **O'Shaughnessy #1 value metric** |
| 25 | `pb_mrq` | P/B | `pb_mrq` (Value) | lower | Weakest single value metric |
| 26 | `pe_ttm` | P/E | `pe_ttm` (Value) | lower | Classic, weaker than EV metrics |
| 27 | `ev_sales` | EV/Sales | compute: EV/revenue (filings) | lower | Better than P/S for distortion |
| 28 | `revenue_growth_3yr` | Revenue Growth 3yr | `revenue_growth_3yr` (Fundsmith) | higher | O'Shaughnessy growth-value |

### GROUP D — GROWTH (8 factors)
*Source: PROD_OBQ_Growth_Scores, PROD_LONGEQ_SCORES*
*Tortoriello Chapter 18 + O'Shaughnessy*

| # | ID | Factor | Source Column | Direction | Target |
|---|---|---|---|---|---|
| 29 | `revenue_ps_cagr_1y` | Revenue/Share CAGR 1yr | Growth table | higher | Weak alone |
| 30 | `revenue_ps_cagr_3y` | Revenue/Share CAGR 3yr | Growth table | higher | More stable |
| 31 | `revenue_ps_cagr_5y` | Revenue/Share CAGR 5yr | Growth table | higher | Long-run compounder |
| 32 | `eps_cagr_1y` | EPS CAGR 1yr | Growth table | higher | Stronger than revenue |
| 33 | `eps_cagr_3y` | EPS CAGR 3yr | Growth table | higher | **Best growth stability** |
| 34 | `eps_cagr_5y` | EPS CAGR 5yr | Growth table | higher | Long-run earnings power |
| 35 | `fcf_ps_cagr_3y` | FCF/Share CAGR 3yr | Growth table | higher | Best growth quality |
| 36 | `fcf_cagr_5yr` | FCF CAGR 5yr | LongEQ table | higher | LongEQ compounding gate |

### GROUP E — MOMENTUM (6 factors)
*Source: PROD_OBQ_Momentum_Scores*
*Jegadeesh-Titman + Gray/Vogel FIP validation*

| # | ID | Factor | Source Column | Direction | Target |
|---|---|---|---|---|---|
| 37 | `af_r3m` | 3-Month Price Momentum | Momentum table | higher | Short-term, high turnover |
| 38 | `af_r6m` | 6-Month Price Momentum | Momentum table | higher | Intermediate |
| 39 | `af_r12m` | 12-Month Price Momentum | Momentum table | higher | **Standard 12-1 momentum** |
| 40 | `fip_6m` | FIP 6-Month (smooth path) | Momentum table | higher | Gray/Vogel smooth momentum |
| 41 | `fip_12m` | FIP 12-Month (smooth path) | Momentum table | higher | **Best quality momentum** |
| 42 | `systemscore` | Systematic/R² Score | Momentum table | higher | LongEQ-style price quality |

### GROUP F — CAPITAL ALLOCATION (3 factors)
*Source: PROD_LONGEQ_SCORES, PROD_OBQ_Value_Scores*
*O'Shaughnessy Shareholder Yield methodology*

| # | ID | Factor | Source Column | Direction | Target |
|---|---|---|---|---|---|
| 43 | `fcf_yield_cap` | FCF Yield (capital return proxy) | `fcf_yield` (LongEQ) | higher | Shareholder yield proxy |
| 44 | `sharecount_neg` | Anti-dilution (neg share growth) | `sharecount_chg_5yr` | lower | Buyback/discipline signal |
| 45 | `rd_ratio` | R&D / Revenue | `rd_ratio` (Moat) | higher | Innovation moat signal |

### GROUP G — MOAT COMPONENTS (5 factors)
*Source: PROD_MOAT_SCORES*
*Morningstar 5-moat-source decomposition*

| # | ID | Factor | Source Column | Direction | Target |
|---|---|---|---|---|---|
| 46 | `score_intangible` | Intangible Assets Moat | Moat table | higher | IP/brand competitive advantage |
| 47 | `score_switching` | Switching Cost Moat | Moat table | higher | Sticky revenue |
| 48 | `score_network` | Network Effect Moat | Moat table | higher | Platform advantage |
| 49 | `score_cost` | Cost Advantage Moat | Moat table | higher | Scale advantage |
| 50 | `score_scale` | Efficient Scale Moat | Moat table | higher | Oligopoly advantage |

---

## Two-Factor Combos — 19 Combinations

### Tortoriello Combos (T1-T8)
| # | Factor A | Factor B | Published Result | Validation Target |
|---|---|---|---|---|
| T1 | EV/EBITDA | Cash ROIC | +7.9% excess, Sharpe 0.87 | **His #1 combo** |
| T2 | EV/EBITDA | ROIC | +6.8% excess, Sharpe 0.90 | His #2 combo |
| T3 | P/FCF | ROIC | Value + Capital Return | Strong combo |
| T4 | P/S | FCF Margin | Revenue Value + Cash Quality | O'S bridge |
| T5 | EV/EBITDA | Interest Coverage | Value + Financial Fortress | Chapter 15+16 combo |
| T6 | EV/EBITDA | Operating Margin | Value + Profitability | Chapter 15+16 |
| T7 | FCF Yield | Net Debt/EBITDA | Yield + Balance Sheet | Income + Quality |
| T8 | EV/EBITDA | Earnings Quality | Value + Earnings Quality | Anti-manipulation |

### O'Shaughnessy Combos (O1-O5)
| # | Factor A | Factor B | Published Result | Validation Target |
|---|---|---|---|---|
| O1 | P/S | 12-Month Momentum | **"Reasonable Runaways" — his #1** | WWWS Chapter 20 |
| O2 | EV/EBITDA | 12-Month Momentum | Value + trend confirmation | Strong all-cap |
| O3 | P/S | ROA | Value + Quality, large-cap | WWWS combination |
| O4 | FCF Yield | 12-Month Momentum | Yield + Trend | Capital return + momentum |
| O5 | Value Composite (P/B+P/E+P/S+P/FCF+EV) | Momentum | **WWWS best system** | Multi-value + trend |

### Academic / Extended Combos (L1-L6)
| # | Factor A | Factor B | Source | Rationale |
|---|---|---|---|---|
| L1 | ROIC | 12-Month Momentum | Asness/Moskowitz (2013) | Quality momentum — negatively correlated |
| L2 | GPA | P/B | Novy-Marx (2013) | Profitability within value |
| L3 | Interest Coverage | EV/EBITDA | Tortoriello | Financial fortress + cheap |
| L4 | FCF Margin | EPS CAGR 3yr | Quality compounder | Fundsmith-style growth quality |
| L5 | Operating Margin | Revenue CAGR 3yr | Growth quality | Early-stage growth screen |
| L6 | ROIC | Debt/Assets | Piotroski-inspired | Quality + low leverage safety |

---

## Intelligence Extraction Framework

Beyond pure factor performance, CYC-002 extracts:

### 1. Sector × Factor Matrix
For each of the 50 factors, compute ICIR and Q1 excess return broken out by GIC sector:
- Energy, Materials, Industrials, Consumer Disc, Consumer Staples, Healthcare,
  Financials, IT, Communication, Utilities, Real Estate
- **Expected:** Interest Coverage weak in Financials (different business model),
  GPA strong in Tech, FCF Yield strong in Energy

### 2. Decade Persistence
Split 1990-2024 into four decades and compute ICIR each period:
- 1990-1999 | 2000-2009 | 2010-2019 | 2020-2024
- Identifies: Is the factor getting weaker (arbitraged away) or consistent?

### 3. Bear/Bull Factor Map
Existing bear_score / bull_score computed per factor.
Build a ranked matrix: which factors are most defensive? Most offensive?

### 4. Factor Orthogonality Analysis
Compute IC correlation between all factor pairs across periods.
Identifies: Which factors are genuinely independent (low correlation) = best diversifiers?
Which are redundant (high correlation) = don't add in combos?

### 5. Publication Comparison Table
Side-by-side comparison of OBQ results vs published targets:
- Our ICIR vs Tortoriello's excess return ranking
- Our Q1-Q5 spread vs O'Shaughnessy's Q1 excess
- Pass/fail: within 20% of published result = CONFIRMED

---

## Cycle Roadmap Context

```
CYC-001 ✅  Large-cap ($10B+) — JCN composites + individual scores (DONE)
CYC-002 ← THIS CYCLE
            Large-cap ($10B+) — 50 singles + 19 combos
            Full Tortoriello/O'Shaughnessy validation suite
CYC-003     Mid-cap ($2B-$10B) — Same 50 factors
            Does momentum revive? Do quality factors hold?
CYC-004     Small-cap ($300M-$2B) — Same 50 factors
            Where value premium is strongest historically
CYC-005     Micro-cap ($25M-$200M) — TINY TITANS UNIVERSE
            O'Shaughnessy Tiny Titans exact spec
            + Our full factor list for comparison
            Data confirmed: 200-1,900 stocks/year available
CYC-006     All-cap combined — Universe-aware factor timing
```

---

## Execution Plan

**Batch 1 — Group A+B (Profitability + Financial Strength): 20 factors**
- Data source: PROD_OBQ_Quality_Scores + PROD_OBQ_FinStr_Scores + PROD_LONGEQ_SCORES + PROD_FUNDSMITH_SCORES
- Engine: These tables join to v_backtest_prices the same way as existing factors
- Expected runtime: ~30 min (20 × ~90s each)

**Batch 2 — Group C+D (Valuation + Growth): 16 factors**
- Data source: PROD_OBQ_Value_Scores + PROD_OBQ_Growth_Scores + PROD_LONGEQ_SCORES + v_backtest_filings
- Note: ev_sales needs to be computed from filings (EV/revenue ratio)
- Expected runtime: ~25 min

**Batch 3 — Group E+F+G (Momentum + Capital + Moat): 14 factors**
- Data source: PROD_OBQ_Momentum_Scores + PROD_MOAT_SCORES + PROD_LONGEQ_SCORES
- Expected runtime: ~20 min

**Batch 4 — Two-Factor Combos: 19 combos**
- Method: rank by score_a × score_b composite rank (equal weight of both percentile ranks)
- Expected runtime: ~30 min

**Total estimated runtime: ~105 min factor + ~105 min portfolio = ~3.5 hours**
*(Can be parallelized once GPU is free)*

---

## Success Criteria

| Check | Target |
|---|---|
| Interest Coverage ICIR | > 1.0 (top of quality chart) |
| ROIC ICIR | > 0.8 (Tortoriello Sharpe 0.83) |
| Gross Margin ICIR | < 0.4 (weakest quality — chart bottom) |
| EV/EBITDA ICIR | > 0.8 (Tortoriello 5.3% excess, Sharpe 0.84) |
| P/S ICIR | > 0.7 (O'Shaughnessy #1 value metric) |
| P/S + Momentum combo ICIR | > P/S alone (WWWS "Reasonable Runaways") |
| EV/EBITDA + ROIC combo ICIR | > both alone (Tortoriello +1.5% improvement) |
| 12-1 Momentum ICIR | > 0.5 (standard, but dead in large-cap from CYC-001) |
| Gross Margin ranking | Should rank LOWER than ROIC, ROA, Operating Margin |

**If our rankings match within ±1-2 rank positions of Tortoriello's published table: DATA VALIDATED.**
