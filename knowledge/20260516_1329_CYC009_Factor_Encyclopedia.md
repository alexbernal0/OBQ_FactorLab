# OBQ FactorLab Research Encyclopedia
## CYC-008 / CYC-009 — Factor & Portfolio Model Results
**Generated**: 2026-05-16 13:29  
**Period**: 1995-03-31 to 2024-12-31 (120 quarterly periods)  
**Benchmark**: S&P 500 (SPX spliced) — CAGR=10.7%, MaxDD=-50.8%  
**Pipeline**: GPU-accelerated (CuPy) top-N selection + CPU tranche simulation  

---

## Executive Summary

### Research Cycles
| Cycle | Description | Models |
|---|---|---|
| CYC-003 to CYC-007 | Factor backtests — quintile sort, semi-annual rebalance, All-Cap R3000 | 2,854 factor models |
| CYC-008 | Portfolio backtests — 28 stocks, 4 tranches, quarterly stagger, 12-mo hold, All-Cap | 111 portfolio models |
| CYC-009 | Portfolio backtests — same config, split by market cap quartile (point-in-time) | 440 portfolio models (4 tiers x 110) |

### Portfolio Construction (CYC-008/009)
| Parameter | Value |
|---|---|
| Total stocks | 28 (4 tranches x 7 stocks) |
| Rebalance | Quarterly stagger (T1→Mar, T2→Jun, T3→Sep, T4→Dec) |
| Hold period | 12 months per tranche |
| Cost | 15bps one-way |
| Weight | Equal weight within tranche |
| Sector cap | None |
| Cap tiers (CYC-009) | Point-in-time quartiles: Small (Q1), Mid (Q2), Large (Q3), Mega (Q4) |

### Key Findings

1. **0.4% of portfolio models beat SPX** — only 2 out of 551 models (both Small_Cap)
2. **Small_Cap is the only tier with positive average alpha** (+0.9%) — wins 41% of factor comparisons
3. **Factor signal decays 3-8% CAGR** going from quintile backtest Q1 to best portfolio across tiers
4. **JCN GARP in Large_Cap** had the smallest signal decay (-1.2%) — best factor-to-portfolio translation
5. **28-stock concentration is the primary alpha destroyer** — the factor signal exists but can't survive in a concentrated portfolio
6. **Mid_Cap is a dead zone** — worst win rate (9%), no edge in any factor category

---
## Tier Performance Comparison

| Tier | N | Avg CAGR | Max CAGR | Avg MaxDD | Avg Alpha | Avg Sharpe | Beat SPX |
|---|---|---|---|---|---|---|---|
| **Small_Cap** | 110 | +4.7% | +14.2% | -52.7% | +0.9% | 0.01 | 2/110 |
| **Large_Cap** | 110 | +3.8% | +10.2% | -54.7% | -0.3% | -0.04 | 0/110 |
| **Mega_Cap** | 110 | +3.1% | +7.9% | -53.7% | -0.9% | -0.06 | 0/110 |
| **Mid_Cap** | 110 | +3.0% | +7.1% | -55.3% | -0.9% | -0.07 | 0/110 |
| **All_Cap** | 111 | +2.4% | +8.7% | -56.4% | -1.7% | -0.11 | 0/111 |
| *SPX Benchmark* | — | +10.7% | — | -50.8% | — | 0.60 | — |

### Tier Win Rates
Small_Cap wins highest CAGR for **41%** of factors, Large_Cap 23%, Mega_Cap 14%, All_Cap 13%, Mid_Cap 9%.

---
## Factor Encyclopedia — Full Results

Each factor entry includes:
- **Factor Backtest** (CYC-003 to CYC-007): Quintile spread, Q1 CAGR, IC metrics
- **Portfolio Backtest** (CYC-008/009): CAGR, MaxDD, Alpha, R² per cap tier
- **Signal Translation**: Gap between factor Q1 CAGR and best portfolio CAGR

---
### JCN Alpha Trifecta
**Score column**: `jcn_alpha_trifecta` | **Direction**: Higher is better | **Factor OBQ**: 0.871

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.2% |
| Q5 CAGR | -4.9% |
| Q1-Q5 Spread | +20.8% |
| ICIR | 2.38 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 93% |
| Staircase | 0.14 |
| Q1 Sharpe | 0.89 |
| Q1 MaxDD | -23.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +7.7% | -60.6% | +3.7% | 0.850 | 0.17 | 0.653 |
| Mega_Cap | +5.9% | -51.9% | +2.4% | 0.798 | 0.09 | 0.524 |
| Large_Cap | +5.3% | -52.4% | +2.3% | 0.742 | 0.07 | 0.530 |
| All_Cap | +4.6% | -56.9% | +1.3% | 0.566 | 0.03 | 0.530 |
| Mid_Cap | +2.0% | -57.5% | -2.2% | 0.252 | -0.10 | 0.423 |

**Signal Translation**: Best portfolio = **Small_Cap** at +7.7% CAGR | Factor Q1 = +12.2% | Gap = **-4.6%**

---
### JCN QARP
**Score column**: `jcn_qarp` | **Direction**: Higher is better | **Factor OBQ**: 0.857

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.1% |
| Q5 CAGR | -5.6% |
| Q1-Q5 Spread | +21.3% |
| ICIR | 2.38 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 93% |
| Staircase | 0.14 |
| Q1 Sharpe | 0.89 |
| Q1 MaxDD | -22.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +7.5% | -61.2% | +3.5% | 0.712 | 0.15 | 0.620 |
| All_Cap | +5.4% | -58.7% | +2.1% | 0.523 | 0.06 | 0.562 |
| Mega_Cap | +4.4% | -56.1% | +0.4% | 0.693 | 0.02 | 0.538 |
| Large_Cap | +3.8% | -59.6% | -0.3% | 0.473 | -0.01 | 0.517 |
| Mid_Cap | -0.2% | -66.7% | -4.8% | 0.091 | -0.19 | 0.346 |

**Signal Translation**: Best portfolio = **Small_Cap** at +7.5% CAGR | Factor Q1 = +12.1% | Gap = **-4.6%**

---
### JCN Fortress
**Score column**: `jcn_fortress` | **Direction**: Higher is better | **Factor OBQ**: 0.824

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.0% |
| Q5 CAGR | +0.1% |
| Q1-Q5 Spread | +15.6% |
| ICIR | 2.33 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 97% |
| Staircase | 0.12 |
| Q1 Sharpe | 0.90 |
| Q1 MaxDD | -24.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.5% | -47.7% | +5.0% | 0.823 | 0.18 | 0.587 |
| Mega_Cap | +7.6% | -49.3% | +3.3% | 0.882 | 0.17 | 0.592 |
| All_Cap | +7.0% | -55.8% | +3.5% | 0.651 | 0.13 | 0.582 |
| Large_Cap | +4.7% | -57.4% | +1.1% | 0.600 | 0.03 | 0.541 |
| Mid_Cap | +2.2% | -59.0% | -1.7% | 0.408 | -0.08 | 0.441 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.5% CAGR | Factor Q1 = +12.0% | Gap = **-3.5%**

---
### JCN Composite
**Score column**: `jcn_full_composite` | **Direction**: Higher is better | **Factor OBQ**: 0.822

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.1% |
| Q5 CAGR | -1.7% |
| Q1-Q5 Spread | +17.7% |
| ICIR | 2.48 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 93% |
| Staircase | 0.13 |
| Q1 Sharpe | 0.87 |
| Q1 MaxDD | -26.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.9% | -53.7% | +5.1% | 0.887 | 0.22 | 0.634 |
| All_Cap | +8.7% | -51.3% | +4.6% | 0.895 | 0.21 | 0.594 |
| Large_Cap | +8.3% | -57.2% | +4.1% | 0.796 | 0.19 | 0.618 |
| Mega_Cap | +5.6% | -61.8% | +1.4% | 0.794 | 0.08 | 0.610 |
| Mid_Cap | +1.6% | -66.7% | -2.6% | 0.434 | -0.11 | 0.496 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.9% CAGR | Factor Q1 = +12.1% | Gap = **-3.2%**

---
### OBQ Quality (Universe)
**Score column**: `quality_score_universe` | **Direction**: Higher is better | **Factor OBQ**: 0.821

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.5% |
| Q5 CAGR | -4.5% |
| Q1-Q5 Spread | +20.1% |
| ICIR | 2.01 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 93% |
| Staircase | 0.13 |
| Q1 Sharpe | 0.94 |
| Q1 MaxDD | -24.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +7.9% | -46.2% | +4.1% | 0.947 | 0.17 | 0.592 |
| Large_Cap | +4.6% | -58.4% | +0.3% | 0.646 | 0.03 | 0.518 |
| Small_Cap | +4.6% | -52.6% | +1.4% | 0.802 | 0.03 | 0.532 |
| All_Cap | +4.0% | -46.7% | +1.0% | 0.724 | 0.00 | 0.471 |
| Mid_Cap | +3.7% | -58.5% | +0.2% | 0.777 | -0.01 | 0.492 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +7.9% CAGR | Factor Q1 = +12.5% | Gap = **-4.7%**

---
### OBQ Quality
**Score column**: `quality_score` | **Direction**: Higher is better | **Factor OBQ**: 0.821

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.4% |
| Q5 CAGR | -2.9% |
| Q1-Q5 Spread | +18.7% |
| ICIR | 2.10 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 97% |
| Staircase | 0.13 |
| Q1 Sharpe | 0.93 |
| Q1 MaxDD | -23.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +7.9% | -54.5% | +3.8% | 0.856 | 0.18 | 0.596 |
| Small_Cap | +7.5% | -42.6% | +4.3% | 0.905 | 0.18 | 0.539 |
| All_Cap | +7.4% | -41.9% | +4.3% | 0.930 | 0.16 | 0.547 |
| Mega_Cap | +5.1% | -53.6% | +0.6% | 0.680 | 0.06 | 0.532 |
| Mid_Cap | +0.2% | -58.1% | -3.9% | 0.172 | -0.18 | 0.308 |

**Signal Translation**: Best portfolio = **Large_Cap** at +7.9% CAGR | Factor Q1 = +12.4% | Gap = **-4.5%**

---
### JCN Value-Momentum
**Score column**: `jcn_value_momentum` | **Direction**: Higher is better | **Factor OBQ**: 0.808

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.3% |
| Q5 CAGR | -2.5% |
| Q1-Q5 Spread | +16.8% |
| ICIR | 2.25 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.11 |
| Q1 Sharpe | 0.78 |
| Q1 MaxDD | -26.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +7.6% | -52.8% | +3.9% | 0.897 | 0.17 | 0.610 |
| All_Cap | +6.6% | -49.8% | +3.0% | 0.748 | 0.13 | 0.542 |
| Large_Cap | +4.6% | -53.7% | +0.7% | 0.668 | 0.03 | 0.471 |
| Mid_Cap | +4.2% | -52.1% | +0.6% | 0.786 | 0.01 | 0.510 |
| Small_Cap | +3.8% | -64.6% | -0.3% | 0.312 | -0.01 | 0.504 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +7.6% CAGR | Factor Q1 = +11.3% | Gap = **-3.6%**

---
### OCF / Assets (CYC4)
**Score column**: `cyc4_ocf_assets` | **Direction**: Higher is better | **Factor OBQ**: 0.807

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.6% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +19.1% |
| ICIR | 1.95 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 90% |
| Staircase | 0.12 |
| Q1 Sharpe | 0.83 |
| Q1 MaxDD | -28.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +3.2% | -52.5% | -1.1% | 0.824 | -0.05 | 0.458 |
| Mega_Cap | +2.3% | -46.7% | -1.6% | 0.294 | -0.14 | 0.268 |
| Small_Cap | +1.3% | -47.4% | -2.2% | 0.050 | -0.19 | 0.286 |
| Mid_Cap | +1.0% | -57.5% | -3.0% | 0.017 | -0.18 | 0.318 |
| All_Cap | +0.6% | -34.2% | -3.3% | 0.530 | -0.29 | 0.226 |

**Signal Translation**: Best portfolio = **Large_Cap** at +3.2% CAGR | Factor Q1 = +11.6% | Gap = **-8.4%**

---
### JCN GARP
**Score column**: `jcn_garp` | **Direction**: Higher is better | **Factor OBQ**: 0.794

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.3% |
| Q5 CAGR | -1.9% |
| Q1-Q5 Spread | +16.3% |
| ICIR | 2.51 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 90% |
| Staircase | 0.11 |
| Q1 Sharpe | 0.78 |
| Q1 MaxDD | -26.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +10.2% | -61.1% | +5.7% | 0.904 | 0.29 | 0.660 |
| All_Cap | +5.9% | -62.3% | +2.0% | 0.826 | 0.09 | 0.585 |
| Mega_Cap | +5.6% | -69.8% | +1.8% | 0.517 | 0.07 | 0.591 |
| Small_Cap | +5.3% | -71.4% | +1.9% | 0.316 | 0.06 | 0.581 |
| Mid_Cap | +1.8% | -77.6% | -1.7% | 0.399 | -0.09 | 0.481 |

**Signal Translation**: Best portfolio = **Large_Cap** at +10.2% CAGR | Factor Q1 = +11.3% | Gap = **-1.2%**

---
### Retained Earnings / Total Assets
**Score column**: `cyc4_retained_earnings_ta` | **Direction**: Higher is better | **Factor OBQ**: 0.789

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.1% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +16.4% |
| ICIR | 1.60 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.11 |
| Q1 Sharpe | 0.84 |
| Q1 MaxDD | -30.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +5.9% | -41.7% | +2.7% | 0.928 | 0.10 | 0.503 |
| All_Cap | +4.5% | -33.0% | +1.2% | 0.928 | 0.03 | 0.486 |
| Mid_Cap | +3.1% | -33.7% | -1.5% | 0.858 | -0.06 | 0.410 |
| Small_Cap | +2.7% | -45.0% | -1.0% | 0.799 | -0.08 | 0.413 |
| Large_Cap | +0.1% | -45.4% | -3.5% | 0.107 | -0.28 | 0.130 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +5.9% CAGR | Factor Q1 = +11.1% | Gap = **-5.2%**

---
### Piotroski F-Score
**Score column**: `cyc4_fscore` | **Direction**: Higher is better | **Factor OBQ**: 0.789

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.6% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +15.4% |
| ICIR | 1.76 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.11 |
| Q1 Sharpe | 0.84 |
| Q1 MaxDD | -34.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +4.1% | -46.9% | -0.0% | 0.465 | 0.00 | 0.449 |
| Large_Cap | +3.0% | -43.2% | -1.3% | 0.640 | -0.06 | 0.363 |
| All_Cap | +1.8% | -48.3% | -2.1% | 0.215 | -0.15 | 0.253 |
| Mega_Cap | +0.9% | -38.3% | -2.9% | 0.635 | -0.25 | 0.242 |
| Mid_Cap | -0.1% | -35.4% | -4.2% | 0.048 | -0.38 | 0.025 |

**Signal Translation**: Best portfolio = **Small_Cap** at +4.1% CAGR | Factor Q1 = +11.6% | Gap = **-7.5%**

---
### Moat Score
**Score column**: `moat_score` | **Direction**: Higher is better | **Factor OBQ**: 0.788

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.5% |
| Q5 CAGR | +1.1% |
| Q1-Q5 Spread | +13.7% |
| ICIR | 1.61 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 93% |
| Staircase | 0.11 |
| Q1 Sharpe | 0.93 |
| Q1 MaxDD | -23.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +7.7% | -64.2% | +4.1% | 0.873 | 0.15 | 0.661 |
| Mid_Cap | +5.7% | -50.5% | +2.9% | 0.886 | 0.08 | 0.547 |
| Small_Cap | +5.0% | -47.5% | +1.4% | 0.874 | 0.05 | 0.498 |
| All_Cap | +3.7% | -56.9% | -1.2% | 0.516 | -0.01 | 0.465 |
| Mega_Cap | +2.1% | -74.4% | -2.9% | 0.009 | -0.09 | 0.377 |

**Signal Translation**: Best portfolio = **Large_Cap** at +7.7% CAGR | Factor Q1 = +12.5% | Gap = **-4.8%**

---
### JCN Quality-Momentum
**Score column**: `jcn_quality_momentum` | **Direction**: Higher is better | **Factor OBQ**: 0.778

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.3% |
| Q5 CAGR | -1.1% |
| Q1-Q5 Spread | +17.2% |
| ICIR | 1.84 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 90% |
| Staircase | 0.13 |
| Q1 Sharpe | 0.90 |
| Q1 MaxDD | -25.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +7.8% | -54.8% | +3.8% | 0.822 | 0.18 | 0.583 |
| Mega_Cap | +6.7% | -59.6% | +3.1% | 0.849 | 0.12 | 0.618 |
| Small_Cap | +6.1% | -49.4% | +2.1% | 0.767 | 0.10 | 0.530 |
| Large_Cap | +6.1% | -64.9% | +2.0% | 0.538 | 0.10 | 0.597 |
| Mid_Cap | +5.5% | -53.8% | +0.8% | 0.843 | 0.06 | 0.552 |

**Signal Translation**: Best portfolio = **All_Cap** at +7.8% CAGR | Factor Q1 = +12.3% | Gap = **-4.5%**

---
### JCN Growth-Quality-Momentum
**Score column**: `jcn_growth_quality_momentum` | **Direction**: Higher is better | **Factor OBQ**: 0.777

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.0% |
| Q5 CAGR | -0.5% |
| Q1-Q5 Spread | +16.5% |
| ICIR | 1.91 |
| IC Hit Rate | 90% |
| Monotonicity | 100% |
| Alpha Win Rate | 90% |
| Staircase | 0.12 |
| Q1 Sharpe | 0.82 |
| Q1 MaxDD | -27.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +8.1% | -55.6% | +3.9% | 0.885 | 0.17 | 0.619 |
| Small_Cap | +7.9% | -57.2% | +4.3% | 0.798 | 0.19 | 0.636 |
| Large_Cap | +7.8% | -50.3% | +3.8% | 0.892 | 0.16 | 0.562 |
| Mega_Cap | +7.6% | -52.4% | +4.3% | 0.861 | 0.16 | 0.613 |
| Mid_Cap | +4.5% | -55.4% | +0.1% | 0.760 | 0.02 | 0.513 |

**Signal Translation**: Best portfolio = **All_Cap** at +8.1% CAGR | Factor Q1 = +12.0% | Gap = **-3.9%**

---
### Moat (Scale)
**Score column**: `cyc2_moat_scale` | **Direction**: Higher is better | **Factor OBQ**: 0.777

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.9% |
| Q5 CAGR | +3.6% |
| Q1-Q5 Spread | +10.4% |
| ICIR | 2.28 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 93% |
| Staircase | 0.07 |
| Q1 Sharpe | 0.89 |
| Q1 MaxDD | -22.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +7.7% | -54.1% | +4.0% | 0.924 | 0.16 | 0.598 |
| Mid_Cap | +6.0% | -49.3% | +2.4% | 0.879 | 0.09 | 0.544 |
| Mega_Cap | +5.6% | -53.2% | +1.9% | 0.894 | 0.07 | 0.551 |
| Small_Cap | +5.3% | -62.4% | +1.9% | 0.855 | 0.06 | 0.560 |
| Large_Cap | +1.8% | -62.6% | -1.6% | 0.299 | -0.09 | 0.408 |

**Signal Translation**: Best portfolio = **All_Cap** at +7.7% CAGR | Factor Q1 = +12.9% | Gap = **-5.3%**

---
### LongEQ Rank
**Score column**: `longeq_rank` | **Direction**: Lower is better | **Factor OBQ**: 0.775

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.7% |
| Q5 CAGR | +2.6% |
| Q1-Q5 Spread | +11.6% |
| ICIR | 1.84 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 93% |
| Staircase | 0.10 |
| Q1 Sharpe | 0.85 |
| Q1 MaxDD | -21.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.7% | -56.6% | +4.9% | 0.939 | 0.19 | 0.639 |
| Mega_Cap | +6.2% | -56.4% | +1.9% | 0.896 | 0.09 | 0.579 |
| Large_Cap | +6.1% | -64.7% | +1.7% | 0.860 | 0.08 | 0.593 |
| Mid_Cap | +5.0% | -58.7% | +1.2% | 0.711 | 0.04 | 0.544 |
| All_Cap | -2.9% | -84.9% | -7.6% | 0.410 | -0.27 | 0.230 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.7% CAGR | Factor Q1 = +11.7% | Gap = **-3.1%**

---
### Industrials Efficiency
**Score column**: `cyc5_industrials_efficiency` | **Direction**: Higher is better | **Factor OBQ**: 0.774

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.1% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +15.5% |
| ICIR | 1.72 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.09 |
| Q1 Sharpe | 0.79 |
| Q1 MaxDD | -28.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +3.1% | -51.9% | -0.9% | 0.822 | -0.05 | 0.451 |
| Small_Cap | +2.4% | -45.3% | -1.0% | 0.454 | -0.11 | 0.377 |
| Mega_Cap | +1.9% | -46.3% | -2.0% | 0.210 | -0.18 | 0.230 |
| All_Cap | +0.8% | -38.7% | -3.4% | 0.626 | -0.27 | 0.264 |
| Mid_Cap | +0.6% | -60.1% | -3.2% | 0.003 | -0.20 | 0.293 |

**Signal Translation**: Best portfolio = **Large_Cap** at +3.1% CAGR | Factor Q1 = +11.1% | Gap = **-8.0%**

---
### EBIT / Assets
**Score column**: `cyc4_ebit_assets` | **Direction**: Higher is better | **Factor OBQ**: 0.773

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.0% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +17.6% |
| ICIR | 1.68 |
| IC Hit Rate | 97% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.11 |
| Q1 Sharpe | 0.81 |
| Q1 MaxDD | -29.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +5.9% | -35.0% | +1.8% | 0.856 | 0.11 | 0.517 |
| All_Cap | +5.7% | -45.2% | +1.7% | 0.848 | 0.09 | 0.504 |
| Small_Cap | +4.2% | -31.3% | +0.5% | 0.673 | 0.01 | 0.428 |
| Mid_Cap | +3.1% | -57.7% | -1.3% | 0.536 | -0.04 | 0.481 |
| Large_Cap | +2.0% | -55.0% | -1.8% | 0.690 | -0.12 | 0.449 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +5.9% CAGR | Factor Q1 = +11.0% | Gap = **-5.1%**

---
### Return on Capital Employed
**Score column**: `cyc2_roce` | **Direction**: Higher is better | **Factor OBQ**: 0.741

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.8% |
| Q5 CAGR | -3.3% |
| Q1-Q5 Spread | +17.9% |
| ICIR | 1.98 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 90% |
| Staircase | 0.12 |
| Q1 Sharpe | 0.86 |
| Q1 MaxDD | -25.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +4.3% | -69.5% | +0.3% | 0.370 | 0.01 | 0.568 |
| Small_Cap | +3.8% | -65.5% | +0.1% | 0.698 | -0.01 | 0.569 |
| Mega_Cap | +3.3% | -48.7% | -0.7% | 0.731 | -0.03 | 0.455 |
| All_Cap | +3.1% | -65.4% | -0.2% | 0.682 | -0.04 | 0.496 |
| Mid_Cap | -0.7% | -73.9% | -3.9% | 0.102 | -0.20 | 0.313 |

**Signal Translation**: Best portfolio = **Large_Cap** at +4.3% CAGR | Factor Q1 = +11.8% | Gap = **-7.5%**

---
### Cash Return on Capital
**Score column**: `cyc2_cash_roc` | **Direction**: Higher is better | **Factor OBQ**: 0.729

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.8% |
| Q5 CAGR | -2.5% |
| Q1-Q5 Spread | +15.5% |
| ICIR | 1.71 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.10 |
| Q1 Sharpe | 0.79 |
| Q1 MaxDD | -25.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +5.8% | -54.6% | +2.4% | 0.820 | 0.09 | 0.525 |
| All_Cap | +5.6% | -56.3% | +2.1% | 0.847 | 0.08 | 0.531 |
| Mid_Cap | +3.4% | -50.5% | +0.3% | 0.793 | -0.03 | 0.451 |
| Large_Cap | +2.1% | -57.8% | -2.2% | 0.012 | -0.10 | 0.351 |
| Mega_Cap | +1.4% | -57.0% | -3.1% | 0.140 | -0.14 | 0.401 |

**Signal Translation**: Best portfolio = **Small_Cap** at +5.8% CAGR | Factor Q1 = +10.8% | Gap = **-5.1%**

---
### ROIC
**Score column**: `cyc2_roic` | **Direction**: Higher is better | **Factor OBQ**: 0.729

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.8% |
| Q5 CAGR | -2.5% |
| Q1-Q5 Spread | +15.5% |
| ICIR | 1.71 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.10 |
| Q1 Sharpe | 0.79 |
| Q1 MaxDD | -25.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +5.8% | -54.6% | +2.4% | 0.820 | 0.09 | 0.525 |
| All_Cap | +5.6% | -56.3% | +2.1% | 0.847 | 0.08 | 0.531 |
| Mid_Cap | +3.4% | -50.5% | +0.3% | 0.793 | -0.03 | 0.451 |
| Large_Cap | +2.1% | -57.8% | -2.2% | 0.012 | -0.10 | 0.351 |
| Mega_Cap | +1.4% | -57.0% | -3.1% | 0.140 | -0.14 | 0.401 |

**Signal Translation**: Best portfolio = **Small_Cap** at +5.8% CAGR | Factor Q1 = +10.8% | Gap = **-5.1%**

---
### FCF Margin
**Score column**: `cyc2_fcf_margin` | **Direction**: Higher is better | **Factor OBQ**: 0.715

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.1% |
| Q5 CAGR | -3.0% |
| Q1-Q5 Spread | +16.8% |
| ICIR | 1.92 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 77% |
| Staircase | 0.08 |
| Q1 Sharpe | 0.84 |
| Q1 MaxDD | -25.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +6.5% | -52.7% | +3.3% | 0.786 | 0.12 | 0.552 |
| Small_Cap | +5.8% | -47.7% | +2.4% | 0.853 | 0.09 | 0.539 |
| Mega_Cap | +4.8% | -48.2% | +0.9% | 0.534 | 0.04 | 0.486 |
| Large_Cap | +3.9% | -69.9% | +0.2% | 0.108 | -0.01 | 0.512 |
| All_Cap | +2.5% | -47.5% | -1.5% | 0.487 | -0.08 | 0.401 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +6.5% CAGR | Factor Q1 = +11.1% | Gap = **-4.6%**

---
### Energy Mid-Cycle FCF
**Score column**: `cyc5_energy_midcycle_fcf` | **Direction**: Higher is better | **Factor OBQ**: 0.704

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.4% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +9.4% |
| ICIR | 1.74 |
| IC Hit Rate | 95% |
| Monotonicity | 100% |
| Alpha Win Rate | 67% |
| Staircase | 0.07 |
| Q1 Sharpe | 0.62 |
| Q1 MaxDD | -21.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +6.1% | -39.8% | +2.0% | 0.852 | 0.12 | 0.426 |
| All_Cap | +5.2% | -26.4% | +0.0% | 0.936 | 0.08 | 0.415 |
| Mid_Cap | +3.4% | -36.1% | -0.8% | 0.707 | -0.04 | 0.348 |
| Large_Cap | +2.8% | -31.6% | -1.9% | 0.844 | -0.09 | 0.383 |
| Small_Cap | -0.3% | -45.1% | -4.7% | 0.448 | -0.49 | 0.085 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +6.1% CAGR | Factor Q1 = +9.4% | Gap = **-3.4%**

---
### Return on Assets
**Score column**: `cyc2_roa` | **Direction**: Higher is better | **Factor OBQ**: 0.693

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.1% |
| Q5 CAGR | -4.0% |
| Q1-Q5 Spread | +17.5% |
| ICIR | 1.84 |
| IC Hit Rate | 90% |
| Monotonicity | 100% |
| Alpha Win Rate | 83% |
| Staircase | 0.10 |
| Q1 Sharpe | 0.74 |
| Q1 MaxDD | -26.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +6.9% | -48.7% | +3.0% | 0.767 | 0.12 | 0.549 |
| Large_Cap | +3.9% | -69.8% | -0.5% | 0.321 | -0.00 | 0.537 |
| Small_Cap | +3.3% | -64.5% | -0.2% | 0.698 | -0.04 | 0.544 |
| Mid_Cap | +1.2% | -61.5% | -2.7% | 0.217 | -0.12 | 0.334 |
| All_Cap | +0.4% | -60.3% | -2.9% | 0.002 | -0.18 | 0.287 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +6.9% CAGR | Factor Q1 = +10.1% | Gap = **-3.2%**

---
### GPA (Gross Profit/Assets)
**Score column**: `cyc2_gpa` | **Direction**: Higher is better | **Factor OBQ**: 0.683

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.4% |
| Q5 CAGR | -1.4% |
| Q1-Q5 Spread | +14.4% |
| ICIR | 1.54 |
| IC Hit Rate | 90% |
| Monotonicity | 100% |
| Alpha Win Rate | 83% |
| Staircase | 0.09 |
| Q1 Sharpe | 0.76 |
| Q1 MaxDD | -28.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +7.7% | -45.1% | +5.4% | 0.939 | 0.17 | 0.567 |
| Large_Cap | +5.9% | -59.5% | +1.9% | 0.804 | 0.09 | 0.553 |
| All_Cap | +3.0% | -55.1% | -0.8% | 0.715 | -0.05 | 0.480 |
| Mega_Cap | +2.1% | -54.0% | -2.0% | 0.086 | -0.10 | 0.398 |
| Mid_Cap | +0.3% | -61.3% | -2.8% | 0.088 | -0.18 | 0.328 |

**Signal Translation**: Best portfolio = **Small_Cap** at +7.7% CAGR | Factor Q1 = +10.4% | Gap = **-2.7%**

---
### Real Estate FFO Yield
**Score column**: `cyc5_realestate_ffo_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.671

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.9% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +12.4% |
| ICIR | 1.41 |
| IC Hit Rate | 95% |
| Monotonicity | 100% |
| Alpha Win Rate | 80% |
| Staircase | 0.08 |
| Q1 Sharpe | 0.55 |
| Q1 MaxDD | -25.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +3.5% | -28.0% | -1.0% | 0.771 | -0.04 | 0.419 |
| Mid_Cap | +2.9% | -41.8% | -1.6% | 0.855 | -0.07 | 0.389 |
| Mega_Cap | +2.2% | -30.0% | -2.3% | 0.812 | -0.14 | 0.336 |
| Large_Cap | +1.4% | -38.0% | -3.3% | 0.512 | -0.20 | 0.219 |
| Small_Cap | +0.7% | -39.0% | -3.6% | 0.520 | -0.27 | 0.191 |

**Signal Translation**: Best portfolio = **All_Cap** at +3.5% CAGR | Factor Q1 = +8.9% | Gap = **-5.4%**

---
### Sales / EV
**Score column**: `cyc4_sales_ev` | **Direction**: Higher is better | **Factor OBQ**: 0.663

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.2% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +13.0% |
| ICIR | 1.37 |
| IC Hit Rate | 90% |
| Monotonicity | 100% |
| Alpha Win Rate | 80% |
| Staircase | 0.08 |
| Q1 Sharpe | 0.59 |
| Q1 MaxDD | -20.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +5.5% | -31.8% | +1.3% | 0.938 | 0.11 | 0.446 |
| Mid_Cap | +4.9% | -47.1% | +1.2% | 0.723 | 0.05 | 0.479 |
| Mega_Cap | +2.0% | -22.8% | -2.6% | 0.729 | -0.21 | 0.294 |
| All_Cap | +1.2% | -46.5% | -3.5% | 0.429 | -0.24 | 0.182 |
| Large_Cap | +1.0% | -26.3% | -3.2% | 0.580 | -0.27 | 0.187 |

**Signal Translation**: Best portfolio = **Small_Cap** at +5.5% CAGR | Factor Q1 = +9.2% | Gap = **-3.6%**

---
### OBQ Value (Universe)
**Score column**: `value_score_universe` | **Direction**: Higher is better | **Factor OBQ**: 0.658

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.5% |
| Q5 CAGR | -1.3% |
| Q1-Q5 Spread | +13.3% |
| ICIR | 1.18 |
| IC Hit Rate | 90% |
| Monotonicity | 100% |
| Alpha Win Rate | 77% |
| Staircase | 0.08 |
| Q1 Sharpe | 0.70 |
| Q1 MaxDD | -23.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +7.7% | -50.9% | +4.3% | 0.900 | 0.18 | 0.571 |
| Large_Cap | +5.2% | -49.3% | +1.4% | 0.662 | 0.05 | 0.490 |
| Mid_Cap | +5.1% | -61.0% | +1.6% | 0.843 | 0.05 | 0.587 |
| Mega_Cap | +3.4% | -49.1% | -0.1% | 0.499 | -0.03 | 0.473 |
| All_Cap | +2.2% | -57.5% | -1.3% | 0.213 | -0.08 | 0.425 |

**Signal Translation**: Best portfolio = **Small_Cap** at +7.7% CAGR | Factor Q1 = +10.5% | Gap = **-2.8%**

---
### EBIT / EV
**Score column**: `cyc4_ebit_ev` | **Direction**: Higher is better | **Factor OBQ**: 0.652

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.7% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +10.5% |
| ICIR | 1.34 |
| IC Hit Rate | 90% |
| Monotonicity | 100% |
| Alpha Win Rate | 80% |
| Staircase | 0.08 |
| Q1 Sharpe | 0.65 |
| Q1 MaxDD | -22.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +6.7% | -27.7% | +2.2% | 0.964 | 0.17 | 0.511 |
| Small_Cap | +3.9% | -39.1% | -0.5% | 0.867 | -0.01 | 0.420 |
| Mega_Cap | +2.7% | -36.5% | -2.0% | 0.697 | -0.11 | 0.275 |
| Large_Cap | +1.5% | -42.6% | -3.2% | 0.508 | -0.19 | 0.303 |
| Mid_Cap | +1.2% | -38.0% | -3.2% | 0.342 | -0.28 | 0.160 |

**Signal Translation**: Best portfolio = **All_Cap** at +6.7% CAGR | Factor Q1 = +8.7% | Gap = **-2.1%**

---
### OBQ Value
**Score column**: `value_score` | **Direction**: Higher is better | **Factor OBQ**: 0.626

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.7% |
| Q5 CAGR | +0.3% |
| Q1-Q5 Spread | +11.9% |
| ICIR | 1.20 |
| IC Hit Rate | 90% |
| Monotonicity | 100% |
| Alpha Win Rate | 77% |
| Staircase | 0.07 |
| Q1 Sharpe | 0.70 |
| Q1 MaxDD | -23.9% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +10.6% | -41.8% | +7.6% | 0.897 | 0.29 | 0.586 |
| Mid_Cap | +4.9% | -54.2% | +1.2% | 0.873 | 0.04 | 0.547 |
| Large_Cap | +4.7% | -56.6% | +0.6% | 0.602 | 0.03 | 0.499 |
| Mega_Cap | +4.3% | -57.4% | +0.2% | 0.673 | 0.02 | 0.528 |
| All_Cap | +3.5% | -56.6% | +0.0% | 0.501 | -0.02 | 0.457 |

**Signal Translation**: Best portfolio = **Small_Cap** at +10.6% CAGR | Factor Q1 = +10.7% | Gap = **-0.1%**

---
### Healthcare R&D Yield
**Score column**: `cyc5_healthcare_rnd_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.626

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.7% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +8.7% |
| ICIR | 1.68 |
| IC Hit Rate | 95% |
| Monotonicity | 100% |
| Alpha Win Rate | 60% |
| Staircase | 0.05 |
| Q1 Sharpe | 0.44 |
| Q1 MaxDD | -35.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +3.0% | -29.4% | -1.4% | 0.874 | -0.08 | 0.312 |
| Large_Cap | +2.3% | -44.6% | -1.9% | 0.734 | -0.12 | 0.285 |
| All_Cap | +2.1% | -26.1% | -2.1% | 0.770 | -0.18 | 0.213 |
| Mid_Cap | +1.5% | -34.9% | -3.0% | 0.722 | -0.19 | 0.152 |
| Small_Cap | +0.2% | -42.5% | -4.1% | 0.479 | -0.34 | 0.054 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +3.0% CAGR | Factor Q1 = +6.7% | Gap = **-3.6%**

---
### FCF / EV
**Score column**: `cyc4_fcf_ev` | **Direction**: Higher is better | **Factor OBQ**: 0.620

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.5% |
| Q5 CAGR | +2.9% |
| Q1-Q5 Spread | +8.0% |
| ICIR | 1.83 |
| IC Hit Rate | 100% |
| Monotonicity | 75% |
| Alpha Win Rate | 83% |
| Staircase | 0.04 |
| Q1 Sharpe | 0.69 |
| Q1 MaxDD | -23.7% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +3.4% | -38.1% | -0.9% | 0.614 | -0.04 | 0.383 |
| Large_Cap | +2.8% | -28.4% | -1.4% | 0.879 | -0.10 | 0.350 |
| Mega_Cap | +2.5% | -33.5% | -2.1% | 0.792 | -0.12 | 0.375 |
| All_Cap | +2.4% | -31.5% | -1.9% | 0.865 | -0.16 | 0.371 |
| Mid_Cap | +1.6% | -37.0% | -2.6% | 0.466 | -0.20 | 0.203 |

**Signal Translation**: Best portfolio = **Small_Cap** at +3.4% CAGR | Factor Q1 = +9.5% | Gap = **-6.1%**

---
### Return on Equity
**Score column**: `cyc4_roe` | **Direction**: Higher is better | **Factor OBQ**: 0.608

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +13.1% |
| ICIR | 1.44 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 70% |
| Staircase | 0.06 |
| Q1 Sharpe | 0.64 |
| Q1 MaxDD | -34.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +2.7% | -44.6% | -0.9% | 0.781 | -0.07 | 0.418 |
| Mid_Cap | +2.6% | -65.1% | -1.2% | 0.737 | -0.07 | 0.515 |
| Large_Cap | +2.6% | -38.2% | -1.0% | 0.791 | -0.09 | 0.355 |
| All_Cap | +0.0% | -65.6% | -3.1% | 0.041 | -0.22 | 0.347 |
| Mega_Cap | -0.9% | -64.3% | -4.8% | 0.502 | -0.38 | 0.282 |

**Signal Translation**: Best portfolio = **Small_Cap** at +2.7% CAGR | Factor Q1 = +10.2% | Gap = **-7.5%**

---
### OBQ Momentum
**Score column**: `momentum_score` | **Direction**: Higher is better | **Factor OBQ**: 0.579

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.8% |
| Q5 CAGR | +3.9% |
| Q1-Q5 Spread | +9.0% |
| ICIR | 0.92 |
| IC Hit Rate | 80% |
| Monotonicity | 100% |
| Alpha Win Rate | 87% |
| Staircase | 0.06 |
| Q1 Sharpe | 0.69 |
| Q1 MaxDD | -34.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +7.6% | -49.3% | +4.0% | 0.909 | 0.15 | 0.567 |
| Large_Cap | +6.8% | -60.1% | +2.6% | 0.504 | 0.13 | 0.619 |
| Mid_Cap | +5.3% | -69.4% | +1.6% | 0.782 | 0.06 | 0.615 |
| All_Cap | +5.3% | -50.6% | +1.3% | 0.544 | 0.05 | 0.449 |
| Small_Cap | +4.5% | -47.7% | +0.1% | 0.574 | 0.02 | 0.472 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +7.6% CAGR | Factor Q1 = +10.8% | Gap = **-3.2%**

---
### Moat (Cost Advantage)
**Score column**: `cyc2_moat_cost` | **Direction**: Higher is better | **Factor OBQ**: 0.575

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.3% |
| Q5 CAGR | +6.3% |
| Q1-Q5 Spread | +7.2% |
| ICIR | 1.50 |
| IC Hit Rate | 93% |
| Monotonicity | 100% |
| Alpha Win Rate | 83% |
| Staircase | 0.04 |
| Q1 Sharpe | 0.85 |
| Q1 MaxDD | -25.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +6.1% | -53.5% | +2.2% | 0.813 | 0.09 | 0.574 |
| All_Cap | +5.8% | -54.1% | +1.5% | 0.786 | 0.09 | 0.547 |
| Large_Cap | +5.3% | -60.3% | +0.9% | 0.859 | 0.06 | 0.541 |
| Mega_Cap | +4.3% | -54.9% | +0.3% | 0.641 | 0.01 | 0.509 |
| Small_Cap | +3.6% | -58.4% | -0.7% | 0.684 | -0.02 | 0.488 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +6.1% CAGR | Factor Q1 = +11.3% | Gap = **-5.2%**

---
### Moat (Switching Costs)
**Score column**: `cyc2_moat_switching` | **Direction**: Higher is better | **Factor OBQ**: 0.569

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.5% |
| Q5 CAGR | +4.4% |
| Q1-Q5 Spread | +7.9% |
| ICIR | 1.41 |
| IC Hit Rate | 90% |
| Monotonicity | 75% |
| Alpha Win Rate | 77% |
| Staircase | 0.03 |
| Q1 Sharpe | 0.83 |
| Q1 MaxDD | -26.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +6.2% | -53.4% | +2.6% | 0.791 | 0.10 | 0.502 |
| Mid_Cap | +4.7% | -60.0% | +1.7% | 0.811 | 0.03 | 0.573 |
| All_Cap | +3.9% | -59.8% | -0.1% | 0.530 | -0.00 | 0.512 |
| Large_Cap | +2.6% | -52.0% | -1.1% | 0.432 | -0.07 | 0.362 |
| Mega_Cap | +1.8% | -57.4% | -2.1% | 0.143 | -0.11 | 0.393 |

**Signal Translation**: Best portfolio = **Small_Cap** at +6.2% CAGR | Factor Q1 = +10.5% | Gap = **-4.4%**

---
### Pre-Tax Margin
**Score column**: `cyc4_pretax_margin` | **Direction**: Higher is better | **Factor OBQ**: 0.525

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +15.6% |
| ICIR | 1.34 |
| IC Hit Rate | 87% |
| Monotonicity | 100% |
| Alpha Win Rate | 63% |
| Staircase | 0.06 |
| Q1 Sharpe | 0.72 |
| Q1 MaxDD | -30.7% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +4.4% | -45.1% | +0.7% | 0.510 | 0.02 | 0.473 |
| Mega_Cap | +2.4% | -53.8% | -1.7% | 0.089 | -0.10 | 0.324 |
| Mid_Cap | +2.2% | -43.5% | -1.7% | 0.602 | -0.12 | 0.358 |
| All_Cap | +1.1% | -52.0% | -2.2% | 0.078 | -0.18 | 0.303 |
| Large_Cap | +0.7% | -45.5% | -3.2% | 0.015 | -0.21 | 0.190 |

**Signal Translation**: Best portfolio = **Small_Cap** at +4.4% CAGR | Factor Q1 = +10.2% | Gap = **-5.8%**

---
### Operating Margin
**Score column**: `cyc2_op_margin` | **Direction**: Higher is better | **Factor OBQ**: 0.522

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.6% |
| Q5 CAGR | -2.1% |
| Q1-Q5 Spread | +14.4% |
| ICIR | 1.37 |
| IC Hit Rate | 87% |
| Monotonicity | 75% |
| Alpha Win Rate | 77% |
| Staircase | 0.05 |
| Q1 Sharpe | 0.75 |
| Q1 MaxDD | -25.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +3.2% | -67.4% | -0.7% | 0.773 | -0.04 | 0.543 |
| Large_Cap | +3.0% | -69.7% | -1.8% | 0.290 | -0.05 | 0.489 |
| Small_Cap | +2.7% | -55.5% | -1.1% | 0.483 | -0.07 | 0.446 |
| All_Cap | +2.2% | -68.3% | -2.1% | 0.161 | -0.09 | 0.441 |
| Mega_Cap | +1.4% | -58.1% | -3.2% | 0.000 | -0.13 | 0.324 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +3.2% CAGR | Factor Q1 = +9.6% | Gap = **-6.3%**

---
### Momentum 6M
**Score column**: `cyc2_mom_6m` | **Direction**: Higher is better | **Factor OBQ**: 0.515

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.1% |
| Q5 CAGR | -1.6% |
| Q1-Q5 Spread | +14.2% |
| ICIR | 1.04 |
| IC Hit Rate | 83% |
| Monotonicity | 75% |
| Alpha Win Rate | 67% |
| Staircase | 0.05 |
| Q1 Sharpe | 0.50 |
| Q1 MaxDD | -37.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +2.9% | -58.4% | -0.8% | 0.416 | -0.05 | 0.510 |
| Mid_Cap | +2.0% | -77.1% | -2.1% | 0.600 | -0.08 | 0.512 |
| Large_Cap | +1.0% | -62.2% | -3.4% | 0.019 | -0.13 | 0.325 |
| Mega_Cap | +0.8% | -73.2% | -2.9% | 0.162 | -0.13 | 0.323 |
| All_Cap | -1.6% | -76.4% | -5.3% | 0.484 | -0.20 | 0.195 |

**Signal Translation**: Best portfolio = **Small_Cap** at +2.9% CAGR | Factor Q1 = +7.1% | Gap = **-4.2%**

---
### Asset Turnover
**Score column**: `cyc4_asset_turnover` | **Direction**: Higher is better | **Factor OBQ**: 0.515

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +3.0% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.65 |
| IC Hit Rate | 73% |
| Monotonicity | 100% |
| Alpha Win Rate | 80% |
| Staircase | 0.06 |
| Q1 Sharpe | 0.63 |
| Q1 MaxDD | -36.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +5.6% | -63.0% | +1.4% | 0.922 | 0.08 | 0.613 |
| Mid_Cap | +5.5% | -36.9% | +2.5% | 0.735 | 0.08 | 0.494 |
| All_Cap | +4.3% | -48.2% | -0.8% | 0.581 | 0.02 | 0.416 |
| Large_Cap | +2.5% | -40.6% | -1.9% | 0.613 | -0.10 | 0.357 |
| Mega_Cap | +1.2% | -39.1% | -2.6% | 0.697 | -0.21 | 0.302 |

**Signal Translation**: Best portfolio = **Small_Cap** at +5.6% CAGR | Factor Q1 = +10.2% | Gap = **-4.6%**

---
### Alpha Factor (Universe)
**Score column**: `af_universe_score` | **Direction**: Higher is better | **Factor OBQ**: 0.444

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.6% |
| Q5 CAGR | -0.7% |
| Q1-Q5 Spread | +13.2% |
| ICIR | 0.86 |
| IC Hit Rate | 77% |
| Monotonicity | 75% |
| Alpha Win Rate | 63% |
| Staircase | 0.05 |
| Q1 Sharpe | 0.48 |
| Q1 MaxDD | -43.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +10.0% | -56.5% | +5.9% | 0.910 | 0.24 | 0.670 |
| Large_Cap | +5.0% | -55.2% | +0.5% | 0.735 | 0.04 | 0.536 |
| Mid_Cap | +0.3% | -74.3% | -4.3% | 0.244 | -0.16 | 0.378 |
| All_Cap | -1.1% | -80.4% | -5.0% | 0.463 | -0.21 | 0.269 |
| Mega_Cap | -1.4% | -83.7% | -4.9% | 0.566 | -0.22 | 0.306 |

**Signal Translation**: Best portfolio = **Small_Cap** at +10.0% CAGR | Factor Q1 = +7.6% | Gap = **+2.5%**

---
### Momentum (Alpha Factor)
**Score column**: `momentum_af_score` | **Direction**: Higher is better | **Factor OBQ**: 0.435

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.8% |
| Q5 CAGR | +0.3% |
| Q1-Q5 Spread | +11.9% |
| ICIR | 0.83 |
| IC Hit Rate | 78% |
| Monotonicity | 75% |
| Alpha Win Rate | 63% |
| Staircase | 0.05 |
| Q1 Sharpe | 0.51 |
| Q1 MaxDD | -39.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +10.3% | -42.7% | +6.1% | 0.962 | 0.29 | 0.598 |
| Large_Cap | +2.0% | -66.7% | -2.3% | 0.190 | -0.10 | 0.401 |
| Mid_Cap | +0.4% | -77.7% | -3.9% | 0.195 | -0.15 | 0.438 |
| Mega_Cap | -1.3% | -83.6% | -4.3% | 0.551 | -0.23 | 0.330 |
| All_Cap | -2.7% | -71.6% | -6.4% | 0.682 | -0.30 | 0.156 |

**Signal Translation**: Best portfolio = **Small_Cap** at +10.3% CAGR | Factor Q1 = +7.8% | Gap = **+2.5%**

---
### FCF / Debt
**Score column**: `cyc2_fcf_debt` | **Direction**: Higher is better | **Factor OBQ**: 0.420

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.1% |
| Q5 CAGR | -2.1% |
| Q1-Q5 Spread | +13.4% |
| ICIR | 1.81 |
| IC Hit Rate | 97% |
| Monotonicity | 75% |
| Alpha Win Rate | 57% |
| Staircase | 0.04 |
| Q1 Sharpe | 0.50 |
| Q1 MaxDD | -30.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +5.1% | -58.7% | +0.9% | 0.390 | 0.06 | 0.539 |
| All_Cap | +2.8% | -66.4% | -1.3% | 0.523 | -0.05 | 0.453 |
| Large_Cap | +2.6% | -62.7% | -1.5% | 0.525 | -0.07 | 0.494 |
| Mega_Cap | +1.9% | -59.4% | -2.4% | 0.173 | -0.10 | 0.433 |
| Mid_Cap | -1.8% | -81.6% | -6.6% | 0.000 | -0.25 | 0.298 |

**Signal Translation**: Best portfolio = **Small_Cap** at +5.1% CAGR | Factor Q1 = +7.1% | Gap = **-1.9%**

---
### Repurchase Yield
**Score column**: `cyc4_repurchase_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.414

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.1% |
| Q5 CAGR | +4.8% |
| Q1-Q5 Spread | +7.5% |
| ICIR | 0.84 |
| IC Hit Rate | 79% |
| Monotonicity | 75% |
| Alpha Win Rate | 77% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.62 |
| Q1 MaxDD | 0.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +3.2% | -35.3% | -1.7% | 0.830 | -0.06 | 0.379 |
| Mega_Cap | +2.3% | -34.3% | -1.8% | 0.499 | -0.13 | 0.249 |
| Mid_Cap | +1.3% | -33.2% | -3.1% | 0.654 | -0.26 | 0.202 |
| All_Cap | +0.5% | -47.2% | -3.7% | 0.022 | -0.34 | 0.088 |
| Small_Cap | +0.1% | -51.9% | -4.4% | 0.014 | -0.31 | 0.176 |

**Signal Translation**: Best portfolio = **Large_Cap** at +3.2% CAGR | Factor Q1 = +9.1% | Gap = **-6.0%**

---
### FCF CAGR 5Y
**Score column**: `cyc2_fcf_cagr_5y` | **Direction**: Higher is better | **Factor OBQ**: 0.410

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.6% |
| Q5 CAGR | +5.7% |
| Q1-Q5 Spread | +6.6% |
| ICIR | 1.29 |
| IC Hit Rate | 87% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.03 |
| Q1 Sharpe | 0.73 |
| Q1 MaxDD | -25.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +14.2% | -41.4% | +11.6% | 0.932 | 0.35 | 0.640 |
| Mid_Cap | +4.5% | -48.2% | +0.5% | 0.870 | 0.02 | 0.478 |
| All_Cap | +4.1% | -52.9% | +0.4% | 0.895 | 0.01 | 0.475 |
| Large_Cap | +4.0% | -55.1% | +0.0% | 0.589 | -0.00 | 0.507 |
| Mega_Cap | +3.2% | -60.1% | -0.3% | 0.443 | -0.04 | 0.472 |

**Signal Translation**: Best portfolio = **Small_Cap** at +14.2% CAGR | Factor Q1 = +10.6% | Gap = **+3.6%**

---
### Comms Content ROI
**Score column**: `cyc5_comms_content_roi` | **Direction**: Higher is better | **Factor OBQ**: 0.399

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.4% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.80 |
| IC Hit Rate | 80% |
| Monotonicity | 50% |
| Alpha Win Rate | 70% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.55 |
| Q1 MaxDD | -38.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +5.6% | -35.3% | +2.2% | 0.925 | 0.09 | 0.510 |
| Small_Cap | +2.2% | -48.9% | -0.7% | 0.434 | -0.09 | 0.384 |
| Mega_Cap | +1.5% | -53.0% | -2.1% | 0.057 | -0.15 | 0.268 |
| All_Cap | +1.3% | -37.2% | -2.8% | 0.624 | -0.20 | 0.266 |
| Large_Cap | +0.7% | -47.5% | -3.1% | 0.304 | -0.23 | 0.158 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +5.6% CAGR | Factor Q1 = +8.4% | Gap = **-2.8%**

---
### Momentum 3M
**Score column**: `cyc2_mom_3m` | **Direction**: Higher is better | **Factor OBQ**: 0.384

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.8% |
| Q5 CAGR | -0.2% |
| Q1-Q5 Spread | +11.3% |
| ICIR | 0.83 |
| IC Hit Rate | 80% |
| Monotonicity | 75% |
| Alpha Win Rate | 63% |
| Staircase | 0.03 |
| Q1 Sharpe | 0.51 |
| Q1 MaxDD | -38.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +9.4% | -50.4% | +4.8% | 0.882 | 0.28 | 0.612 |
| Mid_Cap | +4.5% | -58.8% | +0.4% | 0.640 | 0.02 | 0.542 |
| Large_Cap | +3.7% | -67.6% | -0.4% | 0.582 | -0.01 | 0.563 |
| All_Cap | +0.3% | -66.9% | -4.2% | 0.003 | -0.15 | 0.284 |
| Mega_Cap | -0.3% | -71.2% | -4.5% | 0.258 | -0.17 | 0.282 |

**Signal Translation**: Best portfolio = **Small_Cap** at +9.4% CAGR | Factor Q1 = +6.8% | Gap = **+2.7%**

---
### FCF CAGR 3Y
**Score column**: `cyc2_fcf_cagr_3y` | **Direction**: Higher is better | **Factor OBQ**: 0.384

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.1% |
| Q5 CAGR | +6.2% |
| Q1-Q5 Spread | +6.4% |
| ICIR | 0.93 |
| IC Hit Rate | 75% |
| Monotonicity | 75% |
| Alpha Win Rate | 57% |
| Staircase | 0.03 |
| Q1 Sharpe | 0.76 |
| Q1 MaxDD | -28.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +7.8% | -36.1% | +3.5% | 0.914 | 0.21 | 0.503 |
| All_Cap | +7.0% | -66.8% | +2.2% | 0.820 | 0.12 | 0.603 |
| Large_Cap | +5.7% | -59.7% | +2.0% | 0.853 | 0.08 | 0.582 |
| Mid_Cap | +3.5% | -49.3% | -0.6% | 0.681 | -0.02 | 0.436 |
| Mega_Cap | +3.0% | -63.2% | -1.4% | 0.619 | -0.04 | 0.489 |

**Signal Translation**: Best portfolio = **Small_Cap** at +7.8% CAGR | Factor Q1 = +11.1% | Gap = **-3.3%**

---
### Momentum 12M
**Score column**: `cyc2_mom_12m` | **Direction**: Higher is better | **Factor OBQ**: 0.380

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.9% |
| Q5 CAGR | -1.1% |
| Q1-Q5 Spread | +12.7% |
| ICIR | 0.89 |
| IC Hit Rate | 73% |
| Monotonicity | 75% |
| Alpha Win Rate | 63% |
| Staircase | 0.04 |
| Q1 Sharpe | 0.41 |
| Q1 MaxDD | -45.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +3.2% | -61.2% | -1.7% | 0.613 | -0.04 | 0.532 |
| Mega_Cap | +3.0% | -61.6% | -0.2% | 0.371 | -0.04 | 0.481 |
| Large_Cap | +2.7% | -60.4% | -1.6% | 0.215 | -0.06 | 0.478 |
| Mid_Cap | -0.6% | -71.9% | -5.5% | 0.028 | -0.19 | 0.348 |
| All_Cap | -2.5% | -77.9% | -6.7% | 0.601 | -0.29 | 0.214 |

**Signal Translation**: Best portfolio = **Small_Cap** at +3.2% CAGR | Factor Q1 = +5.9% | Gap = **-2.7%**

---
### FCF Yield
**Score column**: `cyc2_fcf_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.364

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.5% |
| Q5 CAGR | -5.3% |
| Q1-Q5 Spread | +15.4% |
| ICIR | 1.89 |
| IC Hit Rate | 90% |
| Monotonicity | 50% |
| Alpha Win Rate | 53% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.48 |
| Q1 MaxDD | -34.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +5.8% | -61.4% | +1.2% | 0.675 | 0.08 | 0.610 |
| Large_Cap | +3.3% | -52.6% | -0.6% | 0.386 | -0.03 | 0.457 |
| All_Cap | +2.4% | -58.3% | -1.3% | 0.089 | -0.07 | 0.385 |
| Mega_Cap | +2.3% | -57.7% | -2.1% | 0.008 | -0.08 | 0.422 |
| Mid_Cap | -1.3% | -71.2% | -5.6% | 0.007 | -0.25 | 0.246 |

**Signal Translation**: Best portfolio = **Small_Cap** at +5.8% CAGR | Factor Q1 = +6.5% | Gap = **-0.7%**

---
### OBQ FinStr
**Score column**: `finstr_score` | **Direction**: Higher is better | **Factor OBQ**: 0.360

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.6% |
| Q5 CAGR | +5.4% |
| Q1-Q5 Spread | +6.2% |
| ICIR | 0.34 |
| IC Hit Rate | 57% |
| Monotonicity | 100% |
| Alpha Win Rate | 77% |
| Staircase | 0.04 |
| Q1 Sharpe | 0.60 |
| Q1 MaxDD | -31.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +12.6% | -46.7% | +9.5% | 0.939 | 0.34 | 0.659 |
| Large_Cap | +6.7% | -51.4% | +3.2% | 0.692 | 0.11 | 0.559 |
| All_Cap | +4.4% | -59.3% | +0.1% | 0.354 | 0.02 | 0.501 |
| Mid_Cap | +3.8% | -64.3% | -0.1% | 0.525 | -0.01 | 0.535 |
| Mega_Cap | +2.8% | -60.8% | -1.5% | 0.027 | -0.06 | 0.422 |

**Signal Translation**: Best portfolio = **Small_Cap** at +12.6% CAGR | Factor Q1 = +8.6% | Gap = **+4.0%**

---
### Net Payout Yield
**Score column**: `cyc4_net_payout_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.357

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.0% |
| Q5 CAGR | +5.6% |
| Q1-Q5 Spread | +6.7% |
| ICIR | 0.76 |
| IC Hit Rate | 76% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.58 |
| Q1 MaxDD | 0.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +4.0% | -30.4% | -0.6% | 0.887 | 0.00 | 0.431 |
| Mid_Cap | +2.3% | -37.4% | -2.1% | 0.795 | -0.14 | 0.347 |
| Large_Cap | +1.6% | -30.4% | -3.5% | 0.780 | -0.20 | 0.305 |
| Small_Cap | +1.4% | -25.0% | -3.0% | 0.843 | -0.25 | 0.246 |
| All_Cap | +0.9% | -30.4% | -3.6% | 0.495 | -0.33 | 0.135 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +4.0% CAGR | Factor Q1 = +9.0% | Gap = **-5.0%**

---
### Rulebreaker Rank
**Score column**: `rulebreaker_rank` | **Direction**: Lower is better | **Factor OBQ**: 0.357

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.9% |
| Q5 CAGR | +3.0% |
| Q1-Q5 Spread | +10.5% |
| ICIR | -0.50 |
| IC Hit Rate | 36% |
| Monotonicity | 100% |
| Alpha Win Rate | 77% |
| Staircase | 0.09 |
| Q1 Sharpe | 0.69 |
| Q1 MaxDD | -31.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.7% | -56.7% | +4.9% | 0.939 | 0.19 | 0.649 |
| Mega_Cap | +6.2% | -56.4% | +1.9% | 0.897 | 0.09 | 0.569 |
| Large_Cap | +6.2% | -64.9% | +1.7% | 0.860 | 0.09 | 0.582 |
| Mid_Cap | +5.0% | -58.7% | +1.2% | 0.711 | 0.04 | 0.550 |
| All_Cap | -2.6% | -84.9% | -7.8% | 0.475 | -0.26 | 0.233 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.7% CAGR | Factor Q1 = +9.9% | Gap = **-1.2%**

---
### Systematic Score
**Score column**: `cyc2_sys_score` | **Direction**: Higher is better | **Factor OBQ**: 0.356

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.7% |
| Q5 CAGR | +2.1% |
| Q1-Q5 Spread | +8.3% |
| ICIR | 0.91 |
| IC Hit Rate | 75% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.03 |
| Q1 Sharpe | 0.64 |
| Q1 MaxDD | -35.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +6.7% | -64.8% | +3.4% | 0.837 | 0.11 | 0.620 |
| Small_Cap | +4.8% | -62.0% | +1.2% | 0.752 | 0.04 | 0.535 |
| All_Cap | +3.5% | -58.3% | +0.8% | 0.436 | -0.03 | 0.490 |
| Mega_Cap | +3.4% | -60.7% | -0.1% | 0.500 | -0.03 | 0.533 |
| Large_Cap | +3.0% | -59.5% | -0.6% | 0.183 | -0.04 | 0.419 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +6.7% CAGR | Factor Q1 = +8.7% | Gap = **-2.0%**

---
### Earnings Quality
**Score column**: `cyc2_earn_quality` | **Direction**: Higher is better | **Factor OBQ**: 0.356

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.3% |
| Q5 CAGR | +6.5% |
| Q1-Q5 Spread | +4.7% |
| ICIR | 0.79 |
| IC Hit Rate | 77% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.72 |
| Q1 MaxDD | -23.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +6.8% | -63.1% | +2.0% | 0.836 | 0.13 | 0.630 |
| Small_Cap | +6.1% | -55.8% | +2.7% | 0.909 | 0.11 | 0.467 |
| Mid_Cap | +5.3% | -48.2% | +0.9% | 0.814 | 0.06 | 0.512 |
| Large_Cap | +5.3% | -53.5% | +2.0% | 0.783 | 0.06 | 0.546 |
| All_Cap | +2.8% | -58.8% | -0.7% | 0.554 | -0.05 | 0.433 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +6.8% CAGR | Factor Q1 = +10.3% | Gap = **-3.5%**

---
### Shareholder Yield
**Score column**: `cyc4_shareholder_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.352

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.9% |
| Q5 CAGR | +5.4% |
| Q1-Q5 Spread | +6.7% |
| ICIR | 0.77 |
| IC Hit Rate | 76% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.60 |
| Q1 MaxDD | 0.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +4.4% | -42.0% | -0.2% | 0.901 | 0.03 | 0.437 |
| Large_Cap | +2.2% | -30.8% | -2.8% | 0.799 | -0.14 | 0.288 |
| Small_Cap | +2.2% | -25.7% | -2.2% | 0.852 | -0.18 | 0.287 |
| All_Cap | +1.5% | -29.7% | -2.7% | 0.584 | -0.27 | 0.172 |
| Mid_Cap | +1.5% | -32.1% | -3.0% | 0.578 | -0.22 | 0.204 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +4.4% CAGR | Factor Q1 = +8.9% | Gap = **-4.5%**

---
### Interest Coverage
**Score column**: `cyc2_int_cov` | **Direction**: Higher is better | **Factor OBQ**: 0.350

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.5% |
| Q5 CAGR | -1.1% |
| Q1-Q5 Spread | +11.5% |
| ICIR | 1.26 |
| IC Hit Rate | 80% |
| Monotonicity | 50% |
| Alpha Win Rate | 60% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.49 |
| Q1 MaxDD | -32.7% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +5.7% | -63.8% | +2.6% | 0.820 | 0.07 | 0.614 |
| Mid_Cap | +4.4% | -54.0% | +0.6% | 0.786 | 0.02 | 0.531 |
| Small_Cap | +3.7% | -58.9% | -0.3% | 0.267 | -0.02 | 0.519 |
| Large_Cap | +2.4% | -62.5% | -1.4% | 0.513 | -0.08 | 0.497 |
| Mega_Cap | +0.4% | -70.0% | -4.5% | 0.118 | -0.16 | 0.366 |

**Signal Translation**: Best portfolio = **All_Cap** at +5.7% CAGR | Factor Q1 = +6.5% | Gap = **-0.8%**

---
### Momentum (Systematic)
**Score column**: `momentum_sys_score` | **Direction**: Higher is better | **Factor OBQ**: 0.333

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.7% |
| Q5 CAGR | +3.7% |
| Q1-Q5 Spread | +6.5% |
| ICIR | 0.72 |
| IC Hit Rate | 73% |
| Monotonicity | 75% |
| Alpha Win Rate | 63% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.66 |
| Q1 MaxDD | -34.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +7.1% | -64.9% | +3.3% | 0.888 | 0.14 | 0.635 |
| All_Cap | +5.3% | -51.7% | +2.2% | 0.836 | 0.06 | 0.513 |
| Small_Cap | +4.1% | -57.3% | +0.0% | 0.815 | 0.00 | 0.532 |
| Mega_Cap | +1.6% | -65.1% | -1.9% | 0.407 | -0.12 | 0.449 |
| Large_Cap | +1.0% | -60.8% | -3.0% | 0.013 | -0.16 | 0.378 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +7.1% CAGR | Factor Q1 = +8.7% | Gap = **-1.6%**

---
### OBQ FinStr (Universe)
**Score column**: `finstr_score_universe` | **Direction**: Higher is better | **Factor OBQ**: 0.326

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.0% |
| Q5 CAGR | +6.0% |
| Q1-Q5 Spread | +5.1% |
| ICIR | 0.11 |
| IC Hit Rate | 56% |
| Monotonicity | 100% |
| Alpha Win Rate | 63% |
| Staircase | 0.04 |
| Q1 Sharpe | 0.53 |
| Q1 MaxDD | -38.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.8% | -66.3% | +4.9% | 0.727 | 0.21 | 0.705 |
| Large_Cap | +3.7% | -64.2% | -0.0% | 0.681 | -0.02 | 0.555 |
| All_Cap | +3.4% | -61.8% | -1.0% | 0.293 | -0.03 | 0.508 |
| Mega_Cap | +2.9% | -47.7% | -0.7% | 0.540 | -0.05 | 0.458 |
| Mid_Cap | +2.5% | -66.8% | -1.6% | 0.414 | -0.07 | 0.474 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.8% CAGR | Factor Q1 = +8.0% | Gap = **+0.8%**

---
### Gross Margin
**Score column**: `cyc2_gross_margin` | **Direction**: Higher is better | **Factor OBQ**: 0.324

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.1% |
| Q5 CAGR | +4.9% |
| Q1-Q5 Spread | +5.6% |
| ICIR | 0.53 |
| IC Hit Rate | 73% |
| Monotonicity | 100% |
| Alpha Win Rate | 63% |
| Staircase | 0.03 |
| Q1 Sharpe | 0.58 |
| Q1 MaxDD | -27.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +6.3% | -49.7% | +3.6% | 0.839 | 0.11 | 0.547 |
| Mid_Cap | +5.3% | -57.5% | +2.0% | 0.833 | 0.06 | 0.558 |
| All_Cap | +4.8% | -55.9% | +1.2% | 0.468 | 0.04 | 0.493 |
| Small_Cap | +4.7% | -54.3% | +0.2% | 0.733 | 0.03 | 0.521 |
| Mega_Cap | +2.8% | -56.2% | -1.0% | 0.454 | -0.05 | 0.461 |

**Signal Translation**: Best portfolio = **Large_Cap** at +6.3% CAGR | Factor Q1 = +8.1% | Gap = **-1.7%**

---
### Cash Conversion
**Score column**: `cyc2_cash_conv` | **Direction**: Higher is better | **Factor OBQ**: 0.322

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.7% |
| Q5 CAGR | +7.4% |
| Q1-Q5 Spread | +4.3% |
| ICIR | 0.97 |
| IC Hit Rate | 77% |
| Monotonicity | 75% |
| Alpha Win Rate | 63% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.77 |
| Q1 MaxDD | -24.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +5.4% | -57.1% | +2.0% | 0.763 | 0.06 | 0.554 |
| Small_Cap | +4.2% | -48.8% | +0.3% | 0.787 | 0.01 | 0.435 |
| Mega_Cap | +3.5% | -58.1% | -0.3% | 0.634 | -0.03 | 0.516 |
| Mid_Cap | +2.7% | -51.1% | -1.2% | 0.584 | -0.06 | 0.438 |
| Large_Cap | +2.1% | -59.4% | -1.7% | 0.143 | -0.09 | 0.373 |

**Signal Translation**: Best portfolio = **All_Cap** at +5.4% CAGR | Factor Q1 = +10.7% | Gap = **-5.3%**

---
### Revenue CAGR 5Y
**Score column**: `cyc2_rev_cagr_5y` | **Direction**: Higher is better | **Factor OBQ**: 0.322

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.6% |
| Q5 CAGR | +3.8% |
| Q1-Q5 Spread | +5.1% |
| ICIR | 0.69 |
| IC Hit Rate | 77% |
| Monotonicity | 75% |
| Alpha Win Rate | 53% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.49 |
| Q1 MaxDD | -34.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +5.1% | -69.0% | +0.9% | 0.528 | 0.04 | 0.583 |
| Mid_Cap | +2.8% | -56.0% | -1.0% | 0.531 | -0.05 | 0.496 |
| All_Cap | +2.2% | -63.6% | -1.0% | 0.558 | -0.08 | 0.461 |
| Mega_Cap | +0.3% | -60.2% | -3.6% | 0.000 | -0.17 | 0.310 |
| Small_Cap | +0.2% | -63.6% | -3.9% | 0.119 | -0.19 | 0.270 |

**Signal Translation**: Best portfolio = **Large_Cap** at +5.1% CAGR | Factor Q1 = +7.6% | Gap = **-2.5%**

---
### EPS CAGR 5Y
**Score column**: `cyc2_eps_cagr_5y` | **Direction**: Higher is better | **Factor OBQ**: 0.317

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.9% |
| Q5 CAGR | +6.3% |
| Q1-Q5 Spread | +4.7% |
| ICIR | 0.56 |
| IC Hit Rate | 70% |
| Monotonicity | 75% |
| Alpha Win Rate | 63% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.67 |
| Q1 MaxDD | -32.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +7.8% | -49.0% | +4.6% | 0.902 | 0.17 | 0.581 |
| Mega_Cap | +6.4% | -52.8% | +2.3% | 0.898 | 0.11 | 0.575 |
| All_Cap | +5.1% | -57.9% | +0.6% | 0.858 | 0.05 | 0.518 |
| Mid_Cap | +4.8% | -48.7% | +1.8% | 0.849 | 0.03 | 0.497 |
| Large_Cap | +3.0% | -65.0% | -1.0% | 0.433 | -0.05 | 0.465 |

**Signal Translation**: Best portfolio = **Small_Cap** at +7.8% CAGR | Factor Q1 = +9.9% | Gap = **-2.1%**

---
### Net Debt / EBIT
**Score column**: `cyc2_nd_ebit` | **Direction**: Lower is better | **Factor OBQ**: 0.296

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.9% |
| Q5 CAGR | +8.5% |
| Q1-Q5 Spread | +0.7% |
| ICIR | 0.40 |
| IC Hit Rate | 60% |
| Monotonicity | 50% |
| Alpha Win Rate | 73% |
| Staircase | 0.00 |
| Q1 Sharpe | 0.53 |
| Q1 MaxDD | -39.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.7% | -56.6% | +4.9% | 0.940 | 0.19 | 0.648 |
| Mega_Cap | +6.2% | -56.4% | +1.9% | 0.896 | 0.09 | 0.577 |
| Large_Cap | +6.2% | -64.9% | +1.7% | 0.860 | 0.09 | 0.582 |
| Mid_Cap | +5.0% | -58.7% | +1.2% | 0.711 | 0.04 | 0.544 |
| All_Cap | -2.4% | -84.7% | -6.8% | 0.242 | -0.25 | 0.218 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.7% CAGR | Factor Q1 = +7.9% | Gap = **+0.8%**

---
### Skip-Month Momentum
**Score column**: `cyc4_skip_month_mom` | **Direction**: Higher is better | **Factor OBQ**: 0.294

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +0.1% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.88 |
| IC Hit Rate | 70% |
| Monotonicity | 75% |
| Alpha Win Rate | 57% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.44 |
| Q1 MaxDD | -38.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +4.3% | -48.4% | +0.0% | 0.867 | 0.01 | 0.486 |
| All_Cap | +4.0% | -54.6% | -0.2% | 0.802 | -0.00 | 0.514 |
| Mega_Cap | +2.5% | -55.4% | -1.3% | 0.293 | -0.08 | 0.459 |
| Small_Cap | +2.3% | -54.0% | -1.9% | 0.380 | -0.10 | 0.408 |
| Large_Cap | +1.4% | -49.1% | -2.8% | 0.184 | -0.14 | 0.273 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +4.3% CAGR | Factor Q1 = +10.2% | Gap = **-5.9%**

---
### Price/Sales
**Score column**: `cyc2_ps` | **Direction**: Lower is better | **Factor OBQ**: 0.290

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.5% |
| Q5 CAGR | -3.5% |
| Q1-Q5 Spread | +16.1% |
| ICIR | -0.94 |
| IC Hit Rate | 23% |
| Monotonicity | 100% |
| Alpha Win Rate | 83% |
| Staircase | 0.09 |
| Q1 Sharpe | 0.70 |
| Q1 MaxDD | -22.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.5% | -57.2% | +4.6% | 0.938 | 0.19 | 0.652 |
| Mid_Cap | +6.8% | -58.6% | +2.9% | 0.879 | 0.11 | 0.581 |
| Large_Cap | +6.4% | -65.0% | +2.1% | 0.870 | 0.09 | 0.598 |
| Mega_Cap | +4.4% | -56.4% | +0.4% | 0.766 | 0.02 | 0.543 |
| All_Cap | -2.2% | -83.8% | -7.1% | 0.121 | -0.24 | 0.215 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.5% CAGR | Factor Q1 = +10.5% | Gap = **-2.0%**

---
### Moat (Intangibles)
**Score column**: `cyc2_moat_intangible` | **Direction**: Higher is better | **Factor OBQ**: 0.288

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.2% |
| Q5 CAGR | +6.8% |
| Q1-Q5 Spread | +3.1% |
| ICIR | 0.34 |
| IC Hit Rate | 63% |
| Monotonicity | 100% |
| Alpha Win Rate | 67% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.59 |
| Q1 MaxDD | -30.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +4.9% | -61.1% | +1.5% | 0.697 | 0.04 | 0.566 |
| Mega_Cap | +4.9% | -57.7% | +1.4% | 0.393 | 0.04 | 0.551 |
| Mid_Cap | +3.9% | -79.0% | +0.0% | 0.757 | -0.01 | 0.598 |
| Small_Cap | +2.6% | -63.0% | -1.3% | 0.644 | -0.07 | 0.476 |
| All_Cap | +1.9% | -60.0% | -2.0% | 0.438 | -0.09 | 0.411 |

**Signal Translation**: Best portfolio = **Large_Cap** at +4.9% CAGR | Factor Q1 = +8.2% | Gap = **-3.3%**

---
### Intangibles / Book
**Score column**: `cyc4_intangibles_pb` | **Direction**: Higher is better | **Factor OBQ**: 0.278

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.8% |
| Q5 CAGR | +4.4% |
| Q1-Q5 Spread | +7.8% |
| ICIR | 0.31 |
| IC Hit Rate | 62% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.44 |
| Q1 MaxDD | -36.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +1.1% | -29.8% | -3.5% | 0.230 | -0.29 | 0.137 |
| Mega_Cap | +0.6% | -39.6% | -4.2% | 0.217 | -0.30 | 0.092 |
| All_Cap | +0.2% | -41.5% | -4.6% | 0.006 | -0.34 | 0.014 |
| Small_Cap | +0.2% | -53.5% | -4.1% | 0.180 | -0.30 | 0.237 |
| Large_Cap | -0.7% | -42.1% | -4.7% | 0.066 | -0.47 | 0.006 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +1.1% CAGR | Factor Q1 = +7.8% | Gap = **-6.7%**

---
### EPS CAGR 3Y
**Score column**: `cyc2_eps_cagr_3y` | **Direction**: Higher is better | **Factor OBQ**: 0.253

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.9% |
| Q5 CAGR | +6.7% |
| Q1-Q5 Spread | +3.1% |
| ICIR | 0.42 |
| IC Hit Rate | 67% |
| Monotonicity | 75% |
| Alpha Win Rate | 57% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.60 |
| Q1 MaxDD | -30.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +7.3% | -56.1% | +2.5% | 0.890 | 0.14 | 0.601 |
| Mega_Cap | +5.8% | -45.7% | +2.1% | 0.902 | 0.08 | 0.529 |
| Large_Cap | +5.5% | -58.4% | +2.4% | 0.648 | 0.07 | 0.573 |
| Mid_Cap | +5.2% | -52.9% | +1.2% | 0.863 | 0.05 | 0.537 |
| Small_Cap | +4.6% | -60.3% | +0.6% | 0.457 | 0.03 | 0.540 |

**Signal Translation**: Best portfolio = **All_Cap** at +7.3% CAGR | Factor Q1 = +8.9% | Gap = **-1.6%**

---
### Fundsmith Rank
**Score column**: `fundsmith_rank` | **Direction**: Lower is better | **Factor OBQ**: 0.251

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.7% |
| Q5 CAGR | -0.3% |
| Q1-Q5 Spread | +15.6% |
| ICIR | -1.07 |
| IC Hit Rate | 17% |
| Monotonicity | 75% |
| Alpha Win Rate | 93% |
| Staircase | 0.08 |
| Q1 Sharpe | 0.81 |
| Q1 MaxDD | -27.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.7% | -56.6% | +4.9% | 0.939 | 0.19 | 0.639 |
| Mega_Cap | +6.2% | -56.4% | +1.9% | 0.896 | 0.09 | 0.573 |
| Large_Cap | +6.2% | -64.7% | +1.7% | 0.860 | 0.08 | 0.578 |
| Mid_Cap | +5.0% | -58.7% | +1.2% | 0.711 | 0.04 | 0.544 |
| All_Cap | -2.7% | -85.6% | -7.2% | 0.491 | -0.26 | 0.244 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.7% CAGR | Factor Q1 = +10.7% | Gap = **-2.0%**

---
### Operating Leverage
**Score column**: `cyc4_op_leverage` | **Direction**: Higher is better | **Factor OBQ**: 0.236

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +2.5% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.87 |
| IC Hit Rate | 68% |
| Monotonicity | 75% |
| Alpha Win Rate | 53% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.38 |
| Q1 MaxDD | -28.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +3.1% | -49.3% | -0.5% | 0.784 | -0.05 | 0.393 |
| Mega_Cap | +2.5% | -44.6% | -1.3% | 0.758 | -0.09 | 0.362 |
| Small_Cap | +2.2% | -56.4% | -2.0% | 0.594 | -0.12 | 0.397 |
| All_Cap | +0.6% | -43.4% | -3.0% | 0.487 | -0.22 | 0.225 |
| Mid_Cap | -0.7% | -55.1% | -4.5% | 0.089 | -0.35 | 0.111 |

**Signal Translation**: Best portfolio = **Large_Cap** at +3.1% CAGR | Factor Q1 = +10.2% | Gap = **-7.1%**

---
### IT Rule of 40
**Score column**: `cyc5_it_rule_of_40` | **Direction**: Higher is better | **Factor OBQ**: 0.232

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.7% |
| Q5 CAGR | +0.7% |
| Q1-Q5 Spread | +5.1% |
| ICIR | 0.87 |
| IC Hit Rate | 76% |
| Monotonicity | 75% |
| Alpha Win Rate | 37% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.42 |
| Q1 MaxDD | -38.7% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | -0.1% | -41.7% | -4.0% | 0.118 | -0.34 | 0.109 |
| Small_Cap | -0.3% | -54.4% | -4.2% | 0.010 | -0.32 | 0.224 |
| All_Cap | -0.4% | -50.2% | -4.3% | 0.018 | -0.28 | 0.070 |
| Mid_Cap | -0.5% | -48.2% | -3.3% | 0.125 | -0.33 | 0.067 |
| Large_Cap | -1.1% | -64.4% | -5.1% | 0.315 | -0.39 | 0.182 |

**Signal Translation**: Best portfolio = **Mega_Cap** at -0.1% CAGR | Factor Q1 = +5.7% | Gap = **-5.8%**

---
### EPS CAGR 1Y
**Score column**: `cyc2_eps_cagr_1y` | **Direction**: Higher is better | **Factor OBQ**: 0.212

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.0% |
| Q5 CAGR | +6.9% |
| Q1-Q5 Spread | +3.7% |
| ICIR | 0.47 |
| IC Hit Rate | 69% |
| Monotonicity | 75% |
| Alpha Win Rate | 53% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.62 |
| Q1 MaxDD | -32.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +7.2% | -46.4% | +3.1% | 0.908 | 0.16 | 0.532 |
| All_Cap | +6.3% | -44.1% | +1.6% | 0.894 | 0.10 | 0.497 |
| Large_Cap | +4.9% | -59.3% | +1.3% | 0.811 | 0.04 | 0.576 |
| Mid_Cap | +4.1% | -52.5% | +0.5% | 0.721 | 0.00 | 0.513 |
| Mega_Cap | +2.9% | -62.0% | -1.1% | 0.644 | -0.05 | 0.499 |

**Signal Translation**: Best portfolio = **Small_Cap** at +7.2% CAGR | Factor Q1 = +9.0% | Gap = **-1.8%**

---
### Staples Dividend Growth
**Score column**: `cyc5_staples_div_growth` | **Direction**: Higher is better | **Factor OBQ**: 0.210

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.7% |
| Q5 CAGR | +8.4% |
| Q1-Q5 Spread | +1.1% |
| ICIR | 0.38 |
| IC Hit Rate | 61% |
| Monotonicity | 75% |
| Alpha Win Rate | 53% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.60 |
| Q1 MaxDD | -32.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +2.9% | -38.7% | -1.2% | 0.790 | -0.08 | 0.383 |
| Mid_Cap | +2.4% | -31.5% | -1.7% | 0.813 | -0.11 | 0.316 |
| Mega_Cap | +1.9% | -48.2% | -2.3% | 0.576 | -0.13 | 0.269 |
| Large_Cap | +1.5% | -56.0% | -2.4% | 0.298 | -0.15 | 0.350 |
| Small_Cap | +1.1% | -37.1% | -2.7% | 0.679 | -0.23 | 0.277 |

**Signal Translation**: Best portfolio = **All_Cap** at +2.9% CAGR | Factor Q1 = +8.7% | Gap = **-5.8%**

---
### Price/Book
**Score column**: `cyc2_pb` | **Direction**: Lower is better | **Factor OBQ**: 0.208

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +9.5% |
| Q5 CAGR | -1.8% |
| Q1-Q5 Spread | +12.9% |
| ICIR | -0.87 |
| IC Hit Rate | 28% |
| Monotonicity | 100% |
| Alpha Win Rate | 70% |
| Staircase | 0.07 |
| Q1 Sharpe | 0.66 |
| Q1 MaxDD | -23.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.1% | -56.9% | +4.3% | 0.933 | 0.17 | 0.647 |
| Mega_Cap | +6.1% | -59.0% | +2.4% | 0.814 | 0.08 | 0.588 |
| Large_Cap | +5.9% | -64.8% | +1.6% | 0.839 | 0.07 | 0.614 |
| Mid_Cap | +5.4% | -58.6% | +1.6% | 0.784 | 0.06 | 0.542 |
| All_Cap | -2.5% | -74.3% | -7.0% | 0.528 | -0.27 | 0.180 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.1% CAGR | Factor Q1 = +9.5% | Gap = **-1.5%**

---
### ROE Deviation
**Score column**: `cyc4_roe_dev` | **Direction**: Higher is better | **Factor OBQ**: 0.208

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.9% |
| Q5 CAGR | +1.1% |
| Q1-Q5 Spread | +10.9% |
| ICIR | 0.65 |
| IC Hit Rate | 79% |
| Monotonicity | 75% |
| Alpha Win Rate | 47% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.33 |
| Q1 MaxDD | -28.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +3.1% | -43.4% | -1.3% | 0.736 | -0.05 | 0.340 |
| Large_Cap | +2.8% | -42.4% | -1.3% | 0.793 | -0.06 | 0.344 |
| Mega_Cap | +2.6% | -38.1% | -1.3% | 0.785 | -0.09 | 0.360 |
| Mid_Cap | +2.1% | -51.7% | -2.1% | 0.669 | -0.09 | 0.378 |
| All_Cap | +1.1% | -50.2% | -3.4% | 0.476 | -0.17 | 0.260 |

**Signal Translation**: Best portfolio = **Small_Cap** at +3.1% CAGR | Factor Q1 = +10.9% | Gap = **-7.8%**

---
### Pre-Tax Margin Dev
**Score column**: `cyc4_pretax_margin_dev` | **Direction**: Higher is better | **Factor OBQ**: 0.207

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.9% |
| Q5 CAGR | +2.7% |
| Q1-Q5 Spread | +10.9% |
| ICIR | 0.45 |
| IC Hit Rate | 64% |
| Monotonicity | 75% |
| Alpha Win Rate | 43% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.31 |
| Q1 MaxDD | -28.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +2.6% | -58.3% | -1.6% | 0.810 | -0.07 | 0.451 |
| All_Cap | +1.4% | -73.5% | -2.6% | 0.634 | -0.15 | 0.454 |
| Large_Cap | +1.1% | -60.0% | -3.1% | 0.513 | -0.15 | 0.321 |
| Mid_Cap | +0.9% | -44.3% | -3.3% | 0.728 | -0.23 | 0.202 |
| Mega_Cap | -0.5% | -55.0% | -4.6% | 0.025 | -0.29 | 0.121 |

**Signal Translation**: Best portfolio = **Small_Cap** at +2.6% CAGR | Factor Q1 = +10.9% | Gap = **-8.3%**

---
### Gross Margin (CYC4)
**Score column**: `cyc4_gross_margin` | **Direction**: Higher is better | **Factor OBQ**: 0.206

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +5.5% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.47 |
| IC Hit Rate | 70% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.51 |
| Q1 MaxDD | -33.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +7.2% | -44.2% | +4.0% | 0.916 | 0.15 | 0.556 |
| Mid_Cap | +4.5% | -60.1% | +1.6% | 0.906 | 0.02 | 0.562 |
| Large_Cap | +2.5% | -39.7% | -1.0% | 0.682 | -0.10 | 0.338 |
| Small_Cap | +1.5% | -44.0% | -2.0% | 0.285 | -0.18 | 0.321 |
| Mega_Cap | -0.8% | -49.0% | -4.5% | 0.106 | -0.36 | 0.101 |

**Signal Translation**: Best portfolio = **All_Cap** at +7.2% CAGR | Factor Q1 = +10.2% | Gap = **-3.0%**

---
### Debt / Equity
**Score column**: `cyc4_debt_equity` | **Direction**: Lower is better | **Factor OBQ**: 0.204

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +9.7% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 1.28 |
| IC Hit Rate | 90% |
| Monotonicity | 50% |
| Alpha Win Rate | 37% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.19 |
| Q1 MaxDD | -38.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +8.8% | -53.0% | +4.7% | 0.932 | 0.20 | 0.598 |
| Mega_Cap | +7.4% | -45.7% | +4.0% | 0.914 | 0.15 | 0.584 |
| Mid_Cap | +6.1% | -51.0% | +1.9% | 0.834 | 0.09 | 0.521 |
| Small_Cap | +5.2% | -59.9% | +1.5% | 0.845 | 0.05 | 0.582 |
| All_Cap | +0.7% | -66.1% | -3.6% | 0.100 | -0.18 | 0.299 |

**Signal Translation**: Best portfolio = **Large_Cap** at +8.8% CAGR | Factor Q1 = +10.2% | Gap = **-1.4%**

---
### OBQ Growth
**Score column**: `growth_score` | **Direction**: Higher is better | **Factor OBQ**: 0.200

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.9% |
| Q5 CAGR | +5.4% |
| Q1-Q5 Spread | +3.2% |
| ICIR | 0.48 |
| IC Hit Rate | 68% |
| Monotonicity | 75% |
| Alpha Win Rate | 57% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.47 |
| Q1 MaxDD | -40.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +5.2% | -68.9% | +0.6% | 0.693 | 0.04 | 0.602 |
| Small_Cap | +4.0% | -65.5% | +0.4% | 0.465 | 0.00 | 0.522 |
| Large_Cap | +0.7% | -62.6% | -3.0% | 0.026 | -0.14 | 0.293 |
| Mid_Cap | +0.7% | -75.8% | -2.9% | 0.165 | -0.13 | 0.393 |
| Mega_Cap | -0.9% | -72.0% | -4.9% | 0.383 | -0.22 | 0.259 |

**Signal Translation**: Best portfolio = **All_Cap** at +5.2% CAGR | Factor Q1 = +6.9% | Gap = **-1.7%**

---
### OBQ Growth (Universe)
**Score column**: `growth_score_universe` | **Direction**: Higher is better | **Factor OBQ**: 0.182

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.8% |
| Q5 CAGR | +5.4% |
| Q1-Q5 Spread | +2.9% |
| ICIR | 0.36 |
| IC Hit Rate | 67% |
| Monotonicity | 50% |
| Alpha Win Rate | 60% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.47 |
| Q1 MaxDD | -41.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +2.8% | -65.9% | -0.7% | 0.326 | -0.05 | 0.470 |
| Large_Cap | +2.6% | -68.4% | -1.3% | 0.477 | -0.05 | 0.515 |
| Mega_Cap | +1.6% | -70.0% | -2.6% | 0.043 | -0.11 | 0.408 |
| Mid_Cap | +1.0% | -72.3% | -2.2% | 0.453 | -0.13 | 0.436 |
| All_Cap | +0.3% | -66.3% | -4.0% | 0.123 | -0.15 | 0.307 |

**Signal Translation**: Best portfolio = **Small_Cap** at +2.8% CAGR | Factor Q1 = +6.8% | Gap = **-4.0%**

---
### Accruals Ratio
**Score column**: `cyc4_accruals_ratio` | **Direction**: Lower is better | **Factor OBQ**: 0.182

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +8.3% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 1.38 |
| IC Hit Rate | 93% |
| Monotonicity | 50% |
| Alpha Win Rate | 50% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.00 |
| Q1 MaxDD | -38.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +8.8% | -53.0% | +4.7% | 0.932 | 0.20 | 0.598 |
| Mega_Cap | +7.5% | -45.7% | +4.0% | 0.915 | 0.16 | 0.582 |
| Mid_Cap | +6.1% | -51.0% | +1.9% | 0.834 | 0.09 | 0.521 |
| Small_Cap | +5.2% | -59.9% | +1.5% | 0.845 | 0.05 | 0.582 |
| All_Cap | +0.3% | -58.4% | -4.1% | 0.065 | -0.22 | 0.235 |

**Signal Translation**: Best portfolio = **Large_Cap** at +8.8% CAGR | Factor Q1 = +10.2% | Gap = **-1.4%**

---
### Price/FCF
**Score column**: `cyc2_pfcf` | **Direction**: Lower is better | **Factor OBQ**: 0.175

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +11.6% |
| Q5 CAGR | +8.4% |
| Q1-Q5 Spread | +4.8% |
| ICIR | -0.46 |
| IC Hit Rate | 43% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.77 |
| Q1 MaxDD | -21.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +6.9% | -56.3% | +2.4% | 0.917 | 0.14 | 0.560 |
| Large_Cap | +5.7% | -66.0% | +1.6% | 0.829 | 0.07 | 0.599 |
| Mid_Cap | +5.6% | -61.1% | +1.8% | 0.857 | 0.06 | 0.574 |
| Mega_Cap | +5.4% | -57.3% | +1.5% | 0.852 | 0.06 | 0.559 |
| All_Cap | +1.2% | -62.9% | -3.7% | 0.013 | -0.12 | 0.295 |

**Signal Translation**: Best portfolio = **Small_Cap** at +6.9% CAGR | Factor Q1 = +11.6% | Gap = **-4.7%**

---
### Revenue CAGR 3Y
**Score column**: `cyc2_rev_cagr_3y` | **Direction**: Higher is better | **Factor OBQ**: 0.169

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.2% |
| Q5 CAGR | +3.3% |
| Q1-Q5 Spread | +3.7% |
| ICIR | 0.57 |
| IC Hit Rate | 70% |
| Monotonicity | 75% |
| Alpha Win Rate | 47% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.45 |
| Q1 MaxDD | -39.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +6.3% | -58.7% | +3.0% | 0.646 | 0.12 | 0.607 |
| Large_Cap | +2.8% | -64.6% | -0.9% | 0.364 | -0.05 | 0.477 |
| All_Cap | +0.3% | -67.5% | -4.2% | 0.046 | -0.13 | 0.301 |
| Mid_Cap | +0.1% | -62.4% | -3.2% | 0.074 | -0.18 | 0.336 |
| Mega_Cap | -4.0% | -84.9% | -8.4% | 0.580 | -0.34 | 0.195 |

**Signal Translation**: Best portfolio = **Small_Cap** at +6.3% CAGR | Factor Q1 = +6.2% | Gap = **+0.1%**

---
### Market Beta
**Score column**: `cyc4_market_beta` | **Direction**: Lower is better | **Factor OBQ**: 0.159

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.8% |
| Q5 CAGR | +5.6% |
| Q1-Q5 Spread | +10.8% |
| ICIR | 0.32 |
| IC Hit Rate | 60% |
| Monotonicity | 75% |
| Alpha Win Rate | 57% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.35 |
| Q1 MaxDD | -34.9% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +8.3% | -51.7% | +4.1% | 0.923 | 0.18 | 0.583 |
| Mega_Cap | +6.8% | -44.4% | +3.3% | 0.912 | 0.13 | 0.558 |
| Mid_Cap | +6.4% | -50.6% | +2.1% | 0.853 | 0.10 | 0.522 |
| Small_Cap | +5.3% | -59.9% | +1.6% | 0.845 | 0.06 | 0.589 |
| All_Cap | -0.4% | -50.7% | -4.5% | 0.008 | -0.29 | 0.141 |

**Signal Translation**: Best portfolio = **Large_Cap** at +8.3% CAGR | Factor Q1 = +10.8% | Gap = **-2.5%**

---
### Revenue Growth 3Y
**Score column**: `cyc2_rev_growth_3y` | **Direction**: Higher is better | **Factor OBQ**: 0.153

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.6% |
| Q5 CAGR | +8.5% |
| Q1-Q5 Spread | -0.1% |
| ICIR | 0.08 |
| IC Hit Rate | 60% |
| Monotonicity | 50% |
| Alpha Win Rate | 57% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.58 |
| Q1 MaxDD | -26.7% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +8.1% | -58.2% | +4.1% | 0.900 | 0.17 | 0.619 |
| Large_Cap | +7.4% | -58.0% | +3.4% | 0.762 | 0.13 | 0.600 |
| Mid_Cap | +5.1% | -67.5% | +1.1% | 0.792 | 0.05 | 0.604 |
| Small_Cap | +4.0% | -59.4% | -0.7% | 0.519 | 0.00 | 0.532 |
| Mega_Cap | +3.2% | -58.1% | -1.0% | 0.320 | -0.04 | 0.494 |

**Signal Translation**: Best portfolio = **All_Cap** at +8.1% CAGR | Factor Q1 = +7.6% | Gap = **+0.5%**

---
### Idiosyncratic Volatility
**Score column**: `cyc4_idio_vol` | **Direction**: Lower is better | **Factor OBQ**: 0.149

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.8% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +15.3% |
| ICIR | 0.00 |
| IC Hit Rate | 30% |
| Monotonicity | 100% |
| Alpha Win Rate | 63% |
| Staircase | 0.06 |
| Q1 Sharpe | 0.86 |
| Q1 MaxDD | -27.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +8.3% | -51.7% | +4.1% | 0.923 | 0.18 | 0.583 |
| Mega_Cap | +6.8% | -44.4% | +3.3% | 0.912 | 0.13 | 0.558 |
| Mid_Cap | +6.4% | -50.6% | +2.1% | 0.853 | 0.10 | 0.522 |
| Small_Cap | +5.3% | -59.9% | +1.6% | 0.845 | 0.06 | 0.589 |
| All_Cap | -0.4% | -50.7% | -4.5% | 0.008 | -0.29 | 0.141 |

**Signal Translation**: Best portfolio = **Large_Cap** at +8.3% CAGR | Factor Q1 = +10.8% | Gap = **-2.5%**

---
### Revenue CAGR 1Y
**Score column**: `cyc2_rev_cagr_1y` | **Direction**: Higher is better | **Factor OBQ**: 0.148

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.0% |
| Q5 CAGR | +3.4% |
| Q1-Q5 Spread | +4.3% |
| ICIR | 0.56 |
| IC Hit Rate | 67% |
| Monotonicity | 75% |
| Alpha Win Rate | 43% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.36 |
| Q1 MaxDD | -38.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +1.5% | -63.8% | -2.0% | 0.474 | -0.10 | 0.417 |
| Small_Cap | +1.2% | -62.7% | -2.5% | 0.032 | -0.17 | 0.315 |
| Mega_Cap | -0.5% | -72.4% | -4.9% | 0.304 | -0.19 | 0.251 |
| Mid_Cap | -0.8% | -80.8% | -3.0% | 0.098 | -0.18 | 0.350 |
| All_Cap | -1.2% | -66.8% | -5.4% | 0.002 | -0.21 | 0.233 |

**Signal Translation**: Best portfolio = **Large_Cap** at +1.5% CAGR | Factor Q1 = +5.0% | Gap = **-3.5%**

---
### Realized Volatility
**Score column**: `cyc4_realized_vol` | **Direction**: Lower is better | **Factor OBQ**: 0.145

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +16.2% |
| ICIR | 0.00 |
| IC Hit Rate | 25% |
| Monotonicity | 100% |
| Alpha Win Rate | 67% |
| Staircase | 0.06 |
| Q1 Sharpe | 0.82 |
| Q1 MaxDD | -26.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +8.8% | -53.0% | +4.7% | 0.932 | 0.20 | 0.598 |
| Mega_Cap | +6.5% | -44.0% | +3.0% | 0.915 | 0.12 | 0.544 |
| Mid_Cap | +6.1% | -51.0% | +1.9% | 0.834 | 0.09 | 0.521 |
| Small_Cap | +5.2% | -59.9% | +1.5% | 0.845 | 0.05 | 0.582 |
| All_Cap | -0.1% | -56.8% | -4.7% | 0.033 | -0.26 | 0.184 |

**Signal Translation**: Best portfolio = **Large_Cap** at +8.8% CAGR | Factor Q1 = +10.2% | Gap = **-1.4%**

---
### Moat (Network Effect)
**Score column**: `cyc2_moat_network` | **Direction**: Higher is better | **Factor OBQ**: 0.134

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +4.5% |
| Q5 CAGR | +7.4% |
| Q1-Q5 Spread | -0.5% |
| ICIR | -0.22 |
| IC Hit Rate | 51% |
| Monotonicity | 50% |
| Alpha Win Rate | 50% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.32 |
| Q1 MaxDD | -40.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +4.7% | -63.1% | +1.2% | 0.694 | 0.03 | 0.584 |
| Mid_Cap | +3.9% | -61.1% | +0.3% | 0.738 | -0.00 | 0.532 |
| Mega_Cap | +1.0% | -60.8% | -3.9% | 0.017 | -0.12 | 0.345 |
| Large_Cap | +0.0% | -71.6% | -3.7% | 0.383 | -0.17 | 0.309 |
| All_Cap | -1.1% | -71.9% | -5.1% | 0.236 | -0.20 | 0.241 |

**Signal Translation**: Best portfolio = **Small_Cap** at +4.7% CAGR | Factor Q1 = +4.5% | Gap = **+0.2%**

---
### Materials Cash Conv Cycle
**Score column**: `cyc5_materials_ccc` | **Direction**: Higher is better | **Factor OBQ**: 0.133

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.7% |
| Q5 CAGR | +6.4% |
| Q1-Q5 Spread | +12.7% |
| ICIR | 0.00 |
| IC Hit Rate | 47% |
| Monotonicity | 100% |
| Alpha Win Rate | 57% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.54 |
| Q1 MaxDD | -33.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +2.8% | -50.8% | -1.6% | 0.819 | -0.06 | 0.472 |
| All_Cap | +2.2% | -52.9% | -1.6% | 0.439 | -0.11 | 0.410 |
| Mid_Cap | +1.5% | -42.8% | -2.3% | 0.555 | -0.17 | 0.323 |
| Mega_Cap | +0.2% | -47.7% | -3.9% | 0.000 | -0.31 | 0.157 |
| Large_Cap | -1.4% | -49.5% | -5.6% | 0.332 | -0.58 | 0.061 |

**Signal Translation**: Best portfolio = **Small_Cap** at +2.8% CAGR | Factor Q1 = +12.7% | Gap = **-9.9%**

---
### Momentum (FIP)
**Score column**: `momentum_fip_score` | **Direction**: Higher is better | **Factor OBQ**: 0.131

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.2% |
| Q5 CAGR | +4.7% |
| Q1-Q5 Spread | +1.9% |
| ICIR | 0.24 |
| IC Hit Rate | 67% |
| Monotonicity | 50% |
| Alpha Win Rate | 43% |
| Staircase | 0.00 |
| Q1 Sharpe | 0.39 |
| Q1 MaxDD | -38.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +7.0% | -64.3% | +2.8% | 0.738 | 0.13 | 0.628 |
| Small_Cap | +6.8% | -35.1% | +3.4% | 0.904 | 0.16 | 0.537 |
| All_Cap | +5.0% | -60.8% | +1.7% | 0.475 | 0.04 | 0.536 |
| Mid_Cap | +2.0% | -79.3% | -1.5% | 0.187 | -0.08 | 0.438 |
| Mega_Cap | +1.3% | -75.0% | -2.2% | 0.013 | -0.10 | 0.452 |

**Signal Translation**: Best portfolio = **Large_Cap** at +7.0% CAGR | Factor Q1 = +5.2% | Gap = **+1.8%**

---
### Price/Earnings
**Score column**: `cyc2_pe` | **Direction**: Lower is better | **Factor OBQ**: 0.129

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +7.0% |
| Q1-Q5 Spread | +4.5% |
| ICIR | -0.36 |
| IC Hit Rate | 41% |
| Monotonicity | 75% |
| Alpha Win Rate | 57% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.74 |
| Q1 MaxDD | -23.3% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +7.1% | -56.7% | +3.2% | 0.887 | 0.12 | 0.605 |
| Small_Cap | +5.7% | -56.6% | +1.2% | 0.869 | 0.09 | 0.501 |
| Mid_Cap | +5.3% | -59.0% | +1.3% | 0.790 | 0.05 | 0.547 |
| Mega_Cap | +3.9% | -61.9% | -0.3% | 0.717 | -0.01 | 0.537 |
| All_Cap | +2.1% | -60.6% | -3.0% | 0.580 | -0.08 | 0.408 |

**Signal Translation**: Best portfolio = **Large_Cap** at +7.1% CAGR | Factor Q1 = +10.2% | Gap = **-3.1%**

---
### Financials Capital Adequacy
**Score column**: `cyc5_financials_capital` | **Direction**: Higher is better | **Factor OBQ**: 0.125

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.9% |
| Q5 CAGR | +8.8% |
| Q1-Q5 Spread | +5.9% |
| ICIR | 0.00 |
| IC Hit Rate | 30% |
| Monotonicity | 25% |
| Alpha Win Rate | 43% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.15 |
| Q1 MaxDD | -49.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +3.0% | -39.4% | -0.3% | 0.662 | -0.06 | 0.345 |
| Small_Cap | +2.0% | -47.4% | -2.1% | 0.526 | -0.13 | 0.384 |
| All_Cap | +0.1% | -50.7% | -4.0% | 0.008 | -0.37 | 0.100 |
| Mid_Cap | -1.0% | -62.5% | -5.3% | 0.413 | -0.42 | 0.217 |
| Large_Cap | -1.2% | -50.3% | -5.2% | 0.452 | -0.40 | 0.094 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +3.0% CAGR | Factor Q1 = +5.9% | Gap = **-2.9%**

---
### Share Count Change
**Score column**: `cyc2_share_chg` | **Direction**: Lower is better | **Factor OBQ**: 0.122

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.7% |
| Q5 CAGR | +8.2% |
| Q1-Q5 Spread | -0.1% |
| ICIR | 0.07 |
| IC Hit Rate | 53% |
| Monotonicity | 50% |
| Alpha Win Rate | 57% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.57 |
| Q1 MaxDD | -28.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +8.9% | -56.8% | +5.4% | 0.933 | 0.20 | 0.663 |
| Large_Cap | +6.6% | -63.7% | +2.5% | 0.851 | 0.10 | 0.613 |
| Mid_Cap | +6.2% | -59.9% | +2.3% | 0.815 | 0.09 | 0.578 |
| Mega_Cap | +5.4% | -57.1% | +1.2% | 0.860 | 0.06 | 0.573 |
| All_Cap | +3.8% | -63.8% | -1.3% | 0.489 | -0.01 | 0.490 |

**Signal Translation**: Best portfolio = **Small_Cap** at +8.9% CAGR | Factor Q1 = +7.7% | Gap = **+1.2%**

---
### Sales Stability
**Score column**: `cyc4_sales_stability` | **Direction**: Higher is better | **Factor OBQ**: 0.121

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.00 |
| IC Hit Rate | 30% |
| Monotonicity | 75% |
| Alpha Win Rate | 67% |
| Staircase | 0.04 |
| Q1 Sharpe | 0.63 |
| Q1 MaxDD | -28.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +1.7% | -45.3% | -2.5% | 0.537 | -0.15 | 0.193 |
| Large_Cap | +0.6% | -50.0% | -3.7% | 0.381 | -0.24 | 0.095 |
| Mega_Cap | -0.4% | -55.2% | -3.6% | 0.305 | -0.30 | 0.124 |
| All_Cap | -1.1% | -80.2% | -5.3% | 0.004 | -0.27 | 0.163 |
| Mid_Cap | -1.3% | -44.2% | -5.3% | 0.134 | -0.48 | -0.066 |

**Signal Translation**: Best portfolio = **Small_Cap** at +1.7% CAGR | Factor Q1 = +10.2% | Gap = **-8.5%**

---
### Cash Conversion Cycle
**Score column**: `cyc4_cash_conv_cycle` | **Direction**: Lower is better | **Factor OBQ**: 0.115

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.7% |
| Q5 CAGR | +6.4% |
| Q1-Q5 Spread | +8.7% |
| ICIR | 0.00 |
| IC Hit Rate | 47% |
| Monotonicity | 100% |
| Alpha Win Rate | 57% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.55 |
| Q1 MaxDD | -33.5% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +9.4% | -54.2% | +5.2% | 0.939 | 0.22 | 0.629 |
| Small_Cap | +7.1% | -61.5% | +3.5% | 0.880 | 0.14 | 0.641 |
| Mega_Cap | +6.1% | -49.8% | +2.5% | 0.883 | 0.10 | 0.564 |
| Mid_Cap | +5.3% | -45.2% | +1.3% | 0.911 | 0.06 | 0.494 |
| All_Cap | +4.7% | -64.5% | -0.0% | 0.717 | 0.03 | 0.524 |

**Signal Translation**: Best portfolio = **Large_Cap** at +9.4% CAGR | Factor Q1 = +8.7% | Gap = **+0.7%**

---
### EPS Stability
**Score column**: `cyc4_eps_stability` | **Direction**: Higher is better | **Factor OBQ**: 0.105

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +6.6% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.00 |
| IC Hit Rate | 43% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.50 |
| Q1 MaxDD | -28.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +2.5% | -44.4% | -0.8% | 0.786 | -0.11 | 0.363 |
| Small_Cap | +2.3% | -42.9% | -2.1% | 0.619 | -0.10 | 0.324 |
| Mid_Cap | +1.6% | -56.5% | -2.7% | 0.539 | -0.13 | 0.318 |
| Large_Cap | +1.0% | -35.4% | -3.0% | 0.556 | -0.20 | 0.198 |
| All_Cap | -0.3% | -36.4% | -4.4% | 0.367 | -0.53 | -0.071 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +2.5% CAGR | Factor Q1 = +10.2% | Gap = **-7.7%**

---
### EV/EBITDA
**Score column**: `cyc2_ev_ebitda` | **Direction**: Lower is better | **Factor OBQ**: 0.104

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +5.4% |
| Q1-Q5 Spread | +5.8% |
| ICIR | -0.51 |
| IC Hit Rate | 35% |
| Monotonicity | 75% |
| Alpha Win Rate | 60% |
| Staircase | 0.02 |
| Q1 Sharpe | 0.71 |
| Q1 MaxDD | -22.4% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +7.2% | -60.3% | +3.3% | 0.887 | 0.12 | 0.627 |
| Small_Cap | +6.0% | -56.7% | +1.5% | 0.850 | 0.10 | 0.549 |
| Mid_Cap | +5.3% | -60.3% | +1.3% | 0.835 | 0.05 | 0.548 |
| Mega_Cap | +3.6% | -62.1% | -0.7% | 0.672 | -0.02 | 0.538 |
| All_Cap | +0.2% | -61.5% | -4.8% | 0.108 | -0.15 | 0.234 |

**Signal Translation**: Best portfolio = **Large_Cap** at +7.2% CAGR | Factor Q1 = +10.2% | Gap = **-3.0%**

---
### Debt / Assets
**Score column**: `cyc2_debt_assets` | **Direction**: Higher is better | **Factor OBQ**: 0.094

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +2.8% |
| Q5 CAGR | +7.6% |
| Q1-Q5 Spread | -2.4% |
| ICIR | 0.87 |
| IC Hit Rate | 80% |
| Monotonicity | 25% |
| Alpha Win Rate | 33% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.25 |
| Q1 MaxDD | -41.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +2.5% | -61.0% | -1.5% | 0.568 | -0.07 | 0.454 |
| Mid_Cap | +2.4% | -75.0% | -1.3% | 0.743 | -0.06 | 0.557 |
| Small_Cap | +0.8% | -74.8% | -3.4% | 0.060 | -0.14 | 0.408 |
| All_Cap | -2.6% | -87.2% | -6.9% | 0.272 | -0.29 | 0.228 |
| Mega_Cap | -5.0% | -93.8% | -9.6% | 0.818 | -0.33 | 0.174 |

**Signal Translation**: Best portfolio = **Large_Cap** at +2.5% CAGR | Factor Q1 = +2.8% | Gap = **-0.3%**

---
### ROA Deviation
**Score column**: `cyc4_roa_dev` | **Direction**: Higher is better | **Factor OBQ**: 0.089

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.9% |
| Q5 CAGR | +2.5% |
| Q1-Q5 Spread | +10.9% |
| ICIR | 0.23 |
| IC Hit Rate | 54% |
| Monotonicity | 75% |
| Alpha Win Rate | 37% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.23 |
| Q1 MaxDD | -28.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +2.2% | -54.3% | -1.5% | 0.756 | -0.11 | 0.389 |
| Small_Cap | +0.9% | -77.2% | -3.6% | 0.659 | -0.15 | 0.460 |
| Mega_Cap | -1.2% | -53.8% | -5.3% | 0.213 | -0.43 | 0.044 |
| Mid_Cap | -1.3% | -38.3% | -5.4% | 0.374 | -0.48 | -0.065 |
| All_Cap | -1.3% | -65.9% | -5.6% | 0.016 | -0.33 | 0.115 |

**Signal Translation**: Best portfolio = **Large_Cap** at +2.2% CAGR | Factor Q1 = +10.9% | Gap = **-8.7%**

---
### Net Debt / EBITDA
**Score column**: `cyc2_nd_ebitda` | **Direction**: Lower is better | **Factor OBQ**: 0.084

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +4.0% |
| Q5 CAGR | +7.8% |
| Q1-Q5 Spread | -2.2% |
| ICIR | 0.68 |
| IC Hit Rate | 77% |
| Monotonicity | 50% |
| Alpha Win Rate | 33% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.30 |
| Q1 MaxDD | -39.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +9.3% | -56.6% | +5.9% | 0.941 | 0.22 | 0.647 |
| Large_Cap | +6.1% | -59.3% | +2.0% | 0.872 | 0.08 | 0.579 |
| Mid_Cap | +5.5% | -60.6% | +1.9% | 0.802 | 0.06 | 0.603 |
| Mega_Cap | +5.3% | -59.5% | +1.3% | 0.796 | 0.05 | 0.568 |
| All_Cap | -2.3% | -88.0% | -7.0% | 0.140 | -0.23 | 0.276 |

**Signal Translation**: Best portfolio = **Small_Cap** at +9.3% CAGR | Factor Q1 = +4.0% | Gap = **+5.3%**

---
### Utilities Safe Yield
**Score column**: `cyc5_utilities_safe_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.078

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.3% |
| Q5 CAGR | +9.9% |
| Q1-Q5 Spread | +0.3% |
| ICIR | 0.00 |
| IC Hit Rate | 57% |
| Monotonicity | 25% |
| Alpha Win Rate | 50% |
| Staircase | 0.00 |
| Q1 Sharpe | 0.64 |
| Q1 MaxDD | -32.6% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +4.4% | -36.7% | +0.8% | 0.828 | 0.02 | 0.463 |
| Large_Cap | +3.3% | -43.8% | -0.6% | 0.772 | -0.05 | 0.441 |
| Mid_Cap | +2.3% | -40.8% | -1.2% | 0.705 | -0.11 | 0.363 |
| Small_Cap | +2.0% | -40.8% | -2.1% | 0.702 | -0.13 | 0.330 |
| All_Cap | -0.1% | -47.6% | -4.1% | 0.271 | -0.32 | 0.119 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +4.4% CAGR | Factor Q1 = +8.3% | Gap = **-3.9%**

---
### Interest % of Operating Income
**Score column**: `cyc2_int_pct_op` | **Direction**: Higher is better | **Factor OBQ**: 0.075

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.5% |
| Q5 CAGR | +7.5% |
| Q1-Q5 Spread | -0.6% |
| ICIR | 0.38 |
| IC Hit Rate | 62% |
| Monotonicity | 75% |
| Alpha Win Rate | 37% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.41 |
| Q1 MaxDD | -37.2% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +5.9% | -50.3% | +2.1% | 0.820 | 0.08 | 0.547 |
| Small_Cap | +2.7% | -43.6% | -0.7% | 0.587 | -0.07 | 0.403 |
| Mega_Cap | +1.7% | -67.1% | -2.2% | 0.017 | -0.10 | 0.397 |
| All_Cap | +0.6% | -71.9% | -3.4% | 0.000 | -0.17 | 0.391 |
| Mid_Cap | -0.5% | -79.4% | -4.7% | 0.252 | -0.19 | 0.437 |

**Signal Translation**: Best portfolio = **Large_Cap** at +5.9% CAGR | Factor Q1 = +5.5% | Gap = **+0.5%**

---
### Dividend Yield
**Score column**: `cyc4_dividend_yield` | **Direction**: Higher is better | **Factor OBQ**: 0.071

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.0% |
| Q5 CAGR | +9.7% |
| Q1-Q5 Spread | +4.9% |
| ICIR | 0.01 |
| IC Hit Rate | 57% |
| Monotonicity | 50% |
| Alpha Win Rate | 43% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.63 |
| Q1 MaxDD | -31.7% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Small_Cap | +3.6% | -43.2% | +0.2% | 0.861 | -0.02 | 0.435 |
| Mid_Cap | +3.1% | -34.7% | -0.6% | 0.880 | -0.06 | 0.433 |
| All_Cap | +2.6% | -47.0% | -1.0% | 0.518 | -0.08 | 0.385 |
| Large_Cap | +1.8% | -37.2% | -2.0% | 0.685 | -0.16 | 0.353 |
| Mega_Cap | +0.8% | -50.1% | -2.7% | 0.301 | -0.24 | 0.249 |

**Signal Translation**: Best portfolio = **Small_Cap** at +3.6% CAGR | Factor Q1 = +8.0% | Gap = **-4.4%**

---
### Change AR / Assets
**Score column**: `cyc4_change_ar_assets` | **Direction**: Higher is better | **Factor OBQ**: 0.066

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.5% |
| Q5 CAGR | +6.4% |
| Q1-Q5 Spread | +8.5% |
| ICIR | 0.53 |
| IC Hit Rate | 62% |
| Monotonicity | 50% |
| Alpha Win Rate | 40% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.35 |
| Q1 MaxDD | -38.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | +6.7% | -55.6% | +2.4% | 0.798 | 0.10 | 0.521 |
| Small_Cap | +3.5% | -67.6% | -1.1% | 0.881 | -0.03 | 0.573 |
| Mid_Cap | +1.1% | -39.1% | -2.4% | 0.746 | -0.18 | 0.265 |
| Mega_Cap | -0.7% | -45.0% | -4.8% | 0.087 | -0.35 | 0.096 |
| Large_Cap | -1.3% | -46.0% | -5.0% | 0.565 | -0.48 | 0.066 |

**Signal Translation**: Best portfolio = **All_Cap** at +6.7% CAGR | Factor Q1 = +8.5% | Gap = **-1.7%**

---
### Log Market Cap
**Score column**: `cyc4_log_market_cap` | **Direction**: Higher is better | **Factor OBQ**: 0.065

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.8% |
| Q5 CAGR | +5.1% |
| Q1-Q5 Spread | +7.8% |
| ICIR | 0.11 |
| IC Hit Rate | 62% |
| Monotonicity | 50% |
| Alpha Win Rate | 37% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.31 |
| Q1 MaxDD | -29.8% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +3.2% | -24.2% | -1.2% | 0.920 | -0.07 | 0.373 |
| Small_Cap | +1.7% | -36.1% | -2.8% | 0.625 | -0.18 | 0.257 |
| Large_Cap | +0.5% | -30.4% | -3.4% | 0.033 | -0.30 | 0.040 |
| All_Cap | +0.4% | -37.8% | -4.5% | 0.001 | -0.29 | 0.076 |
| Mega_Cap | +0.4% | -37.8% | -4.5% | 0.001 | -0.29 | 0.080 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +3.2% CAGR | Factor Q1 = +7.8% | Gap = **-4.6%**

---
### Tax Paid / Sales
**Score column**: `cyc4_tax_paid_sales` | **Direction**: Higher is better | **Factor OBQ**: 0.053

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +5.8% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.14 |
| IC Hit Rate | 53% |
| Monotonicity | 75% |
| Alpha Win Rate | 33% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.27 |
| Q1 MaxDD | -37.9% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mid_Cap | +4.5% | -57.7% | +0.3% | 0.886 | 0.02 | 0.535 |
| Small_Cap | -0.1% | -67.7% | -3.8% | 0.074 | -0.27 | 0.295 |
| Mega_Cap | -0.6% | -51.8% | -5.0% | 0.481 | -0.40 | 0.134 |
| Large_Cap | -0.7% | -62.9% | -5.1% | 0.330 | -0.29 | 0.228 |
| All_Cap | -1.8% | -58.0% | -5.9% | 0.712 | -0.51 | 0.108 |

**Signal Translation**: Best portfolio = **Mid_Cap** at +4.5% CAGR | Factor Q1 = +10.2% | Gap = **-5.7%**

---
### Consumer Disc Brand Value
**Score column**: `cyc5_consumer_disc_brand` | **Direction**: Higher is better | **Factor OBQ**: 0.046

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.1% |
| Q5 CAGR | +4.1% |
| Q1-Q5 Spread | +6.1% |
| ICIR | 0.06 |
| IC Hit Rate | 56% |
| Monotonicity | 75% |
| Alpha Win Rate | 37% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.33 |
| Q1 MaxDD | -39.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Mega_Cap | +1.1% | -54.0% | -2.1% | 0.580 | -0.20 | 0.240 |
| All_Cap | +0.5% | -33.1% | -2.8% | 0.373 | -0.30 | 0.083 |
| Small_Cap | -1.2% | -48.3% | -5.5% | 0.181 | -0.48 | 0.001 |
| Large_Cap | -1.2% | -44.9% | -5.1% | 0.588 | -0.54 | -0.021 |
| Mid_Cap | -1.6% | -44.3% | -6.1% | 0.671 | -0.65 | -0.033 |

**Signal Translation**: Best portfolio = **Mega_Cap** at +1.1% CAGR | Factor Q1 = +6.1% | Gap = **-5.0%**

---
### combo_T7
**Score column**: `combo_T7` | **Direction**: Higher is better | **Factor OBQ**: 0.035

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.0% |
| Q5 CAGR | +9.2% |
| Q1-Q5 Spread | -2.2% |
| ICIR | -0.22 |
| IC Hit Rate | 50% |
| Monotonicity | 75% |
| Alpha Win Rate | 50% |
| Staircase | -0.01 |
| Q1 Sharpe | 0.50 |
| Q1 MaxDD | -47.3% |

*No portfolio backtest available for this factor.*

---
### Change Inventory / Assets
**Score column**: `cyc4_change_inv_assets` | **Direction**: Higher is better | **Factor OBQ**: 0.033

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.2% |
| Q5 CAGR | +6.0% |
| Q1-Q5 Spread | +8.2% |
| ICIR | 0.00 |
| IC Hit Rate | 52% |
| Monotonicity | 75% |
| Alpha Win Rate | 43% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.41 |
| Q1 MaxDD | -38.1% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| Large_Cap | +3.5% | -58.5% | -0.5% | 0.746 | -0.03 | 0.465 |
| Mega_Cap | +2.4% | -40.4% | -1.2% | 0.726 | -0.10 | 0.379 |
| Mid_Cap | +2.2% | -63.2% | -1.4% | 0.537 | -0.09 | 0.454 |
| Small_Cap | +2.0% | -49.8% | -1.8% | 0.546 | -0.11 | 0.372 |
| All_Cap | +1.9% | -59.2% | -1.9% | 0.572 | -0.11 | 0.382 |

**Signal Translation**: Best portfolio = **Large_Cap** at +3.5% CAGR | Factor Q1 = +8.2% | Gap = **-4.7%**

---
### Altman Z-Score
**Score column**: `cyc4_altman_z` | **Direction**: Higher is better | **Factor OBQ**: 0.016

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.4% |
| Q5 CAGR | +0.4% |
| Q1-Q5 Spread | +6.0% |
| ICIR | 0.19 |
| IC Hit Rate | 50% |
| Monotonicity | 100% |
| Alpha Win Rate | 13% |
| Staircase | 0.05 |
| Q1 Sharpe | 0.07 |
| Q1 MaxDD | 0.0% |

#### Portfolio Backtest (28 stocks, 4-tranche quarterly)
| Cap Tier | CAGR | MaxDD | Alpha | R² | Sharpe | OBQ Port |
|---|---|---|---|---|---|---|
| All_Cap | -1.6% | -43.7% | -5.6% | 0.258 | -1.31 | -0.161 |

**Signal Translation**: Best portfolio = **All_Cap** at -1.6% CAGR | Factor Q1 = +5.4% | Gap = **-7.0%**

---
### Cash / Assets
**Score column**: `cyc2_cash_assets` | **Direction**: Higher is better | **Factor OBQ**: -0.009

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +2.4% |
| Q5 CAGR | +7.5% |
| Q1-Q5 Spread | -3.1% |
| ICIR | -0.46 |
| IC Hit Rate | 40% |
| Monotonicity | 75% |
| Alpha Win Rate | 37% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.22 |
| Q1 MaxDD | -47.8% |

*No portfolio backtest available for this factor.*

---
### Quick Ratio
**Score column**: `cyc4_quick_ratio` | **Direction**: Higher is better | **Factor OBQ**: -0.025

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +7.0% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.00 |
| IC Hit Rate | 42% |
| Monotonicity | 50% |
| Alpha Win Rate | 50% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.20 |
| Q1 MaxDD | -38.1% |

*No portfolio backtest available for this factor.*

---
### CapEx / OCF
**Score column**: `cyc2_capex_ocf` | **Direction**: Higher is better | **Factor OBQ**: -0.025

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +2.0% |
| Q5 CAGR | +6.0% |
| Q1-Q5 Spread | -3.4% |
| ICIR | 0.60 |
| IC Hit Rate | 64% |
| Monotonicity | 75% |
| Alpha Win Rate | 30% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.20 |
| Q1 MaxDD | -62.2% |

*No portfolio backtest available for this factor.*

---
### Current Ratio
**Score column**: `cyc4_current_ratio` | **Direction**: Higher is better | **Factor OBQ**: -0.034

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +10.2% |
| Q5 CAGR | +7.0% |
| Q1-Q5 Spread | +10.2% |
| ICIR | 0.00 |
| IC Hit Rate | 43% |
| Monotonicity | 50% |
| Alpha Win Rate | 47% |
| Staircase | 0.01 |
| Q1 Sharpe | 0.22 |
| Q1 MaxDD | -38.1% |

*No portfolio backtest available for this factor.*

---
### combo_T5
**Score column**: `combo_T5` | **Direction**: Higher is better | **Factor OBQ**: -0.087

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.7% |
| Q5 CAGR | +10.1% |
| Q1-Q5 Spread | -2.4% |
| ICIR | -0.55 |
| IC Hit Rate | 33% |
| Monotonicity | 25% |
| Alpha Win Rate | 37% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.55 |
| Q1 MaxDD | -44.6% |

*No portfolio backtest available for this factor.*

---
### combo_L3
**Score column**: `combo_L3` | **Direction**: Higher is better | **Factor OBQ**: -0.087

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.7% |
| Q5 CAGR | +10.1% |
| Q1-Q5 Spread | -2.4% |
| ICIR | -0.55 |
| IC Hit Rate | 33% |
| Monotonicity | 25% |
| Alpha Win Rate | 37% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.55 |
| Q1 MaxDD | -44.6% |

*No portfolio backtest available for this factor.*

---
### FIP 12M
**Score column**: `cyc2_fip_12m` | **Direction**: Higher is better | **Factor OBQ**: -0.091

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +2.1% |
| Q5 CAGR | +4.6% |
| Q1-Q5 Spread | -1.5% |
| ICIR | -0.07 |
| IC Hit Rate | 43% |
| Monotonicity | 75% |
| Alpha Win Rate | 23% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.21 |
| Q1 MaxDD | -51.3% |

*No portfolio backtest available for this factor.*

---
### FIP 6M
**Score column**: `cyc2_fip_6m` | **Direction**: Higher is better | **Factor OBQ**: -0.093

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +1.8% |
| Q5 CAGR | +4.5% |
| Q1-Q5 Spread | -1.7% |
| ICIR | -0.16 |
| IC Hit Rate | 53% |
| Monotonicity | 50% |
| Alpha Win Rate | 30% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.20 |
| Q1 MaxDD | -44.2% |

*No portfolio backtest available for this factor.*

---
### combo_O5
**Score column**: `combo_O5` | **Direction**: Higher is better | **Factor OBQ**: -0.093

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +7.3% |
| Q5 CAGR | +10.9% |
| Q1-Q5 Spread | -3.6% |
| ICIR | -0.53 |
| IC Hit Rate | 35% |
| Monotonicity | 25% |
| Alpha Win Rate | 37% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.54 |
| Q1 MaxDD | -47.5% |

*No portfolio backtest available for this factor.*

---
### R&D Ratio
**Score column**: `cyc2_rd_ratio` | **Direction**: Higher is better | **Factor OBQ**: -0.127

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +2.7% |
| Q5 CAGR | +8.3% |
| Q1-Q5 Spread | -4.5% |
| ICIR | -0.38 |
| IC Hit Rate | 38% |
| Monotonicity | 75% |
| Alpha Win Rate | 37% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.23 |
| Q1 MaxDD | -53.7% |

*No portfolio backtest available for this factor.*

---
### combo_T6
**Score column**: `combo_T6` | **Direction**: Higher is better | **Factor OBQ**: -0.139

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.4% |
| Q5 CAGR | +10.3% |
| Q1-Q5 Spread | -3.9% |
| ICIR | -0.61 |
| IC Hit Rate | 33% |
| Monotonicity | 25% |
| Alpha Win Rate | 30% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.44 |
| Q1 MaxDD | -50.8% |

*No portfolio backtest available for this factor.*

---
### combo_T3
**Score column**: `combo_T3` | **Direction**: Higher is better | **Factor OBQ**: -0.140

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +8.4% |
| Q5 CAGR | +12.5% |
| Q1-Q5 Spread | -4.1% |
| ICIR | -0.81 |
| IC Hit Rate | 32% |
| Monotonicity | 25% |
| Alpha Win Rate | 40% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.59 |
| Q1 MaxDD | -40.8% |

*No portfolio backtest available for this factor.*

---
### combo_O2
**Score column**: `combo_O2` | **Direction**: Higher is better | **Factor OBQ**: -0.143

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.5% |
| Q5 CAGR | +10.3% |
| Q1-Q5 Spread | -4.7% |
| ICIR | -0.46 |
| IC Hit Rate | 42% |
| Monotonicity | 50% |
| Alpha Win Rate | 33% |
| Staircase | -0.01 |
| Q1 Sharpe | 0.40 |
| Q1 MaxDD | -49.8% |

*No portfolio backtest available for this factor.*

---
### combo_L6
**Score column**: `combo_L6` | **Direction**: Higher is better | **Factor OBQ**: -0.152

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +4.9% |
| Q5 CAGR | +9.6% |
| Q1-Q5 Spread | -4.8% |
| ICIR | -0.76 |
| IC Hit Rate | 28% |
| Monotonicity | 25% |
| Alpha Win Rate | 30% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.36 |
| Q1 MaxDD | -53.0% |

*No portfolio backtest available for this factor.*

---
### Working Capital / Assets
**Score column**: `cyc4_wc_assets` | **Direction**: Higher is better | **Factor OBQ**: -0.165

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +12.1% |
| Q5 CAGR | +0.0% |
| Q1-Q5 Spread | +14.5% |
| ICIR | 0.00 |
| IC Hit Rate | 17% |
| Monotonicity | 100% |
| Alpha Win Rate | 17% |
| Staircase | 0.09 |
| Q1 Sharpe | 0.18 |
| Q1 MaxDD | 0.0% |

*No portfolio backtest available for this factor.*

---
### combo_T8
**Score column**: `combo_T8` | **Direction**: Higher is better | **Factor OBQ**: -0.179

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +6.3% |
| Q5 CAGR | +11.0% |
| Q1-Q5 Spread | -4.7% |
| ICIR | -0.57 |
| IC Hit Rate | 32% |
| Monotonicity | 25% |
| Alpha Win Rate | 27% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.50 |
| Q1 MaxDD | -44.5% |

*No portfolio backtest available for this factor.*

---
### combo_L4
**Score column**: `combo_L4` | **Direction**: Higher is better | **Factor OBQ**: -0.220

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.9% |
| Q5 CAGR | +12.1% |
| Q1-Q5 Spread | -6.2% |
| ICIR | -0.81 |
| IC Hit Rate | 27% |
| Monotonicity | 25% |
| Alpha Win Rate | 20% |
| Staircase | -0.01 |
| Q1 Sharpe | 0.42 |
| Q1 MaxDD | -50.0% |

*No portfolio backtest available for this factor.*

---
### combo_T1
**Score column**: `combo_T1` | **Direction**: Higher is better | **Factor OBQ**: -0.240

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.9% |
| Q5 CAGR | +10.9% |
| Q1-Q5 Spread | -5.0% |
| ICIR | -0.87 |
| IC Hit Rate | 23% |
| Monotonicity | 50% |
| Alpha Win Rate | 20% |
| Staircase | -0.01 |
| Q1 Sharpe | 0.43 |
| Q1 MaxDD | -48.2% |

*No portfolio backtest available for this factor.*

---
### combo_T2
**Score column**: `combo_T2` | **Direction**: Higher is better | **Factor OBQ**: -0.240

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +5.9% |
| Q5 CAGR | +10.9% |
| Q1-Q5 Spread | -5.0% |
| ICIR | -0.87 |
| IC Hit Rate | 23% |
| Monotonicity | 50% |
| Alpha Win Rate | 20% |
| Staircase | -0.01 |
| Q1 Sharpe | 0.43 |
| Q1 MaxDD | -48.2% |

*No portfolio backtest available for this factor.*

---
### combo_L5
**Score column**: `combo_L5` | **Direction**: Higher is better | **Factor OBQ**: -0.291

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +2.7% |
| Q5 CAGR | +10.2% |
| Q1-Q5 Spread | -7.6% |
| ICIR | -1.01 |
| IC Hit Rate | 18% |
| Monotonicity | 25% |
| Alpha Win Rate | 17% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.24 |
| Q1 MaxDD | -53.8% |

*No portfolio backtest available for this factor.*

---
### Moat Rank
**Score column**: `moat_rank` | **Direction**: Lower is better | **Factor OBQ**: -0.295

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | +1.0% |
| Q5 CAGR | +12.5% |
| Q1-Q5 Spread | -10.6% |
| ICIR | -1.02 |
| IC Hit Rate | 26% |
| Monotonicity | 25% |
| Alpha Win Rate | 23% |
| Staircase | 0.00 |
| Q1 Sharpe | 0.15 |
| Q1 MaxDD | -56.1% |

*No portfolio backtest available for this factor.*

---
### combo_O1
**Score column**: `combo_O1` | **Direction**: Higher is better | **Factor OBQ**: -0.304

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | -2.5% |
| Q5 CAGR | +10.3% |
| Q1-Q5 Spread | -12.7% |
| ICIR | -1.05 |
| IC Hit Rate | 28% |
| Monotonicity | 25% |
| Alpha Win Rate | 23% |
| Staircase | -0.00 |
| Q1 Sharpe | -0.00 |
| Q1 MaxDD | -79.6% |

*No portfolio backtest available for this factor.*

---
### combo_L1
**Score column**: `combo_L1` | **Direction**: Higher is better | **Factor OBQ**: -0.312

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | -1.8% |
| Q5 CAGR | +10.5% |
| Q1-Q5 Spread | -12.3% |
| ICIR | -0.99 |
| IC Hit Rate | 22% |
| Monotonicity | 50% |
| Alpha Win Rate | 23% |
| Staircase | -0.00 |
| Q1 Sharpe | 0.04 |
| Q1 MaxDD | -80.5% |

*No portfolio backtest available for this factor.*

---
### combo_O4
**Score column**: `combo_O4` | **Direction**: Higher is better | **Factor OBQ**: -0.318

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | -4.0% |
| Q5 CAGR | +9.2% |
| Q1-Q5 Spread | -13.2% |
| ICIR | -0.92 |
| IC Hit Rate | 27% |
| Monotonicity | 50% |
| Alpha Win Rate | 17% |
| Staircase | -0.00 |
| Q1 Sharpe | -0.07 |
| Q1 MaxDD | -85.3% |

*No portfolio backtest available for this factor.*

---
### combo_O3
**Score column**: `combo_O3` | **Direction**: Higher is better | **Factor OBQ**: -0.386

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | -4.1% |
| Q5 CAGR | +10.5% |
| Q1-Q5 Spread | -14.6% |
| ICIR | -1.45 |
| IC Hit Rate | 17% |
| Monotonicity | 25% |
| Alpha Win Rate | 13% |
| Staircase | -0.00 |
| Q1 Sharpe | -0.07 |
| Q1 MaxDD | -89.2% |

*No portfolio backtest available for this factor.*

---
### combo_L2
**Score column**: `combo_L2` | **Direction**: Higher is better | **Factor OBQ**: -0.392

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | -4.1% |
| Q5 CAGR | +10.0% |
| Q1-Q5 Spread | -14.1% |
| ICIR | -1.40 |
| IC Hit Rate | 12% |
| Monotonicity | 25% |
| Alpha Win Rate | 13% |
| Staircase | -0.00 |
| Q1 Sharpe | -0.05 |
| Q1 MaxDD | -87.5% |

*No portfolio backtest available for this factor.*

---
### combo_T4
**Score column**: `combo_T4` | **Direction**: Higher is better | **Factor OBQ**: -0.401

#### Factor Backtest (Quintile Sort)
| Metric | Value |
|---|---|
| Q1 CAGR | -6.0% |
| Q5 CAGR | +11.7% |
| Q1-Q5 Spread | -17.7% |
| ICIR | -1.53 |
| IC Hit Rate | 15% |
| Monotonicity | 0% |
| Alpha Win Rate | 10% |
| Staircase | 0.00 |
| Q1 Sharpe | -0.15 |
| Q1 MaxDD | -92.0% |

*No portfolio backtest available for this factor.*

---
## Conclusions & Next Steps

### What the Data Shows
1. **Factor signals are real** — cross-sectional IC, spread, and monotonicity confirm predictive power
2. **Portfolio translation is lossy** — average 4-5% CAGR decay from factor Q1 to portfolio
3. **Concentration destroys alpha** — 28 stocks is insufficient diversification for factor exposure
4. **Small-cap is where alpha lives** — factor signals are strongest in the smallest quartile
5. **CYC-4/5 factors have coverage gaps** — ~40 factors lack sufficient score history for valid portfolio testing

### Recommended CYC-010 Design
1. **Universe**: Top 500 by market cap (eliminates micro-cap noise, matches institutional reality)
2. **Portfolio size**: 50-100 stocks (3-4x more diversification vs current 28)
3. **Benchmark**: Russell 1000 equal-weight (fairer comparison than cap-weighted SPX)
4. **Momentum gate**: Pre-filter stocks with 6M momentum > 0 before factor ranking
5. **Factor whitelist**: Only factors with >90% score coverage (exclude CYC-4/5 sparse factors)
6. **Annual December rebalance**: CYC-006 found this optimal, semiannual as secondary test

---
*Generated by OBQ FactorLab GPU Pipeline | 2026-05-16 13:29*