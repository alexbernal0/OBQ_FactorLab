# -*- coding: utf-8 -*-
"""
CYC-001 Batch 2 — Remaining v_backtest_scores factors
Adds jcn_alpha_trifecta, momentum_sys_score,
universe-normalized variants, and AF score variants.
"""
import sys, time, traceback
sys.path.insert(0, '.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model
from engine.portfolio_bank import save_portfolio_model

SCORES_BATCH2 = {
    "jcn_alpha_trifecta":   "JCN Alpha Trifecta (value+quality+momentum trifecta)",
    "momentum_sys_score":   "Momentum Systematic Score",
    "value_score_universe": "Value Score (Universe-Normalized rank)",
    "quality_score_universe":"Quality Score (Universe-Normalized rank)",
    "growth_score_universe": "Growth Score (Universe-Normalized rank)",
    "finstr_score_universe": "Financial Strength (Universe-Normalized rank)",
    "af_universe_score":    "Alpha Factor Score (Universe-Normalized)",
}

COMMON = dict(
    start_date="1990-07-31", end_date="2024-12-31",
    min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0,
    transaction_cost_bps=15.0,
)

results, errors = [], []

def _cb(msg): print(f"    {msg}")

print("CYC-001 BATCH 2 —", len(SCORES_BATCH2), "scores x 2 = ", len(SCORES_BATCH2)*2, "runs")

for score_col, notation in SCORES_BATCH2.items():
    print(f"\n--- {score_col} ---")

    # Factor backtest
    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score_col, n_buckets=5, hold_months=6,
            rebalance_freq="semi-annual", cap_tier="all",
            run_label=f"{notation} | 5Q | 6mo | Large-Cap | 1990-2024 [CYC-001-BASELINE]",
            **COMMON
        ), cb=_cb)
        if r.get("status") == "complete":
            sid = save_factor_model(r, overwrite=True)
            fm = r.get("factor_metrics", {})
            print(f"  FACTOR OK: {sid} | ICIR={fm.get('icir',0):.3f} | Spread={fm.get('quintile_spread_cagr',0)*100:.2f}%")
            results.append({"type":"factor","score":score_col,"sid":sid,"icir":fm.get("icir")})
        else:
            print(f"  FACTOR ERR: {r.get('error')}")
            errors.append({"type":"factor","score":score_col,"error":r.get("error")})
    except Exception as e:
        traceback.print_exc()
        errors.append({"type":"factor","score":score_col,"error":str(e)})

    # Portfolio backtest
    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score_col, top_n=20, sector_max=5,
            rebalance_freq="semi-annual", cap_tier="all",
            run_label=f"{notation} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 [CYC-001-BASELINE]",
            **COMMON
        ), cb=_cb)
        if r2.get("status") == "complete":
            sid2 = save_portfolio_model(r2, overwrite=True)
            pm = r2.get("portfolio_metrics", {})
            print(f"  PORT   OK: {sid2} | CAGR={pm.get('cagr',0)*100:.2f}% | Sharpe={pm.get('sharpe',0):.3f}")
            results.append({"type":"portfolio","score":score_col,"sid":sid2,"sharpe":pm.get("sharpe")})
        else:
            print(f"  PORT   ERR: {r2.get('error')}")
            errors.append({"type":"portfolio","score":score_col,"error":r2.get("error")})
    except Exception as e:
        traceback.print_exc()
        errors.append({"type":"portfolio","score":score_col,"error":str(e)})

print(f"\nBatch 2 complete: {len(results)} saved, {len(errors)} errors")
for e in errors:
    print(f"  ERROR [{e['type']}] {e['score']}: {e['error']}")
