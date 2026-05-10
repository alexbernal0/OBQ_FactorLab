# -*- coding: utf-8 -*-
"""
CYC-001 Batch 3 — Separate Table Factors
LongEQ, Rulebreaker, Fundsmith, Moat
Quarterly rebalance only (as specified — no monthly).
"""
import sys, time, traceback
sys.path.insert(0, '.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model
from engine.portfolio_bank import save_portfolio_model

# Separate-table scores — quarterly rebalance only
SCORES_BATCH3 = {
    "longeq_rank":      ("LongEQ Rank (quality gates rank)",         "lower_better"),
    "rulebreaker_rank": ("Rulebreaker Rank (growth quality rank)",    "lower_better"),
    "fundsmith_rank":   ("Fundsmith Rank (quality compounders rank)", "lower_better"),
    "moat_score":       ("Moat Score (economic moat gates)",          "higher_better"),
    "moat_rank":        ("Moat Rank (economic moat rank)",            "lower_better"),
}

COMMON = dict(
    start_date="1990-07-31", end_date="2024-12-31",
    min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0,
    transaction_cost_bps=15.0,
)

results, errors = [], []
def _cb(msg): print(f"    {msg}")

print(f"CYC-001 BATCH 3 — {len(SCORES_BATCH3)} scores x 2 = {len(SCORES_BATCH3)*2} runs")
print("Rebalance: Quarterly only (semi-annual for separate tables is too sparse)")

for score_col, (notation, direction) in SCORES_BATCH3.items():
    print(f"\n--- {score_col} ({direction}) ---")

    # Factor backtest (5-quintile, quarterly)
    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score_col,
            score_direction=direction,
            n_buckets=5, hold_months=3,       # 3-month hold for quarterly
            rebalance_freq="quarterly",
            cap_tier="all",
            run_label=f"{notation} | 5Q | 3mo | Large-Cap | 1990-2024 [CYC-001-BASELINE]",
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

    # Portfolio backtest (Top-20, quarterly)
    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score_col,
            score_direction=direction,
            top_n=20, sector_max=5,
            rebalance_freq="quarterly",
            cap_tier="all",
            run_label=f"{notation} | Top-20 | Quarterly | 5/Sector | Large-Cap | 1990-2024 [CYC-001-BASELINE]",
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

print(f"\nBatch 3 complete: {len(results)} saved, {len(errors)} errors")
for e in errors:
    print(f"  ERROR [{e['type']}] {e['score']}: {e['error']}")
