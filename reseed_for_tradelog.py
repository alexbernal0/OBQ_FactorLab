# -*- coding: utf-8 -*-
"""
Reseed all CYC-001 baseline models to populate trade logs.
Only reruns models that have CYC-001-BASELINE in their label.
"""
import sys, time, traceback
sys.path.insert(0, '.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model, get_all_portfolio_models

COMMON = dict(
    start_date="1990-07-31", end_date="2024-12-31",
    min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0,
    transaction_cost_bps=15.0,
)

def _cb(msg): print(f"    {msg}")

# Load existing models that need trade logs
factor_models   = [m for m in get_all_models(200) if "CYC-001" in (m.get("run_label") or "") and not m.get("trade_log_json")]
portfolio_models = [m for m in get_all_portfolio_models(200) if "CYC-001" in (m.get("run_label") or "") and not m.get("trade_log_json")]

# Check if trade_log_json column exists and is populated
print(f"Factor models needing trade log: {len(factor_models)}")
print(f"Portfolio models needing trade log: {len(portfolio_models)}")

# Actually just reseed all CYC-001 models regardless
all_factor   = [m for m in get_all_models(200) if "CYC-001" in (m.get("run_label") or "")]
all_portfolio = [m for m in get_all_portfolio_models(200) if "CYC-001" in (m.get("run_label") or "")]
print(f"Total CYC-001 factor: {len(all_factor)}, portfolio: {len(all_portfolio)}")

count = 0
for m in all_factor:
    score_col = m.get("score_column")
    label     = m.get("run_label", "")
    print(f"\n[FACTOR] {score_col}")
    try:
        cfg = FactorBacktestConfig(
            score_column=score_col, n_buckets=5, hold_months=6,
            rebalance_freq="semi-annual", cap_tier="all",
            run_label=label, **COMMON
        )
        r = run_factor_backtest(cfg, cb=_cb)
        if r.get("status") == "complete":
            tl = r.get("trade_log", [])
            sid = save_factor_model(r, overwrite=True)
            print(f"  OK: {sid} | {len(tl)} trade log entries")
            count += 1
        else:
            print(f"  ERR: {r.get('error')}")
    except Exception as e:
        traceback.print_exc()

for m in all_portfolio:
    score_col = m.get("score_column")
    label     = m.get("run_label", "")
    print(f"\n[PORTFOLIO] {score_col}")
    try:
        cfg = PortfolioBacktestConfig(
            score_column=score_col, top_n=20, sector_max=5,
            rebalance_freq="semi-annual", cap_tier="all",
            run_label=label, **COMMON
        )
        r = run_portfolio_backtest(cfg, cb=_cb)
        if r.get("status") == "complete":
            tl = r.get("trade_log", [])
            sid = save_portfolio_model(r, overwrite=True)
            print(f"  OK: {sid} | {len(tl)} trade log entries")
            count += 1
        else:
            print(f"  ERR: {r.get('error')}")
    except Exception as e:
        traceback.print_exc()

print(f"\nReseed complete: {count} models updated with trade logs")
