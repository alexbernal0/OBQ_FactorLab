# -*- coding: utf-8 -*-
import sys; sys.path.insert(0,'.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.strategy_bank import save_factor_model

def _cb(msg): print(f"  {msg}")

r = run_factor_backtest(FactorBacktestConfig(
    score_column="moat_score", score_direction="higher_better",
    n_buckets=5, hold_months=3, rebalance_freq="quarterly",
    min_price=5.0, min_adv_usd=1_000_000.0, min_market_cap=10_000_000_000.0,
    cap_tier="all", transaction_cost_bps=15.0,
    start_date="1990-07-31", end_date="2024-12-31",
    run_label="Moat Score (economic moat gates) | 5Q | 3mo | Large-Cap | 1990-2024 [CYC-001-BASELINE]",
), cb=_cb)

if r.get("status") == "complete":
    sid = save_factor_model(r, overwrite=True)
    fm = r.get("factor_metrics", {})
    print(f"Saved: {sid}")
    print(f"  ICIR={fm.get('icir',0):.3f}  Spread={fm.get('quintile_spread_cagr',0)*100:.2f}%")
else:
    print(f"ERROR: {r.get('error')}")
