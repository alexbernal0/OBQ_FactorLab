"""
Rerun saved bank models with full 1990-2024 history and save all new JSON fields.
Run once to migrate existing models.
"""
import sys
sys.path.insert(0, '.')

from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.strategy_bank import save_factor_model

RUNS = [
    dict(
        score_column="jcn_full_composite",
        n_buckets=5,
        hold_months=6,
        start_date="2005-01-31",
        end_date="2024-12-31",
        min_price=5.0,
        min_adv_usd=1_000_000,
        cap_tier="all",
        rebalance_freq="semi-annual",
        transaction_cost_bps=15.0,
        run_label="JCN Composite | 5Q | 6mo | All-Cap | 2005-2024 PROD-v1",
    ),
    dict(
        score_column="jcn_full_composite",
        n_buckets=5,
        hold_months=6,
        start_date="2010-01-31",
        end_date="2024-12-31",
        min_price=5.0,
        min_adv_usd=1_000_000,
        cap_tier="all",
        rebalance_freq="semi-annual",
        transaction_cost_bps=15.0,
        run_label="JCN Composite | 5Q | 6mo | All-Cap | 2010-2024",
    ),
]

for params in RUNS:
    print(f"\n=== {params['run_label']} ===")
    cfg = FactorBacktestConfig(**params)

    log_msgs = []
    def cb(level, msg):
        log_msgs.append(msg)
        print(f"  {msg}")

    result = run_factor_backtest(cfg, cb=cb)

    if result.get("status") == "complete":
        fm = result.get("factor_metrics", {})
        sid = save_factor_model(result, overwrite=True)
        dates = result.get("dates", [])
        tort = result.get("tortoriello", {})
        pd = result.get("period_data", [])
        print(f"\nSaved: {sid}")
        print(f"  ICIR         = {fm.get('icir', 0):.3f}")
        print(f"  Q1-Q5 Spread = {fm.get('quintile_spread_cagr', 0)*100:.2f}%")
        print(f"  Q1 CAGR      = {fm.get('q1_cagr', 0)*100:.2f}%")
        print(f"  Dates        = {dates[0] if dates else '?'} -> {dates[-1] if dates else '?'} ({len(dates)} periods)")
        print(f"  Tortoriello  = {list(tort.keys())} buckets")
        print(f"  Period data  = {len(pd)} rows")
        print(f"  Elapsed      = {result.get('elapsed_s')}s")
    else:
        print(f"ERROR: {result.get('error')}")
