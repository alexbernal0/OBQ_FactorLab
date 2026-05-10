"""
Seed PM-Cycle-001: JCN Composite Top-20, Quarterly, 5/sector max, 1990-2024, no stop.
"""
import sys
sys.path.insert(0, '.')

from engine.portfolio_backtest import PortfolioBacktestConfig, run_portfolio_backtest
from engine.portfolio_bank import save_portfolio_model

cfg = PortfolioBacktestConfig(
    score_column        = "jcn_full_composite",
    score_direction     = "higher_better",
    top_n               = 20,
    sector_max          = 5,
    rebalance_freq      = "quarterly",
    start_date          = "1990-07-31",
    end_date            = "2024-12-31",
    min_price           = 5.0,
    min_adv_usd         = 1_000_000.0,
    cap_tier            = "all",
    transaction_cost_bps= 15.0,
    stop_loss_pct       = 0.0,
    weight_scheme       = "equal",
    run_label           = "JCN Top-20 | Quarterly | 5/Sector | 1990-2024 | No Stop [PM-Cycle-001]",
)

print("=" * 60)
print("Seeding PM-Cycle-001")
print(f"  Score:      {cfg.score_column}")
print(f"  Top-N:      {cfg.top_n}")
print(f"  Sector cap: {cfg.sector_max}")
print(f"  Rebalance:  {cfg.rebalance_freq}")
print(f"  Period:     {cfg.start_date} to {cfg.end_date}")
print(f"  Stop loss:  None")
print("=" * 60)

def cb(msg):
    print(f"  {msg}")

result = run_portfolio_backtest(cfg, cb=cb)

if result.get("status") == "complete":
    sid = save_portfolio_model(result, overwrite=True)
    pm  = result.get("portfolio_metrics", {})
    print(f"\nSaved: {sid}")
    print(f"  CAGR        = {(pm.get('cagr',0)*100):.2f}%")
    print(f"  Sharpe      = {pm.get('sharpe',0):.3f}")
    print(f"  Max DD      = {(pm.get('max_dd',0)*100):.2f}%")
    print(f"  Calmar      = {pm.get('calmar',0):.3f}")
    print(f"  Win Mo%     = {(pm.get('win_rate_monthly',0)*100):.1f}%")
    print(f"  Surefire    = {pm.get('surefire_ratio',0):.1f}")
    print(f"  Periods     = {result.get('n_periods')}")
    print(f"  Elapsed     = {result.get('elapsed_s')}s")
    spy = result.get("spy_metrics", {})
    if spy:
        print(f"\n  SPY CAGR    = {(spy.get('cagr',0)*100):.2f}%")
        print(f"  SPY Sharpe  = {spy.get('sharpe',0):.3f}")
else:
    print(f"\nERROR: {result.get('error')}")
