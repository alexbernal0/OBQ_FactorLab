import sys, time; sys.path.insert(0,'.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
def cb(m):
    if any(k in m.lower() for k in ['complete','loaded','score','error']): print(f"  {m}")
t0 = time.time()
r = run_factor_backtest(FactorBacktestConfig(
    score_column='cyc2_mom_12m', score_direction='higher_better',
    start_date='2015-01-31', end_date='2020-12-31',
    n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
    cap_tier='all', min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0
), cb=cb)
if r.get('status') == 'complete':
    fm = r['factor_metrics']
    print(f"OK: ICIR={fm.get('icir',0):.3f} Spread={fm.get('quintile_spread_cagr',0)*100:.2f}% ({round(time.time()-t0)}s)")
else:
    print(f"ERROR: {r.get('error')}")
