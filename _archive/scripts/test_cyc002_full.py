"""Quick end-to-end test of one CYC-002 factor (short date range)."""
import sys, time; sys.path.insert(0,'.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig

QUICK = dict(start_date='2015-01-31', end_date='2022-12-31',
             n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
             cap_tier='all', min_price=5.0, min_adv_usd=1_000_000.0,
             min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0)

def cb(m):
    if any(k in m.lower() for k in ['complete','error','r3000']): print(f"  {m}")

print("Testing cyc2_int_cov (Interest Coverage)...")
t0 = time.time()
r = run_factor_backtest(FactorBacktestConfig(
    score_column='cyc2_int_cov', score_direction='higher',
    run_label='test', **QUICK
), cb=cb)
if r.get('status') == 'complete':
    fm = r['factor_metrics']
    print(f"FACTOR OK: ICIR={fm.get('icir',0):.3f} spread={fm.get('quintile_spread_cagr',0)*100:.2f}% ({round(time.time()-t0,1)}s)")
else:
    print(f"FACTOR ERROR: {r.get('error')}")

print("\nTesting portfolio cyc2_int_cov...")
t0 = time.time()
r2 = run_portfolio_backtest(PortfolioBacktestConfig(
    score_column='cyc2_int_cov', score_direction='higher',
    top_n=20, sector_max=5, run_label='test',
    **{k:v for k,v in QUICK.items() if k not in ('n_buckets','hold_months')}
), cb=cb)
if r2.get('status') == 'complete':
    pm = r2['portfolio_metrics']
    print(f"PORTFOLIO OK: CAGR={pm.get('cagr',0)*100:.2f}% Sharpe={pm.get('sharpe',0):.3f} ({round(time.time()-t0,1)}s)")
else:
    print(f"PORTFOLIO ERROR: {r2.get('error')}")
