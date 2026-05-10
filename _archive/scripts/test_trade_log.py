"""Quick test: does the engine actually generate trade_log entries?"""
import sys; sys.path.insert(0,'.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig

def cb(msg):
    if 'trade' in msg.lower() or 'error' in msg.lower() or 'complete' in msg.lower():
        print(f"  {msg}")

r = run_factor_backtest(FactorBacktestConfig(
    score_column='jcn_qarp', n_buckets=5, hold_months=6,
    rebalance_freq='semi-annual', min_market_cap=10_000_000_000.0,
    min_price=5.0, min_adv_usd=1_000_000.0, transaction_cost_bps=15.0,
    start_date='2010-01-31', end_date='2012-12-31',  # short test
    run_label='test'
), cb=cb)

tl = r.get('trade_log', [])
print(f"\nStatus: {r.get('status')}")
print(f"Trade log entries: {len(tl)}")
if tl:
    print(f"First entry: {tl[0]}")
    print(f"Last entry:  {tl[-1]}")
else:
    print("EMPTY trade log - checking why...")
    print(f"  dates count: {len(r.get('dates', []))}")
    print(f"  n_obs: {r.get('n_obs')}")
