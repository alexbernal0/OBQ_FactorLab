import sys; sys.path.insert(0,'.')
from engine.spy_backtest import run_spy_backtest

# Test 1: pre-1993 start should auto-route to SPX splice
r = run_spy_backtest(start_date='1990-07-31', end_date='2024-12-31')
m = r['portfolio_metrics']
print("1990 start (auto-routes to SPX splice):")
print(f"  Label:  {r['run_label']}")
print(f"  CAGR:   {m['cagr']*100:.2f}%")
print(f"  Sharpe: {m['sharpe']:.3f}")
print(f"  MaxDD:  {m['max_dd']*100:.1f}%")
print(f"  Start:  {r['start_date']}")

# Test 2: post-1993 start stays as SPY
r2 = run_spy_backtest(start_date='1995-01-01', end_date='2024-12-31')
m2 = r2['portfolio_metrics']
print("\n1995 start (stays as SPY):")
print(f"  Label:  {r2['run_label'][:40]}")
print(f"  CAGR:   {m2['cagr']*100:.2f}%")
print(f"  Sharpe: {m2['sharpe']:.3f}")
