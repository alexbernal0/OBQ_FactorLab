"""Verify the spliced SPX total return benchmark."""
import sys; sys.path.insert(0,'.')
from engine.spx_backtest import run_spx_backtest
from engine.spy_backtest import run_spy_backtest

def cb(msg): print(f"  {msg}")

print("=== SPX TOTAL RETURN BENCHMARK (1990-2024) ===")
r_spx = run_spx_backtest(start_date='1990-07-31', end_date='2024-12-31', cb=cb)
m = r_spx['portfolio_metrics']
ann = r_spx['annual_ret_by_year']

print(f"\nSPX CAGR 1990-2024:  {m['cagr']*100:.2f}%")
print(f"SPX Sharpe:          {m['sharpe']:.3f}")
print(f"SPX Max DD:          {m['max_dd']*100:.1f}%")
print(f"SPX Terminal ($10K): ${m['terminal_wealth']:,.0f}")

print("\nAnnual returns (SPX splice vs SPY for overlap):")
print(f"{'YEAR':<6} {'SPX TOTAL':>10}")
for a in ann:
    if 1990 <= a['year'] <= 2000:
        r = a['ret']*100
        print(f"  {a['year']}  {r:>8.2f}%")

print("\n=== SPY ONLY (1993-2024 for comparison) ===")
r_spy = run_spy_backtest(start_date='1993-01-29', end_date='2024-12-31')
ms = r_spy['portfolio_metrics']
print(f"SPY CAGR 1993-2024:  {ms['cagr']*100:.2f}%")
print(f"SPY Sharpe:          {ms['sharpe']:.3f}")
print()
print("SPX total return vs known published data (1990-1992):")
KNOWN_TOTAL = {1990:-3.10, 1991:30.47, 1992:7.62}
for a in ann:
    if a['year'] in KNOWN_TOTAL:
        our = a['ret']*100
        known = KNOWN_TOTAL[a['year']]
        diff = abs(our - known)
        print(f"  {a['year']}: ours={our:.2f}%  known={known:.2f}%  diff={diff:.2f}%  {'OK' if diff < 1.5 else 'CHECK'}")
