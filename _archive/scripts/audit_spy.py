"""Pull SPY ground truth for Grok audit."""
import sys; sys.path.insert(0,'.')
import json, math
import numpy as np
import pandas as pd
import duckdb, os
from dotenv import load_dotenv; load_dotenv()

# ── 1. Pull raw SPY monthly returns directly from DB ─────────────────────────
MIRROR = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(MIRROR, read_only=True)

df = con.execute("""
    SELECT date::DATE AS dt, adjusted_close AS close
    FROM PROD_EOD_ETFs
    WHERE symbol = 'SPY.US'
      AND date::DATE BETWEEN '1993-01-01'::DATE AND '2024-12-31'::DATE
      AND adjusted_close IS NOT NULL AND adjusted_close > 0.01
    ORDER BY dt
""").fetchdf()
con.close()

df['dt'] = pd.to_datetime(df['dt'])
df = df.set_index('dt').sort_index()
monthly = df['close'].resample('ME').last().dropna()
monthly_ret = monthly.pct_change().dropna()
annual_ret  = monthly_ret.resample('YE').apply(lambda x: (1+x).prod()-1)

equity = (1 + monthly_ret).cumprod()
equity = pd.concat([pd.Series([1.0], index=[monthly.index[0]]), equity])

# ── 2. GIPS-standard manual calculations ─────────────────────────────────────
n_months = len(monthly_ret)
n_years  = n_months / 12.0
final_equity = float(equity.iloc[-1])

cagr         = final_equity ** (1.0 / n_years) - 1
total_return = final_equity - 1
ann_vol      = float(monthly_ret.std() * np.sqrt(12))
sharpe       = (cagr - 0.04) / ann_vol if ann_vol > 0 else 0
sortino_denom = float(monthly_ret[monthly_ret < 0].std() * np.sqrt(12))
sortino      = (cagr - 0.04) / sortino_denom if sortino_denom > 0 else 0

# Max drawdown
roll_max = equity.cummax()
dd       = (equity / roll_max) - 1
max_dd   = float(dd.min())
calmar   = cagr / abs(max_dd) if max_dd != 0 else 0

win_rate = float((monthly_ret > 0).mean())
best_mo  = float(monthly_ret.max())
worst_mo = float(monthly_ret.min())

print("=" * 60)
print("SPY GROUND TRUTH — GIPS MANUAL CALCULATIONS")
print("=" * 60)
print(f"Period:          {str(equity.index[0].date())} to {str(equity.index[-1].date())}")
print(f"N months:        {n_months}")
print(f"N years:         {n_years:.4f}")
print(f"Final equity:    {final_equity:.6f}")
print()
print(f"CAGR:            {cagr*100:.4f}%")
print(f"Total Return:    {total_return*100:.2f}%")
print(f"Ann Volatility:  {ann_vol*100:.4f}%")
print(f"Sharpe (rf=4%):  {sharpe:.6f}")
print(f"Sortino (rf=4%): {sortino:.6f}")
print(f"Max Drawdown:    {max_dd*100:.4f}%")
print(f"Calmar:          {calmar:.6f}")
print(f"Win Rate (Mo):   {win_rate*100:.2f}%")
print(f"Best Month:      {best_mo*100:.2f}%")
print(f"Worst Month:     {worst_mo*100:.2f}%")
print()
print("ANNUAL RETURNS:")
for yr, r in annual_ret.items():
    print(f"  {yr.year}: {r*100:.2f}%")

# ── 3. Now run our engine and compare ────────────────────────────────────────
print()
print("=" * 60)
print("OBQ ENGINE OUTPUT vs MANUAL")
print("=" * 60)
from engine.spy_backtest import run_spy_backtest
res = run_spy_backtest(start_date='1993-01-29', end_date='2024-12-31')
m = res.get('portfolio_metrics', {})

checks = [
    ("CAGR",          cagr,    m.get('cagr',0)),
    ("Ann Vol",       ann_vol, m.get('ann_vol',0)),
    ("Sharpe",        sharpe,  m.get('sharpe',0)),
    ("Sortino",       sortino, m.get('sortino',0)),
    ("Max DD",        max_dd,  m.get('max_dd',0)),
    ("Calmar",        calmar,  m.get('calmar',0)),
    ("Win Rate Mo",   win_rate,m.get('win_rate_monthly',0)),
    ("Best Month",    best_mo, m.get('best_month',0)),
    ("Worst Month",   worst_mo,m.get('worst_month',0)),
]

print(f"{'METRIC':<20} {'MANUAL':>12} {'ENGINE':>12} {'DIFF':>10} {'STATUS'}")
print("-" * 70)
for name, manual, engine in checks:
    diff = abs(manual - engine) if manual and engine else 0
    pct_diff = (diff / abs(manual) * 100) if manual else 0
    status = "PASS" if pct_diff < 0.1 else ("WARN" if pct_diff < 1.0 else "FAIL")
    print(f"{name:<20} {manual*100:>11.4f}% {engine*100:>11.4f}% {pct_diff:>9.4f}%  {status}")
