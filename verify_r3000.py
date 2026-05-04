# -*- coding: utf-8 -*-
"""Verify R3000 universe returns make sense."""
import sys; sys.path.insert(0,'.')
import duckdb, os, numpy as np
from dotenv import load_dotenv; load_dotenv()
from engine.factor_backtest import _load_russell3000_universe_returns, FactorBacktestConfig, _get_con

cfg = FactorBacktestConfig(
    start_date='1990-07-31', end_date='2024-12-31',
    hold_months=6, transaction_cost_bps=0
)
# Semi-annual dates
con = _get_con()
dates_rows = con.execute("""
    SELECT DISTINCT month_date::VARCHAR as d FROM v_backtest_scores
    WHERE month_date >= '1990-07-31' AND month_date <= '2024-12-31'
    AND MONTH(month_date) IN (6,12) AND jcn_full_composite IS NOT NULL
    ORDER BY d
""").fetchall()
dates = [r[0] for r in dates_rows]

df = _load_russell3000_universe_returns(con, cfg, dates)
con.close()

print(f"R3000 periods loaded: {len(df)}")
print(f"Date range: {df['month_date'].min()} to {df['month_date'].max()}")
print(f"Avg members per period: {df['n_members'].mean():.0f}")
print(f"Min members: {df['n_members'].min()} | Max: {df['n_members'].max()}")
print()

# Compute CAGR from R3000 EW
rets = df['r3000_ret'].values
eq   = np.concatenate([[1.0], np.cumprod(1 + rets)])
n_years = len(rets) / 2  # semi-annual
cagr = eq[-1] ** (1/n_years) - 1
print(f"R3000 EW CAGR ({df['n_members'].mean():.0f} avg members): {cagr*100:.2f}%")
print(f"R3000 EW Final equity: {eq[-1]:.2f}x")
print()
print("Sample periods:")
print(df[['month_date','r3000_ret','n_members']].head(10).to_string(index=False))
print("...")
print(df[['month_date','r3000_ret','n_members']].tail(5).to_string(index=False))

# Compare to SPX
from engine.spy_backtest import run_spy_backtest
spy = run_spy_backtest(start_date='1990-07-31', end_date='2024-12-31')
print(f"\nSPX CAGR (cap-weight): {spy['portfolio_metrics']['cagr']*100:.2f}%")
print(f"R3000 EW CAGR:         {cagr*100:.2f}%")
print(f"Difference:            {(cagr - spy['portfolio_metrics']['cagr'])*100:.2f}%")
print()
print("Expected: R3000 EW slightly above or below SPX due to size tilt")
print("If R3000 EW >> SPX cap-weight: small-cap premium embedded in benchmark (correct)")
