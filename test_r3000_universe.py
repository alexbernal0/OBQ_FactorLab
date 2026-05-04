# -*- coding: utf-8 -*-
"""Test Russell 3000 universe benchmark in factor backtest."""
import sys; sys.path.insert(0,'.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig

msgs = []
def cb(msg):
    msgs.append(msg)
    if any(k in msg.lower() for k in ['universe', 'russell', 'r3000', 'complete', 'error']):
        print(f"  {msg}")

r = run_factor_backtest(FactorBacktestConfig(
    score_column='jcn_qarp', n_buckets=5, hold_months=6,
    rebalance_freq='semi-annual',
    min_market_cap=10_000_000_000.0,
    min_price=5.0, min_adv_usd=1_000_000.0,
    transaction_cost_bps=15.0,
    start_date='2000-01-31', end_date='2024-12-31',
    run_label='test R3000'
), cb=cb)

if r.get('status') == 'complete':
    um = r.get('universe_metrics', {})
    fm = r.get('factor_metrics', {})
    print(f"\nQ1 CAGR: {fm.get('q1_cagr',0)*100:.2f}%")
    print(f"Universe CAGR: {um.get('cagr',0)*100:.2f}%  (should be ~10-12% for R3000)")
    print(f"Q1-Q5 Spread: {fm.get('quintile_spread_cagr',0)*100:.2f}%")
    print(f"ICIR: {fm.get('icir',0):.3f}")
    
    # Check tort data has meaningful universe values
    tort = r.get('tortoriello', {})
    t1 = tort.get('1', {})
    print(f"\nTortoriello Q1 avg_excess_vs_univ: {t1.get('avg_excess_vs_univ',0)*100:.2f}%")
    print(f"  (this should now be excess vs R3000, not scored-stock EW)")
else:
    print(f"ERROR: {r.get('error')}")
