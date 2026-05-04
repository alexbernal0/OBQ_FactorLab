import sys; sys.path.insert(0,'.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.strategy_bank import save_factor_model

def cb(msg):
    if 'OBQ Fund' in msg or 'SPY' in msg or 'Error' in msg:
        print(f"  {msg}")

r = run_factor_backtest(FactorBacktestConfig(
    score_column='jcn_qarp', n_buckets=5, hold_months=6,
    rebalance_freq='semi-annual', min_market_cap=10_000_000_000.0,
    min_price=5.0, min_adv_usd=1_000_000.0, transaction_cost_bps=15.0,
    start_date='1990-07-31', end_date='2024-12-31',
    run_label='QARP SPY test'
), cb=cb)

if r.get('status') == 'complete':
    fm = r['factor_metrics']
    print(f"\nQARP Results (vs SPY):")
    print(f"  OBQ Fund Score:     {fm['obq_fund_score']:.4f}")
    print(f"  Alpha Win Rate:     {fm['alpha_win_rate']*100:.1f}%")
    print(f"  Avg Annual Alpha:   {fm['avg_annual_alpha']*100:.2f}%")
    print(f"  Alpha Sharpe:       {fm['alpha_sharpe']:.3f}")
    q1_cagr = fm.get('q1_cagr', 0)
    print(f"  Q1 CAGR:            {q1_cagr*100:.2f}%")
    print(f"  SPY CAGR:           {r['spy_metrics'].get('cagr',0)*100:.2f}%")
    print(f"  Excess vs SPY:      {(q1_cagr - r['spy_metrics'].get('cagr',0))*100:.2f}%/yr")
else:
    print(f"ERROR: {r.get('error')}")
