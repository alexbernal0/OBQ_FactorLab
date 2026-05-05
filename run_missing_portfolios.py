# -*- coding: utf-8 -*-
"""Run the 2 missing portfolio models for Fundsmith factors."""
import sys, time
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.portfolio_bank import save_portfolio_model

COMMON_P = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    rebalance_freq='semi-annual', cap_tier='all',
    min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
    top_n=20, sector_max=5,
)
TAG = "[CYC-002-BASELINE]"

def cb(m): print(f"  {m}", flush=True)

missing = [
    ('cyc2_cash_conv', 'higher_better', 'Cash Conversion (Fundsmith)'),
    ('cyc2_nd_ebit',   'lower_better',  'Net Debt / EBIT (Fundsmith)'),
]

for score_col, direction, display in missing:
    print(f"\nRunning portfolio: {display} ({score_col})", flush=True)
    t0 = time.time()
    r = run_portfolio_backtest(PortfolioBacktestConfig(
        score_column=score_col,
        score_direction=direction,
        run_label=f"{display} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 {TAG}",
        **COMMON_P
    ), cb=cb)
    if r.get('status') == 'complete':
        sid = save_portfolio_model(r, overwrite=True)
        pm = r['portfolio_metrics']
        print(f"  OK sid={sid} CAGR={pm.get('cagr',0)*100:.2f}% Sharpe={pm.get('sharpe',0):.3f} ({round(time.time()-t0)}s)", flush=True)
    else:
        print(f"  ERR: {r.get('error','')}", flush=True)

print("\nDone.", flush=True)
