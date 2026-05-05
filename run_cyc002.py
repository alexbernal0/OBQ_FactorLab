# -*- coding: utf-8 -*-
"""CYC-002 Sequential batch with immediate flush — runs one factor at a time."""
import sys, time, os, traceback
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

from engine.cyc002_factors import CYC002_FACTORS
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model

COMMON = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
    cap_tier='all', min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
)
TAG = "[CYC-002-BASELINE]"

def cb(m):
    if any(k in m.lower() for k in ['complete','error','r3000','trade log']):
        print(f"    {m}", flush=True)

# Skip already-completed factors
existing = {m['score_column'] for m in get_all_models(500) if 'CYC-002' in (m.get('run_label') or '')}
print(f"Already done: {len(existing)} factors", flush=True)

results = []
errors  = []
t_start = time.time()
current_group = None

for i, (score_col, src_table, src_col, direction, display, group) in enumerate(CYC002_FACTORS):
    if score_col in existing:
        print(f"[{i+1:02d}/{len(CYC002_FACTORS)}] SKIP {display} (already in bank)", flush=True)
        continue

    if group != current_group:
        current_group = group
        print(f"\n{'='*65}", flush=True)
        print(f"GROUP: {group}", flush=True)
        print(f"{'='*65}", flush=True)

    print(f"\n[{i+1:02d}/{len(CYC002_FACTORS)}] {display} ({direction})", flush=True)
    t0 = time.time()
    row = {'score': score_col, 'display': display, 'group': group}

    # Factor backtest
    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score_col, score_direction=direction,
            run_label=f"{display} | 5Q | 6mo | Large-Cap | 1990-2024 {TAG}",
            **COMMON
        ), cb=cb)
        if r.get('status') == 'complete':
            sid = save_factor_model(r, overwrite=True)
            fm  = r['factor_metrics']
            row.update({'f_sid': sid, 'icir': fm.get('icir',0),
                        'spread': fm.get('quintile_spread_cagr',0)*100,
                        'staircase': fm.get('staircase_score',0)*100,
                        'q1_cagr': fm.get('q1_cagr',0)*100,
                        'bear': fm.get('bear_score',0)*100,
                        'bull': fm.get('bull_score',0)*100,
                        'fund': fm.get('obq_fund_score',0)})
            print(f"  FACTOR {sid} | ICIR={row['icir']:.3f} | Spread={row['spread']:.2f}% | Stair={row['staircase']:.2f}%", flush=True)
        else:
            print(f"  FACTOR ERR: {r.get('error','')[:80]}", flush=True)
            row['f_err'] = r.get('error','')
    except Exception as e:
        print(f"  FACTOR EXCEPTION: {str(e)[:80]}", flush=True)
        row['f_err'] = str(e)

    # Portfolio backtest
    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score_col, score_direction=direction,
            top_n=20, sector_max=5,
            run_label=f"{display} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 {TAG}",
            **{k:v for k,v in COMMON.items() if k not in ('n_buckets','hold_months')}
        ), cb=cb)
        if r2.get('status') == 'complete':
            sid2 = save_portfolio_model(r2, overwrite=True)
            pm   = r2['portfolio_metrics']
            row.update({'p_sid': sid2, 'cagr': pm.get('cagr',0)*100,
                        'sharpe': pm.get('sharpe',0), 'max_dd': pm.get('max_dd',0)*100})
            print(f"  PORT   {sid2} | CAGR={row['cagr']:.2f}% | Sharpe={row['sharpe']:.3f} | MaxDD={row['max_dd']:.1f}%", flush=True)
        else:
            print(f"  PORT   ERR: {r2.get('error','')[:80]}", flush=True)
            row['p_err'] = r2.get('error','')
    except Exception as e:
        print(f"  PORT   EXCEPTION: {str(e)[:80]}", flush=True)
        row['p_err'] = str(e)

    elapsed = round(time.time()-t0, 1)
    row['elapsed'] = elapsed
    print(f"  Done in {elapsed}s | Total elapsed: {round(time.time()-t_start)/60:.1f}min", flush=True)

    if 'f_err' not in row and 'p_err' not in row:
        results.append(row)
    else:
        errors.append(row)

# ── FINAL SUMMARY ─────────────────────────────────────────────────────────────
total = round(time.time()-t_start)
print(f"\n\n{'='*75}", flush=True)
print(f"CYC-002 COMPLETE: {len(results)} saved | {len(errors)} errors | {total//60}m{total%60}s", flush=True)
print(f"{'='*75}", flush=True)

print(f"\nFACTOR SIGNALS — sorted by ICIR", flush=True)
print(f"{'FACTOR':<42} {'GRP':>2} {'ICIR':>6} {'SPREAD':>8} {'STAIR':>7} {'BEAR':>7} {'FUND':>6}", flush=True)
print("-"*80, flush=True)
for r in sorted(results, key=lambda x: x.get('icir',0), reverse=True):
    g = r['group'][0] if r.get('group') else '?'
    print(f"  {r['display']:<40} {g:>2} {r.get('icir',0):>6.3f} {r.get('spread',0):>7.2f}% {r.get('staircase',0):>6.2f}% {r.get('bear',0):>6.2f}% {r.get('fund',0):>6.3f}", flush=True)

print(f"\nPORTFOLIO MODELS — sorted by Sharpe", flush=True)
print(f"{'FACTOR':<42} {'CAGR':>7} {'SHARPE':>7} {'MAX DD':>8}", flush=True)
print("-"*65, flush=True)
for r in sorted(results, key=lambda x: x.get('sharpe',0), reverse=True):
    print(f"  {r['display']:<40} {r.get('cagr',0):>6.2f}% {r.get('sharpe',0):>7.3f} {r.get('max_dd',0):>7.1f}%", flush=True)

if errors:
    print(f"\nERRORS:", flush=True)
    for e in errors:
        print(f"  {e['score']}: {e.get('f_err','')} | {e.get('p_err','')}", flush=True)

print("\nDONE.", flush=True)
