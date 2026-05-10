# -*- coding: utf-8 -*-
"""Run all remaining/broken CYC-002 factors and final summary."""
import sys, time, traceback
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

from engine.cyc002_factors import CYC002_FACTORS
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model, get_all_portfolio_models

COMMON = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
    cap_tier='all', min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
)
TAG = "[CYC-002-BASELINE]"

def cb(m):
    if any(k in m.lower() for k in ['complete','error','r3000','trade log']): print(f"    {m}", flush=True)

# Get current state
all_f  = get_all_models(500)
cyc2_done = {m['score_column'] for m in all_f if 'CYC-002' in (m.get('run_label') or '')}
# Also check for ones with negative ICIR that need rerun (direction was wrong)
cyc2_bad = {m['score_column'] for m in all_f
            if 'CYC-002' in (m.get('run_label') or '') and (m.get('icir') or 0) < -0.1}

print(f"Done: {len(cyc2_done)} | Bad ICIR (need rerun): {len(cyc2_bad)}", flush=True)
if cyc2_bad:
    print(f"Bad: {cyc2_bad}", flush=True)

# Factors to run: missing + bad
to_run = [(s,t,c,d,n,g) for s,t,c,d,n,g in CYC002_FACTORS
          if s not in cyc2_done or s in cyc2_bad]
print(f"Factors to run: {len(to_run)}", flush=True)

results = []
t_start = time.time()

for i, (score_col, src_table, src_col, direction, display, group) in enumerate(to_run):
    print(f"\n[{i+1}/{len(to_run)}] {display} ({direction})", flush=True)
    t0 = time.time()
    row = {'score': score_col, 'display': display, 'group': group}

    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score_col, score_direction=direction,
            run_label=f"{display} | 5Q | 6mo | Large-Cap | 1990-2024 {TAG}", **COMMON
        ), cb=cb)
        if r.get('status') == 'complete':
            sid = save_factor_model(r, overwrite=True)
            fm = r['factor_metrics']
            row.update({'icir': fm.get('icir',0), 'spread': fm.get('quintile_spread_cagr',0)*100,
                        'staircase': fm.get('staircase_score',0)*100,
                        'q1_cagr': fm.get('q1_cagr',0)*100,
                        'bear': fm.get('bear_score',0)*100,
                        'bull': fm.get('bull_score',0)*100,
                        'fund': fm.get('obq_fund_score',0)})
            print(f"  FACTOR {sid} ICIR={row['icir']:.3f} Spread={row['spread']:.2f}%", flush=True)
        else:
            print(f"  FACTOR ERR: {r.get('error','')[:100]}", flush=True)
    except Exception as e:
        print(f"  FACTOR EX: {str(e)[:100]}", flush=True)
        traceback.print_exc()

    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score_col, score_direction=direction,
            top_n=20, sector_max=5,
            run_label=f"{display} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 {TAG}",
            **{k:v for k,v in COMMON.items() if k not in ('n_buckets','hold_months')}
        ), cb=cb)
        if r2.get('status') == 'complete':
            sid2 = save_portfolio_model(r2, overwrite=True)
            pm = r2['portfolio_metrics']
            row.update({'sharpe': pm.get('sharpe',0), 'cagr': pm.get('cagr',0)*100,
                        'max_dd': pm.get('max_dd',0)*100})
            print(f"  PORT   {sid2} CAGR={row['cagr']:.2f}% Sharpe={row['sharpe']:.3f}", flush=True)
        else:
            print(f"  PORT   ERR: {r2.get('error','')[:100]}", flush=True)
    except Exception as e:
        print(f"  PORT   EX: {str(e)[:100]}", flush=True)

    row['elapsed'] = round(time.time()-t0)
    results.append(row)
    elapsed_total = round(time.time()-t_start)
    remaining = len(to_run) - i - 1
    eta = remaining * (elapsed_total/(i+1))
    print(f"  {row['elapsed']}s | Total: {elapsed_total//60}m | ETA: {eta//60:.0f}m", flush=True)

# ── COMPREHENSIVE FINAL SUMMARY ────────────────────────────────────────────────
print(f"\n{'='*75}", flush=True)
print(f"CYC-002 FINISH RUN COMPLETE: {len(results)} runs", flush=True)
print(f"{'='*75}", flush=True)

# Load ALL CYC-002 results from bank for complete picture
all_cyc2 = [m for m in get_all_models(500) if 'CYC-002' in (m.get('run_label') or '')]
# Deduplicate by score_column — keep highest ICIR
best_per_score = {}
for m in all_cyc2:
    sc = m['score_column']
    if sc not in best_per_score or (m.get('icir') or 0) > (best_per_score[sc].get('icir') or 0):
        best_per_score[sc] = m

print(f"\nALL CYC-002 FACTORS (best run per score, sorted by ICIR):", flush=True)
print(f"{'FACTOR':<35} {'ICIR':>6} {'SPREAD':>8} {'Q1CAGR':>8} {'FUND':>6} {'GROUP'}", flush=True)
print("-"*75, flush=True)
for sc, m in sorted(best_per_score.items(), key=lambda x: x[1].get('icir') or -99, reverse=True):
    label = (m.get('run_label') or '').split('|')[0].strip()[:32]
    icir  = m.get('icir') or 0
    spread = (m.get('quintile_spread_cagr') or 0)*100
    q1    = (m.get('q1_cagr') or 0)*100
    fund  = m.get('obq_fund_score') or 0
    print(f"  {label:<33} {icir:>6.3f} {spread:>7.2f}% {q1:>7.2f}% {fund:>6.3f}", flush=True)

print("\nDONE.", flush=True)
