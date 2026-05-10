# -*- coding: utf-8 -*-
"""
run_cyc002_finish.py
====================
Completes CYC-002:
  1. Runs the 6 remaining momentum factors (symbol join now fixed)
  2. Runs all 19 two-factor combos (new rank-averaging engine)

Usage:
  python run_cyc002_finish.py            # everything
  python run_cyc002_finish.py --momentum # momentum only
  python run_cyc002_finish.py --combos   # combos only
"""
import sys, time, traceback, argparse
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

from engine.cyc002_factors import CYC002_FACTORS, CYC002_COMBOS
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig, run_combo_backtest
from engine.portfolio_backtest import (
    run_portfolio_backtest, PortfolioBacktestConfig, run_combo_portfolio_backtest
)
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model, get_all_portfolio_models

# ── CLI ─────────────────────────────────────────────────────────────────────────
ap = argparse.ArgumentParser()
ap.add_argument("--momentum", action="store_true", help="Run momentum singles only")
ap.add_argument("--combos",   action="store_true", help="Run combos only")
args = ap.parse_args()
run_momentum = args.momentum or (not args.momentum and not args.combos)
run_combos   = args.combos   or (not args.momentum and not args.combos)

# ── Config ──────────────────────────────────────────────────────────────────────
COMMON_F = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
    cap_tier='all', min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
)
COMMON_P = {k: v for k, v in COMMON_F.items()
            if k not in ('n_buckets', 'hold_months')}
COMMON_P.update(top_n=20, sector_max=5)

TAG_F = "[CYC-002-BASELINE]"
TAG_C = "[CYC-002-COMBO]"

def cb(m):
    if any(k in m.lower() for k in ['complete','error','r3000','trade log','loaded','period','merged','dates']):
        print(f"    {m}", flush=True)

# ── Current state ────────────────────────────────────────────────────────────────
all_f        = get_all_models(1000)
cyc2_done_f  = {m['score_column'] for m in all_f if 'CYC-002' in (m.get('run_label') or '')}
all_p        = get_all_portfolio_models(1000)
cyc2_done_p  = {m['score_column'] for m in all_p if 'CYC-002' in (m.get('run_label') or '')}

mom_factor_ids = {'cyc2_mom_3m','cyc2_mom_6m','cyc2_mom_12m','cyc2_fip_6m','cyc2_fip_12m','cyc2_sys_score'}
combo_ids_done = {m['score_column'] for m in all_f if 'CYC-002-COMBO' in (m.get('run_label') or '')}

print(f"State: {len(cyc2_done_f)} factor models | {len(cyc2_done_p)} portfolio models", flush=True)
print(f"Momentum done: {cyc2_done_f & mom_factor_ids}", flush=True)
print(f"Combos done:   {combo_ids_done}", flush=True)

results_summary = []
t_global = time.time()

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1: Momentum factors
# ═══════════════════════════════════════════════════════════════════════════════
if run_momentum:
    mom_factors = [(s,t,c,d,n,g) for s,t,c,d,n,g in CYC002_FACTORS if s in mom_factor_ids]
    missing_mom = [(s,t,c,d,n,g) for s,t,c,d,n,g in mom_factors if s not in cyc2_done_f]
    print(f"\n{'='*70}", flush=True)
    print(f"PHASE 1: MOMENTUM FACTORS — {len(missing_mom)} missing of {len(mom_factors)}", flush=True)
    print(f"{'='*70}", flush=True)

    for i, (score_col, src_table, src_col, direction, display, group) in enumerate(missing_mom):
        print(f"\n[M{i+1}/{len(missing_mom)}] {display} ({direction})", flush=True)
        t0 = time.time()
        row = {'id': score_col, 'display': display, 'type': 'momentum'}

        try:
            r = run_factor_backtest(FactorBacktestConfig(
                score_column=score_col, score_direction=direction,
                run_label=f"{display} | 5Q | 6mo | Large-Cap | 1990-2024 {TAG_F}",
                **COMMON_F
            ), cb=cb)
            if r.get('status') == 'complete':
                sid = save_factor_model(r, overwrite=True)
                fm  = r['factor_metrics']
                row.update(icir=fm.get('icir',0), spread=fm.get('quintile_spread_cagr',0)*100,
                           q1_cagr=fm.get('q1_cagr',0)*100, fund=fm.get('obq_fund_score',0))
                print(f"  FACTOR OK  sid={sid} ICIR={row['icir']:.3f} Spread={row['spread']:.2f}%", flush=True)
            else:
                row['error'] = r.get('error','')[:80]
                print(f"  FACTOR ERR: {row['error']}", flush=True)
        except Exception as e:
            row['error'] = str(e)[:80]
            print(f"  FACTOR EX: {row['error']}", flush=True)
            traceback.print_exc()

        try:
            r2 = run_portfolio_backtest(PortfolioBacktestConfig(
                score_column=score_col, score_direction=direction,
                run_label=f"{display} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 {TAG_F}",
                **COMMON_P
            ), cb=cb)
            if r2.get('status') == 'complete':
                sid2 = save_portfolio_model(r2, overwrite=True)
                pm   = r2['portfolio_metrics']
                row.update(cagr=pm.get('cagr',0)*100, sharpe=pm.get('sharpe',0),
                           max_dd=pm.get('max_dd',0)*100)
                print(f"  PORT   OK  sid={sid2} CAGR={row['cagr']:.2f}% Sharpe={row['sharpe']:.3f}", flush=True)
            else:
                print(f"  PORT   ERR: {r2.get('error','')[:80]}", flush=True)
        except Exception as e:
            print(f"  PORT   EX: {str(e)[:80]}", flush=True)

        row['elapsed'] = round(time.time()-t0)
        results_summary.append(row)
        total_e = round(time.time()-t_global)
        print(f"  {row['elapsed']}s | Total elapsed: {total_e//60}m{total_e%60}s", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Two-factor combos
# ═══════════════════════════════════════════════════════════════════════════════
if run_combos:
    missing_combos = [(cid,fa,fb,dn,src) for cid,fa,fb,dn,src in CYC002_COMBOS
                      if f"combo_{cid}" not in combo_ids_done]
    print(f"\n{'='*70}", flush=True)
    print(f"PHASE 2: TWO-FACTOR COMBOS — {len(missing_combos)} missing of {len(CYC002_COMBOS)}", flush=True)
    print(f"{'='*70}", flush=True)

    base_cfg_f = FactorBacktestConfig(**COMMON_F)
    # COMMON_P already contains start_date, end_date, rebalance_freq etc.
    # (it's COMMON_F minus n_buckets/hold_months + top_n/sector_max)
    base_cfg_p = PortfolioBacktestConfig(**COMMON_P)

    for i, (combo_id, fa, fb, display, source) in enumerate(missing_combos):
        print(f"\n[C{i+1}/{len(missing_combos)}] {combo_id}: {display} ({source})", flush=True)
        t0 = time.time()
        row = {'id': f"combo_{combo_id}", 'display': display, 'type': 'combo'}

        # Factor quintile run
        try:
            r = run_combo_backtest(
                combo_id=combo_id, factor_a_id=fa, factor_b_id=fb,
                display_name=display, source=source,
                cfg_override=base_cfg_f, cb=cb
            )
            if r.get('status') == 'complete':
                sid = save_factor_model(r, overwrite=True)
                fm  = r['factor_metrics']
                row.update(icir=fm.get('icir',0), spread=fm.get('quintile_spread_cagr',0)*100,
                           q1_cagr=fm.get('q1_cagr',0)*100, fund=fm.get('obq_fund_score',0))
                print(f"  COMBO FACTOR OK  sid={sid} ICIR={row['icir']:.3f} Spread={row['spread']:.2f}%", flush=True)
            else:
                row['error'] = r.get('error','')[:80]
                print(f"  COMBO FACTOR ERR: {row['error']}", flush=True)
        except Exception as e:
            row['error'] = str(e)[:80]
            print(f"  COMBO FACTOR EX: {row['error']}", flush=True)
            traceback.print_exc()

        # Portfolio run
        try:
            r2 = run_combo_portfolio_backtest(
                combo_id=combo_id, factor_a_id=fa, factor_b_id=fb,
                display_name=display, source=source,
                cfg=base_cfg_p, cb=cb
            )
            if r2.get('status') == 'complete':
                sid2 = save_portfolio_model(r2, overwrite=True)
                pm   = r2['portfolio_metrics']
                row.update(cagr=pm.get('cagr',0)*100, sharpe=pm.get('sharpe',0),
                           max_dd=pm.get('max_dd',0)*100)
                print(f"  COMBO PORT   OK  sid={sid2} CAGR={row['cagr']:.2f}% Sharpe={row['sharpe']:.3f}", flush=True)
            else:
                print(f"  COMBO PORT   ERR: {r2.get('error','')[:80]}", flush=True)
        except Exception as e:
            print(f"  COMBO PORT   EX: {str(e)[:80]}", flush=True)
            traceback.print_exc()

        row['elapsed'] = round(time.time()-t0)
        results_summary.append(row)
        total_e = round(time.time()-t_global)
        remaining = len(missing_combos) - i - 1
        eta = remaining * (total_e / (i+1)) if i > 0 else 0
        print(f"  {row['elapsed']}s | Total: {total_e//60}m | ETA: {eta//60:.0f}m", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}", flush=True)
total_elapsed = round(time.time() - t_global)
print(f"CYC-002 FINISH COMPLETE — {len(results_summary)} runs in {total_elapsed//60}m{total_elapsed%60}s", flush=True)
print(f"{'='*70}", flush=True)

ok  = [r for r in results_summary if 'error' not in r]
err = [r for r in results_summary if 'error' in r]

print(f"\n  Successful: {len(ok)}", flush=True)
print(f"  Errors:     {len(err)}", flush=True)

if ok:
    print(f"\n  {'TYPE':<8} {'ID':<22} {'ICIR':>6} {'SPREAD':>8} {'CAGR':>8} {'SHARPE':>7}", flush=True)
    print(f"  {'-'*60}", flush=True)
    for r in sorted(ok, key=lambda x: x.get('icir',0), reverse=True):
        print(f"  {r['type']:<8} {r['id']:<22} {r.get('icir',0):>6.3f} "
              f"{r.get('spread',0):>7.2f}% {r.get('cagr',0):>7.2f}% "
              f"{r.get('sharpe',0):>7.3f}", flush=True)

if err:
    print(f"\n  ERRORS:", flush=True)
    for r in err:
        print(f"    {r['id']}: {r.get('error','?')}", flush=True)

# Load full CYC-002 picture from bank
print(f"\n{'='*70}", flush=True)
print("COMPLETE CYC-002 FACTOR BANK SUMMARY", flush=True)
print(f"{'='*70}", flush=True)
all_cyc2 = [m for m in get_all_models(1000) if 'CYC-002' in (m.get('run_label') or '')]
best = {}
for m in all_cyc2:
    sc = m['score_column']
    if sc not in best or (m.get('icir') or 0) > (best[sc].get('icir') or 0):
        best[sc] = m

print(f"\n{'FACTOR':<40} {'ICIR':>6} {'SPREAD':>8} {'Q1CAGR':>8} {'FUND':>7}", flush=True)
print("-"*70, flush=True)
for sc, m in sorted(best.items(), key=lambda x: x[1].get('icir') or -99, reverse=True):
    label = (m.get('run_label') or '').split('|')[0].strip()[:38]
    icir   = m.get('icir') or 0
    spread = (m.get('quintile_spread_cagr') or 0) * 100
    q1     = (m.get('q1_cagr') or 0) * 100
    fund   = m.get('obq_fund_score') or 0
    tag    = "[COMBO]" if "COMBO" in (m.get('run_label') or '') else ""
    print(f"  {label:<38} {icir:>6.3f} {spread:>7.2f}% {q1:>7.2f}% {fund:>7.3f} {tag}", flush=True)

print(f"\nTotal CYC-002 models: {len(best)}", flush=True)
print("DONE.", flush=True)
