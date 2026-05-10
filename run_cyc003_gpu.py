# -*- coding: utf-8 -*-
"""
run_cyc003_gpu.py — CYC-003 Full Universe GPU Run
==================================================
Entry point for the complete CYC-003 factor backtest batch.

Architecture:
  1. load_all_data()    — ONE DuckDB query, all 91 score columns + returns into VRAM (~6s)
  2. run_cyc003_gpu()   — 326 factor×tier runs on pre-loaded VRAM data (~67s)
  3. All results saved to strategy bank

Target: 326 CYC-003 runs in < 2 minutes, >80% GPU utilization.

Usage:
    python run_cyc003_gpu.py
    python run_cyc003_gpu.py --tier all          # single tier
    python run_cyc003_gpu.py --dry-run           # no bank writes
    python run_cyc003_gpu.py --parity           # parity check vs CPU only
    python run_cyc003_gpu.py --start 2000-06-30  # custom date range
"""
from __future__ import annotations

import os
import sys
import time
import argparse
import logging

import io as _io
if isinstance(sys.stdout, _io.TextIOWrapper):
    sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Imports ────────────────────────────────────────────────────────────────────
from engine.gpu_data_loader import load_all_data, MIRROR_DB
from engine.gpu_batch_runner import (
    run_cyc003_gpu,
    run_parity_check,
    CYC001_FACTORS,
    CYC002_SINGLES,
    DEFAULT_CAP_TIERS,
)
from engine.gpu_data_loader import CYC002_COMBOS_DEF

# ── Config ─────────────────────────────────────────────────────────────────────
START_DATE = '1995-03-31'
END_DATE   = '2024-12-31'

# Full combo list
ALL_COMBOS = [(cid, fa, fb, f"combo_{cid}", 'CYC002') for cid, fa, fb in CYC002_COMBOS_DEF]

# All available cap tiers — match historical coverage in factor bank exactly
# Tuple: (cap_tier_arg, min_market_cap, max_market_cap, label)
#   cap_tier_arg: passed to GPUDataPack.cap_mask() — usually 'all'
#   min_market_cap / max_market_cap: applied as additional filter on top of base mask
#   max_market_cap = 0 means no upper bound
ALL_CAP_TIERS = {
    'all':   ('all',  0,    0,        'R3K All-Cap'),
    'large': ('all',  10e9, 0,        'Large-Cap $10B+'),
    'mid':   ('all',  2e9,  10e9,     'Mid-Cap $2B-$10B'),
}


def _print_banner(n_factors, n_combos, tiers, dry_run=False):
    import cupy as cp
    dev = cp.cuda.Device()
    free, total = dev.mem_info
    print(f"\n{'='*65}")
    print(f"  CYC-003 GPU Factor Batch Run")
    print(f"  GPU:      Device {dev.id} | cc={dev.compute_capability} | "
          f"VRAM {free/1e9:.1f}/{total/1e9:.1f}GB")
    print(f"  Factors:  {n_factors} singles × {len(tiers)} tiers = {n_factors * len(tiers)}")
    print(f"  Combos:   {n_combos} combos × {min(len(tiers), 2)} tiers = {n_combos * min(len(tiers), 2)}")
    print(f"  DB:       {MIRROR_DB}")
    print(f"  Date:     {START_DATE} to {END_DATE}")
    print(f"  Tiers:    {list(tiers.keys())}")
    if dry_run:
        print(f"  MODE:     DRY RUN — no bank writes")
    print(f"{'='*65}\n")


def _run_parity(pack):
    """Compare GPU results against CPU on 5 factors. Print diff table."""
    cpu_avail = False
    run_factor_backtest_cpu = None
    FactorBacktestConfig_cpu = None
    try:
        from engine.factor_backtest_cpu_legacy import (  # noqa: PLC0415
            run_factor_backtest as run_factor_backtest_cpu,
            FactorBacktestConfig as FactorBacktestConfig_cpu,
        )
        cpu_avail = True
    except ImportError:
        pass

    print("\n[Parity Check] Running 5 factors on GPU...")
    gpu_results = run_parity_check(pack)

    print(f"\n{'Factor':<30} {'GPU_ICIR':>9} {'GPU_Spread':>10} {'GPU_Q1':>8}", end='')
    if cpu_avail:
        print(f" {'CPU_ICIR':>9} {'CPU_Spread':>10} {'|dICIR|':>8}")
    else:
        print()

    print('-' * (70 if not cpu_avail else 90))

    for score_col, gpu_res in gpu_results.items():
        if gpu_res.get('status') != 'complete':
            print(f"  {score_col:<30} ERROR: {gpu_res.get('error', 'unknown')}")
            continue
        fm = gpu_res.get('factor_metrics', {})
        gpu_icir   = fm.get('icir', 0)
        gpu_spread = fm.get('quintile_spread_cagr', 0)
        gpu_q1     = fm.get('q1_cagr', 0)
        print(f"  {score_col:<30} {gpu_icir:>9.3f} {gpu_spread*100:>9.1f}% {gpu_q1*100:>7.1f}%", end='')

        if cpu_avail and run_factor_backtest_cpu and FactorBacktestConfig_cpu:
            direction = 'higher_better'
            # Look up direction from factor lists
            for sc, d, _, _ in CYC001_FACTORS + CYC002_SINGLES:
                if sc == score_col:
                    direction = d
                    break
            try:
                cfg = FactorBacktestConfig_cpu(
                    score_column=score_col,
                    score_direction=direction,
                    start_date=START_DATE, end_date=END_DATE,
                    rebalance_freq='semi-annual', n_buckets=5, hold_months=6,
                    cap_tier='all', min_price=5.0,
                )
                cpu_res = run_factor_backtest_cpu(cfg)
                if cpu_res.get('status') == 'complete':
                    cfm = cpu_res.get('factor_metrics', {})
                    cpu_icir   = cfm.get('icir', 0)
                    cpu_spread = cfm.get('quintile_spread_cagr', 0)
                    delta_icir = abs(gpu_icir - cpu_icir)
                    flag = ' ✓' if delta_icir < 0.01 else ' ✗ PARITY FAIL'
                    print(f" {cpu_icir:>9.3f} {cpu_spread*100:>9.1f}% {delta_icir:>8.4f}{flag}")
                else:
                    print(f"  CPU error")
            except Exception as e:
                print(f"  CPU exc: {str(e)[:30]}")
        else:
            print()

    print()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='CYC-003 GPU Factor Batch Runner')
    ap.add_argument('--tier',    type=str, default=None,
                    help='Single tier to run: all | large')
    ap.add_argument('--dry-run', action='store_true',
                    help='Do not write to strategy bank')
    ap.add_argument('--parity',  action='store_true',
                    help='Run parity check vs CPU only (5 factors)')
    ap.add_argument('--start',   type=str, default=START_DATE,
                    help=f'Start date (default {START_DATE})')
    ap.add_argument('--end',     type=str, default=END_DATE,
                    help=f'End date (default {END_DATE})')
    ap.add_argument('--no-combos', action='store_true',
                    help='Skip combo runs')
    ap.add_argument('--factors-only', type=str, default=None,
                    help='Comma-separated list of specific score_columns to run')
    args = ap.parse_args()

    # Build active tiers
    tiers = ALL_CAP_TIERS
    if args.tier:
        if args.tier not in ALL_CAP_TIERS:
            log.error(f"Unknown tier '{args.tier}'. Valid: {list(ALL_CAP_TIERS.keys())}")
            sys.exit(1)
        tiers = {args.tier: ALL_CAP_TIERS[args.tier]}

    # Build factor list
    all_factors = CYC001_FACTORS + CYC002_SINGLES
    if args.factors_only:
        keep = set(args.factors_only.split(','))
        all_factors = [f for f in all_factors if f[0] in keep]
        log.info(f"Filtered to {len(all_factors)} specified factors")

    combos = [] if args.no_combos else ALL_COMBOS

    _print_banner(len(all_factors), len(combos), tiers, dry_run=args.dry_run)

    # ── Step 1: Load all data into VRAM ───────────────────────────────────────
    t0 = time.time()
    log.info("Step 1/2: Loading all data into GPU VRAM...")
    pack = load_all_data(
        mirror_db=MIRROR_DB,
        start_date=args.start,
        end_date=args.end,
        rebal_freq='semi-annual',
        hold_months=6,
        min_price=5.0,
        min_adv_usd=1_000_000.0,
    )
    load_s = time.time() - t0
    log.info(f"Data load complete: {load_s:.1f}s | {pack.gpu_status()}")

    # ── Optional parity check ─────────────────────────────────────────────────
    if args.parity:
        _run_parity(pack)
        sys.exit(0)

    # ── Step 2: Run all 326 jobs on pre-loaded VRAM data ─────────────────────
    log.info(f"\nStep 2/2: Running {len(all_factors) * len(tiers) + len(combos) * min(len(tiers), 2)} GPU jobs...")
    t1 = time.time()

    batch_result = run_cyc003_gpu(
        data_pack=pack,
        factor_list=all_factors,
        cap_tiers=tiers,
        combo_list=combos,
        run_label_tag='[CYC-003-GPU]',
        save_to_bank=not args.dry_run,
        cb=lambda m: log.info(m),
    )

    run_s = time.time() - t1
    total_s = time.time() - t0

    # ── Summary ───────────────────────────────────────────────────────────────
    n_ok  = batch_result['ok_count']
    n_err = batch_result['err_count']
    n_tot = batch_result['total']

    print(f"\n{'='*65}")
    print(f"  CYC-003 GPU Run Complete")
    print(f"  Results:  {n_ok}/{n_tot} ok | {n_err} errors")
    print(f"  Load:     {load_s:.1f}s")
    print(f"  Run:      {run_s:.1f}s ({run_s/max(n_tot,1):.2f}s/job)")
    print(f"  TOTAL:    {total_s:.1f}s ({total_s/60:.1f}min)")
    print(f"  GPU util: see nvidia-smi during run")
    print(f"{'='*65}\n")

    # ── Save results log ──────────────────────────────────────────────────────
    log_path = 'cyc003_gpu_results.log'
    with open(log_path, 'w') as f:
        f.write(f"CYC-003 GPU Run — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total: {n_ok}/{n_tot} ok | {n_err} errors | {total_s:.1f}s\n\n")
        f.write('\n'.join(batch_result['results_log']))
    log.info(f"Results saved to {log_path}")

    # Non-zero exit if any errors
    sys.exit(1 if n_err > 0 and n_ok == 0 else 0)
