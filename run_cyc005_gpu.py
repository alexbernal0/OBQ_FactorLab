# -*- coding: utf-8 -*-
"""
run_cyc005_gpu.py — CYC-005 Sector Intelligence GPU Batch Run
=============================================================
Two-component cycle:

  Component A — Sector Champion Run:
    Take top 15 cross-sectional factors (from CYC-003/004)
    Run each WITHIN each of 11 sectors (sector mask = universe)
    15 factors × 11 sectors = 165 jobs

  Component B — Novel Sector Factor Run:
    11 new sector-specific factors (computed in cyc005_score_compute.py)
    Run cross-sectionally (full R3K) AND within target sector
    11 factors × 2 (full + sector) = 22 jobs

  Total: 187 jobs on RTX 3090 (~15-20 min including score computation)

Usage:
    python run_cyc005_gpu.py --full        # score compute + both components
    python run_cyc005_gpu.py --compute-scores
    python run_cyc005_gpu.py               # GPU only (scores already exist)
    python run_cyc005_gpu.py --component a # only sector champion run
    python run_cyc005_gpu.py --component b # only novel factor run
    python run_cyc005_gpu.py --dry-run
"""
from __future__ import annotations

import os
import sys
import time
import argparse
import logging
from typing import Optional

import io as _io
if isinstance(sys.stdout, _io.TextIOWrapper):
    sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

import cupy as cp
from engine.gpu_data_loader import load_all_data, MIRROR_DB
from engine.gpu_factor_compute import run_factor_on_gpu, run_combo_on_gpu
from engine.strategy_bank import save_factor_model

FUND_DB    = os.environ.get("OBQ_FUND_DB", r"D:/OBQ_AI/obq_fundamentals.duckdb")
START_DATE = '1995-03-31'
END_DATE   = '2024-12-31'

# ── GICS sector id mapping (must match gpu_data_loader._SECTOR_MAP) ────────────
SECTOR_MAP = {
    'Energy':                   1,
    'Materials':                2,
    'Industrials':              3,
    'Consumer Discretionary':   4,
    'Consumer Staples':         5,
    'Health Care':              6,
    'Financials':               7,
    'Information Technology':   8,
    'Communication Services':   9,
    'Utilities':                10,
    'Real Estate':              11,
}

# ── Component A: top cross-sectional factors to test within each sector ─────────
# Selected based on highest OBQ Master Score + diverse factor families
SECTOR_CHAMPION_FACTORS = [
    # Quality / Profitability
    ('cyc4_ocf_assets',          'higher_better', 'OCF/Assets'),
    ('cyc4_ebit_assets',         'higher_better', 'EBIT/Assets'),
    ('cyc4_fscore',              'higher_better', 'Piotroski F-Score'),
    ('quality_score_universe',   'higher_better', 'OBQ Quality (Universe)'),
    ('cyc4_retained_earnings_ta','higher_better', 'Retained Earnings/TA'),
    # Valuation
    ('cyc4_ebit_ev',             'higher_better', 'EBIT/EV'),
    ('cyc4_sales_ev',            'higher_better', 'Sales/EV'),
    ('cyc2_ps',                  'lower_better',  'Price/Sales'),
    ('cyc4_pretax_margin',       'higher_better', 'Pretax Margin'),
    # Composite
    ('jcn_qarp',                 'higher_better', 'JCN QARP'),
    ('jcn_alpha_trifecta',       'higher_better', 'JCN Alpha Trifecta'),
    # Momentum
    ('cyc4_skip_month_mom',      'higher_better', 'Skip-Month Momentum'),
    # Low-risk
    ('cyc4_realized_vol',        'lower_better',  'Realized Volatility'),
    # Efficiency
    ('cyc4_asset_turnover',      'higher_better', 'Asset Turnover'),
    # FCF
    ('cyc2_fcf_margin',          'higher_better', 'FCF Margin'),
]

# ── Component B: novel sector-specific factors ──────────────────────────────────
# (score_col, direction, display, target_sector, run_cross_sectional)
NOVEL_SECTOR_FACTORS = [
    ('cyc5_energy_midcycle_fcf',    'higher_better', 'Energy: Mid-Cycle FCF Yield',       'Energy',                 True),
    ('cyc5_materials_ccc',          'lower_better',  'Materials: Cash Conv Cycle',         'Materials',              True),
    ('cyc5_industrials_efficiency', 'higher_better', 'Industrials: Efficiency Composite',  'Industrials',            True),
    ('cyc5_consumer_disc_brand',    'higher_better', 'Cons Disc: Brand-Growth Score',      'Consumer Discretionary', True),
    ('cyc5_staples_div_growth',     'higher_better', 'Cons Staples: Dividend Growth',      'Consumer Staples',       True),
    ('cyc5_healthcare_rnd_yield',   'higher_better', 'Health Care: R&D Yield',             'Health Care',            True),
    ('cyc5_financials_capital',     'higher_better', 'Financials: Capital Adequacy',       'Financials',             True),
    ('cyc5_it_rule_of_40',          'higher_better', 'IT: Rule of 40',                     'Information Technology', True),
    ('cyc5_comms_content_roi',      'higher_better', 'Comm Svcs: Content ROI',             'Communication Services', True),
    ('cyc5_utilities_safe_yield',   'higher_better', 'Utilities: Safe Dividend Yield',     'Utilities',              True),
    ('cyc5_realestate_ffo_yield',   'higher_better', 'Real Estate: FFO Proxy Yield',       'Real Estate',            True),
]


def _run_single_job(
    factor_col: str,
    direction: str,
    display: str,
    pack,
    sector_id: Optional[int],
    sector_name: Optional[str],
    label_tag: str,
    save: bool,
    job_num: int,
    total_jobs: int,
) -> dict:
    """Run one GPU job with optional sector mask. Returns result dict."""
    if factor_col not in pack.score_columns:
        return {'status': 'skip', 'reason': f'{factor_col} not in VRAM'}

    scores_gpu  = pack.score_columns[factor_col]
    lower_better = (direction == 'lower_better')

    # Build sector mask (replaces cap_tier)
    if sector_id is not None:
        cap_mask = (pack.sector_gpu == sector_id)
        tier_label = f"Sector-{sector_name.replace(' ', '-')}"
    else:
        cap_mask = None  # full R3K
        tier_label = 'R3K-All-Cap'

    run_label = f"{display} | 5Q | 6mo | {tier_label} | 1995-2024 {label_tag}"
    config = {
        'score_column': factor_col,
        'n_buckets': 5,
        'hold_months': 6,
        'start_date': START_DATE,
        'end_date': END_DATE,
        'cap_tier': tier_label,
        'rebalance_freq': 'semi-annual',
        'min_price': 5.0,
        'cost_bps': 15.0,
        'sector': sector_name or 'all',
    }

    result = run_factor_on_gpu(
        scores_gpu=scores_gpu,
        returns_gpu=pack.returns_gpu,
        valid_gpu=pack.valid_gpu,
        n_valid=pack.n_valid,
        dates=pack.dates,
        n_buckets=5,
        lower_better=lower_better,
        hold_months=6,
        score_column=factor_col,
        cap_mask_gpu=cap_mask,
        sector_gpu=pack.sector_gpu,
        market_cap_gpu=getattr(pack, 'market_cap_gpu', None),
    )

    if result.get('status') == 'complete':
        result['run_label']    = run_label
        result['config']       = config
        result['score_column'] = factor_col
        if save:
            save_factor_model(result, overwrite=True)
        fm = result.get('factor_metrics', {})
        msg = (f"OK [{job_num}/{total_jobs}] {factor_col} [{tier_label}] "
               f"ICIR={fm.get('icir',0):.3f} OBQ={fm.get('obq_fund_score',0):.3f} "
               f"GPU={result.get('elapsed_gpu_ms',0):.0f}ms")
    else:
        msg = f"ERR [{job_num}/{total_jobs}] {factor_col} [{tier_label}]: {result.get('error','?')}"

    return {**result, '_msg': msg}


def _gpu_warmup(pack) -> None:
    """Force JIT compilation before the real batch."""
    import cupy as cp
    d_s = cp.zeros((5, 50), dtype=cp.float64)
    d_r = cp.zeros((5, 50), dtype=cp.float64)
    d_v = cp.ones((5, 50),  dtype=bool)
    d_n = cp.full(5, 50,    dtype=cp.int32)
    try:
        run_factor_on_gpu(d_s, d_r, d_v, d_n, [f'2000-0{i}-30' for i in range(1,6)], score_column='_warmup')
    except Exception:
        pass


def _check_scores_exist() -> bool:
    try:
        import duckdb
        con = duckdb.connect(FUND_DB, read_only=True)
        exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='scores' AND table_name='obq_cyc005_scores'
        """).fetchone()[0] > 0
        con.close()
        return exists
    except Exception:
        return False


def _print_banner(n_jobs: int, dry_run: bool = False):
    dev = cp.cuda.Device()
    free, total = dev.mem_info
    print(f"\n{'='*65}")
    print(f"  CYC-005 Sector Intelligence GPU Run")
    print(f"  GPU:   Device {dev.id} | cc={dev.compute_capability} | VRAM {free/1e9:.1f}/{total/1e9:.1f}GB")
    print(f"  Jobs:  {n_jobs} total (Component A: sector champions + Component B: novel)")
    print(f"  DB:    {MIRROR_DB}")
    print(f"  Date:  {START_DATE} to {END_DATE}")
    if dry_run:
        print(f"  MODE:  DRY RUN — no bank writes")
    print(f"{'='*65}\n")


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='CYC-005 Sector Intelligence GPU Run')
    ap.add_argument('--compute-scores', action='store_true')
    ap.add_argument('--full',           action='store_true')
    ap.add_argument('--dry-run',        action='store_true')
    ap.add_argument('--component',      type=str, default='both', choices=['a','b','both'])
    ap.add_argument('--start',          default=START_DATE)
    ap.add_argument('--end',            default=END_DATE)
    args = ap.parse_args()

    # ── Score computation ────────────────────────────────────────────────────────
    if args.compute_scores or args.full:
        log.info("=" * 65)
        log.info("CYC-005 Score Computation (CPU)")
        log.info("=" * 65)
        from engine.cyc005_score_compute import run_score_pipeline
        run_score_pipeline(start_date=args.start, end_date=args.end,
                           overwrite=True, dry_run=args.dry_run)
        if not args.full:
            sys.exit(0)

    if not _check_scores_exist():
        print("ERROR: CYC-005 score table not found. Run --compute-scores first.")
        sys.exit(1)

    # ── Data load ────────────────────────────────────────────────────────────────
    t0 = time.time()
    log.info("Loading all data into GPU VRAM (CYC-003 + CYC-004 + CYC-005 scores)...")
    pack = load_all_data(
        mirror_db=MIRROR_DB, fund_db=FUND_DB,
        start_date=args.start, end_date=args.end,
        rebal_freq='semi-annual', hold_months=6,
        min_price=5.0, min_adv_usd=1_000_000.0,
    )
    load_s = time.time() - t0
    log.info(f"Data loaded: {load_s:.1f}s | {pack.gpu_status()}")

    # CYC-005 factors in VRAM?
    cyc5_in_vram = [f[0] for f in NOVEL_SECTOR_FACTORS if f[0] in pack.score_columns]
    cyc5_missing = [f[0] for f in NOVEL_SECTOR_FACTORS if f[0] not in pack.score_columns]
    log.info(f"CYC-005 novel factors in VRAM: {len(cyc5_in_vram)}/11")
    if cyc5_missing:
        log.warning(f"  Missing: {cyc5_missing}")

    # ── JIT warmup ───────────────────────────────────────────────────────────────
    log.info("JIT warmup...")
    _gpu_warmup(pack)

    # ── Build job list ────────────────────────────────────────────────────────────
    jobs = []

    if args.component in ('a', 'both'):
        log.info("\n--- Component A: Sector Champion Run ---")
        for sector_name, sector_id in SECTOR_MAP.items():
            for sc, direction, display in SECTOR_CHAMPION_FACTORS:
                if sc not in pack.score_columns:
                    continue
                jobs.append({
                    'factor_col': sc, 'direction': direction, 'display': display,
                    'sector_id': sector_id, 'sector_name': sector_name,
                    'tag': '[CYC-005a-SECTOR]',
                })

    if args.component in ('b', 'both'):
        log.info("\n--- Component B: Novel Sector Factor Run ---")
        for sc, direction, display, target_sector, run_cross in NOVEL_SECTOR_FACTORS:
            if sc not in pack.score_columns:
                log.warning(f"  SKIP {sc} — not in VRAM")
                continue
            # Within-sector run
            sector_id = SECTOR_MAP.get(target_sector)
            jobs.append({
                'factor_col': sc, 'direction': direction, 'display': display,
                'sector_id': sector_id, 'sector_name': target_sector,
                'tag': '[CYC-005b-NOVEL]',
            })
            # Cross-sectional run (full R3K)
            if run_cross:
                jobs.append({
                    'factor_col': sc, 'direction': direction, 'display': display,
                    'sector_id': None, 'sector_name': None,
                    'tag': '[CYC-005b-NOVEL]',
                })

    total_jobs = len(jobs)
    _print_banner(total_jobs, dry_run=args.dry_run)

    # ── GPU batch ─────────────────────────────────────────────────────────────────
    t_run = time.time()
    ok_count  = 0
    err_count = 0
    log_lines = []

    for i, job in enumerate(jobs, start=1):
        result = _run_single_job(
            factor_col=job['factor_col'],
            direction=job['direction'],
            display=job['display'],
            pack=pack,
            sector_id=job['sector_id'],
            sector_name=job['sector_name'],
            label_tag=job['tag'],
            save=not args.dry_run,
            job_num=i,
            total_jobs=total_jobs,
        )
        msg = result.get('_msg', f"[{i}/{total_jobs}] {job['factor_col']}")
        if result.get('status') == 'complete':
            ok_count += 1
        elif result.get('status') == 'skip':
            log.info(f"  SKIP [{i}/{total_jobs}] {job['factor_col']} — {result.get('reason')}")
            continue
        else:
            err_count += 1
        log_lines.append(msg)

        if i % 20 == 0 or i <= 3 or i == total_jobs:
            elapsed = round(time.time() - t_run)
            rate = i / max(elapsed, 0.1)
            eta  = round((total_jobs - i) / max(rate, 0.01))
            log.info(f"  [{i}/{total_jobs}] ok={ok_count} err={err_count} "
                     f"elapsed={elapsed//60}m{elapsed%60}s ETA={eta//60}m{eta%60}s")

    run_s   = time.time() - t_run
    total_s = time.time() - t0

    print(f"\n{'='*65}")
    print(f"  CYC-005 Complete")
    print(f"  Results:  {ok_count}/{total_jobs} ok | {err_count} errors")
    print(f"  Load:     {load_s:.1f}s")
    print(f"  Run:      {run_s:.1f}s ({run_s/max(total_jobs,1):.2f}s/job)")
    print(f"  TOTAL:    {total_s:.1f}s ({total_s/60:.1f}min)")
    print(f"{'='*65}\n")

    # Save log
    if not args.dry_run:
        with open('cyc005_gpu_results.log', 'w', encoding='utf-8') as f:
            f.write(f"CYC-005 Run — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total: {ok_count}/{total_jobs} ok | {err_count} errors | {total_s:.1f}s\n\n")
            f.write('\n'.join(log_lines))
        log.info("Results saved to cyc005_gpu_results.log")

    sys.exit(1 if err_count > ok_count else 0)
