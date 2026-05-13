# -*- coding: utf-8 -*-
"""
run_cyc007_gpu.py — CYC-007 Sector-Optimized Composite GPU Run
===============================================================
Tests 9 multi-factor composite scores within their target sectors.

Method: rank-based equal-weight composites (built in cyc007_score_compute.py)
Timing: SA-JUN-DEC (semi-annual with Jun 30 + Dec 31 rebalance dates)
Run:    9 composites × within-sector mask on GPU RTX 3090

Comparison baseline (CYC-005 best single-factor per sector):
  cyc7_hc_quality      vs OCF/Assets        HC OBQ 0.876
  cyc7_hc_alpha        vs OCF/Assets        HC OBQ 0.876
  cyc7_it_quality      vs jcn_alpha_trifecta IT OBQ 0.883
  cyc7_it_fcf          vs jcn_alpha_trifecta IT OBQ 0.883
  cyc7_fin_quality     vs jcn_alpha_trifecta FIN OBQ 0.568
  cyc7_condisc_quality vs jcn_alpha_trifecta CD OBQ 0.683
  cyc7_staples_quality vs OCF/Assets        CS OBQ 0.651
  cyc7_ind_quality     vs jcn_alpha_trifecta IND OBQ 0.660
  cyc7_mat_quality     vs jcn_alpha_trifecta MAT OBQ 0.737

Usage:
    python run_cyc007_gpu.py --full          # score compute + GPU run
    python run_cyc007_gpu.py --compute-scores
    python run_cyc007_gpu.py                 # GPU only (scores already exist)
    python run_cyc007_gpu.py --dry-run
    python run_cyc007_gpu.py --analyze       # print comparison table from bank
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

import cupy as cp
from engine.gpu_data_loader import load_all_data, MIRROR_DB
from engine.gpu_factor_compute import run_factor_on_gpu
from engine.strategy_bank import save_factor_model

FUND_DB    = os.environ.get("OBQ_FUND_DB", r"D:/OBQ_AI/obq_fundamentals.duckdb")
BANK_DB    = os.environ.get("OBQ_BANK_DB", r"D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb")
START_DATE = '1995-03-31'
END_DATE   = '2024-12-31'

# ── GICS sector id mapping (must match gpu_data_loader._SECTOR_MAP) ─────────
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

# ── CYC-007 composite run definitions ─────────────────────────────────────────
# (factor_col, direction, display_name, target_sector, cyc005_baseline_obq)
CYC007_RUNS = [
    ('cyc7_hc_quality',      'higher_better', 'HC Quality (OCF+EBIT+F)',         'Health Care',            0.876),
    ('cyc7_hc_alpha',        'higher_better', 'HC Alpha (Trifecta+OCF+FCF)',      'Health Care',            0.876),
    ('cyc7_it_quality',      'higher_better', 'IT Quality (Trifecta+OBQQual+OCF)','Information Technology', 0.883),
    ('cyc7_it_fcf',          'higher_better', 'IT FCF (FCF+OCF+Rule40)',          'Information Technology', 0.883),
    ('cyc7_fin_quality',     'higher_better', 'FIN Quality (F+Trifecta+QARP)',    'Financials',             0.568),
    ('cyc7_condisc_quality', 'higher_better', 'CD Quality (OCF+F+Trifecta)',      'Consumer Discretionary', 0.683),
    ('cyc7_staples_quality', 'higher_better', 'CS Quality (OCF+QARP+Trifecta)',   'Consumer Staples',       0.651),
    ('cyc7_ind_quality',     'higher_better', 'IND Quality (F+Trifecta+IndEff)',  'Industrials',            0.660),
    ('cyc7_mat_quality',     'higher_better', 'MAT Quality (Trifecta+F+OCF)',     'Materials',              0.737),
]


def _check_scores_exist() -> bool:
    try:
        import duckdb
        con = duckdb.connect(FUND_DB, read_only=True)
        exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='scores' AND table_name='obq_cyc007_scores'
        """).fetchone()[0] > 0
        if exists:
            n = con.execute("SELECT COUNT(*) FROM scores.obq_cyc007_scores").fetchone()[0]
            log.info(f"  CYC-007 score table found: {n:,} rows")
        con.close()
        return exists
    except Exception:
        return False


def _gpu_warmup(pack) -> None:
    d_s = cp.zeros((5, 50), dtype=cp.float64)
    d_r = cp.zeros((5, 50), dtype=cp.float64)
    d_v = cp.ones((5, 50),  dtype=bool)
    d_n = cp.full(5, 50,    dtype=cp.int32)
    try:
        run_factor_on_gpu(d_s, d_r, d_v, d_n, [f'2000-0{i}-30' for i in range(1, 6)], score_column='_warmup')
    except Exception:
        pass


def _run_single_job(
    factor_col: str,
    direction: str,
    display: str,
    pack,
    sector_id: Optional[int],
    sector_name: Optional[str],
    save: bool,
    job_num: int,
    total_jobs: int,
) -> dict:
    """Run one GPU job with sector mask. Returns result dict."""
    if factor_col not in pack.score_columns:
        return {'status': 'skip', 'reason': f'{factor_col} not in VRAM (scores not computed?)'}

    scores_gpu   = pack.score_columns[factor_col]
    lower_better = (direction == 'lower_better')

    if sector_id is not None:
        cap_mask   = (pack.sector_gpu == sector_id)
        tier_label = f"Sector-{sector_name.replace(' ', '-')}"
    else:
        cap_mask   = None
        tier_label = 'R3K-All-Cap'

    run_label = f"{display} | 5Q | 6mo | {tier_label} | 1995-2024 [CYC-007]"
    config = {
        'score_column':   factor_col,
        'n_buckets':      5,
        'hold_months':    6,
        'start_date':     START_DATE,
        'end_date':       END_DATE,
        'cap_tier':       tier_label,
        'rebalance_freq': 'semi-annual',
        'min_price':      5.0,
        'cost_bps':       15.0,
        'sector':         sector_name or 'all',
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
        fm  = result.get('factor_metrics', {})
        msg = (f"OK [{job_num}/{total_jobs}] {factor_col} [{tier_label}] "
               f"ICIR={fm.get('icir',0):.3f} OBQ={fm.get('obq_fund_score',0):.3f} "
               f"GPU={result.get('elapsed_gpu_ms',0):.0f}ms")
    else:
        msg = f"ERR [{job_num}/{total_jobs}] {factor_col} [{tier_label}]: {result.get('error','?')}"

    return {**result, '_msg': msg}


def _print_banner(n_jobs: int, dry_run: bool = False):
    dev = cp.cuda.Device()
    free, total = dev.mem_info
    print(f"\n{'='*65}")
    print(f"  CYC-007 Sector-Optimized Composite GPU Run")
    print(f"  GPU:   Device {dev.id} | cc={dev.compute_capability} | VRAM {free/1e9:.1f}/{total/1e9:.1f}GB")
    print(f"  Jobs:  {n_jobs} composites × within-sector")
    print(f"  DB:    {MIRROR_DB}")
    print(f"  Date:  {START_DATE} to {END_DATE}  |  Timing: SA-JUN-DEC")
    if dry_run:
        print(f"  MODE:  DRY RUN — no bank writes")
    print(f"{'='*65}\n")


def _analyze_results():
    """Print CYC-007 results vs CYC-005 baselines from the factor bank."""
    import duckdb
    try:
        con = duckdb.connect(BANK_DB, read_only=True)
    except Exception as e:
        print(f"ERROR: Cannot open bank DB: {e}")
        return

    # Baseline map: {sector_label: (best_factor_name, baseline_obq)}
    BASELINES = {r[0]: (r[1], r[2]) for r in [
        ('Sector-Health-Care',            'OCF/Assets',         0.876),
        ('Sector-Information-Technology', 'jcn_alpha_trifecta', 0.883),
        ('Sector-Financials',             'jcn_alpha_trifecta', 0.568),
        ('Sector-Consumer-Discretionary', 'jcn_alpha_trifecta', 0.683),
        ('Sector-Consumer-Staples',       'OCF/Assets',         0.651),
        ('Sector-Industrials',            'jcn_alpha_trifecta', 0.660),
        ('Sector-Materials',              'jcn_alpha_trifecta', 0.737),
    ]}

    print(f"\n{'='*80}")
    print(f"  CYC-007 RESULTS — Composite vs Single-Factor Baselines")
    print(f"{'='*80}")
    print(f"  {'Composite':<30} {'Sector':<25} {'OBQ':>6}  {'Baseline':>8}  {'Delta':>7}  {'Beat?':>5}")
    print(f"  {'-'*30} {'-'*25} {'-'*6}  {'-'*8}  {'-'*7}  {'-'*5}")

    results = []
    for factor_col, _, display, target_sector, baseline_obq in CYC007_RUNS:
        tier_label = f"Sector-{target_sector.replace(' ', '-')}"
        row = con.execute(f"""
            SELECT fm.obq_fund_score, fm.icir
            FROM factor_models fm
            WHERE fm.score_column = '{factor_col}'
              AND fm.cap_tier = '{tier_label}'
            ORDER BY fm.created_at DESC
            LIMIT 1
        """).fetchone()

        if row:
            obq, icir = row[0], row[1]
            delta = obq - baseline_obq
            beat  = "YES" if obq > baseline_obq else "no"
            results.append((factor_col, target_sector, obq, baseline_obq, delta, beat))
            print(f"  {factor_col:<30} {target_sector:<25} {obq:>6.3f}  {baseline_obq:>8.3f}  {delta:>+7.3f}  {beat:>5}")
        else:
            print(f"  {factor_col:<30} {target_sector:<25} {'N/A':>6}  {baseline_obq:>8.3f}  {'—':>7}  {'—':>5}")

    print(f"\n{'='*80}")
    if results:
        beat_count = sum(1 for r in results if r[5] == "YES")
        avg_delta  = sum(r[4] for r in results) / len(results)
        best = max(results, key=lambda r: r[2])
        print(f"  Beat baseline:  {beat_count}/{len(results)} composites")
        print(f"  Avg OBQ delta:  {avg_delta:+.3f}")
        print(f"  Best composite: {best[0]} OBQ={best[2]:.3f} ({best[1]})")
    print(f"{'='*80}\n")
    con.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='CYC-007 Sector-Optimized Composite GPU Run')
    ap.add_argument('--compute-scores', action='store_true', help='Run score compute pipeline first')
    ap.add_argument('--full',           action='store_true', help='Score compute + GPU run')
    ap.add_argument('--dry-run',        action='store_true', help='No bank writes')
    ap.add_argument('--analyze',        action='store_true', help='Print results table from bank (no GPU run)')
    ap.add_argument('--start',          default=START_DATE)
    ap.add_argument('--end',            default=END_DATE)
    args = ap.parse_args()

    # ── Analyze-only mode ─────────────────────────────────────────────────────
    if args.analyze:
        _analyze_results()
        sys.exit(0)

    # ── Score computation ─────────────────────────────────────────────────────
    if args.compute_scores or args.full:
        log.info("=" * 65)
        log.info("CYC-007 Score Computation (CPU)")
        log.info("=" * 65)
        from engine.cyc007_score_compute import run_score_pipeline
        run_score_pipeline(start_date=args.start, end_date=args.end,
                           overwrite=True, dry_run=args.dry_run)
        if not args.full:
            sys.exit(0)

    # ── Check scores exist ────────────────────────────────────────────────────
    if not _check_scores_exist():
        print("ERROR: CYC-007 score table not found. Run --compute-scores first.")
        sys.exit(1)

    # ── Data load into VRAM ───────────────────────────────────────────────────
    t0 = time.time()
    log.info("Loading all data into GPU VRAM (CYC-003 through CYC-007 scores)...")
    pack = load_all_data(
        mirror_db=MIRROR_DB, fund_db=FUND_DB,
        start_date=args.start, end_date=args.end,
        rebal_freq='semi-annual', hold_months=6,
        min_price=5.0, min_adv_usd=1_000_000.0,
    )
    load_s = time.time() - t0
    log.info(f"Data loaded: {load_s:.1f}s | {pack.gpu_status()}")

    # Verify CYC-007 columns made it into VRAM
    cyc7_in_vram  = [r[0] for r in CYC007_RUNS if r[0] in pack.score_columns]
    cyc7_missing  = [r[0] for r in CYC007_RUNS if r[0] not in pack.score_columns]
    log.info(f"CYC-007 composites in VRAM: {len(cyc7_in_vram)}/{len(CYC007_RUNS)}")
    if cyc7_missing:
        log.warning(f"  Missing from VRAM: {cyc7_missing}")
        log.warning("  Run --compute-scores to build the score table first.")

    # ── JIT warmup ────────────────────────────────────────────────────────────
    log.info("JIT warmup...")
    _gpu_warmup(pack)

    # ── Build job list ─────────────────────────────────────────────────────────
    jobs = []
    for factor_col, direction, display, target_sector, baseline_obq in CYC007_RUNS:
        if factor_col not in pack.score_columns:
            log.warning(f"  SKIP {factor_col} — not in VRAM")
            continue
        sector_id = SECTOR_MAP.get(target_sector)
        jobs.append({
            'factor_col':   factor_col,
            'direction':    direction,
            'display':      display,
            'sector_id':    sector_id,
            'sector_name':  target_sector,
            'baseline_obq': baseline_obq,
        })

    total_jobs = len(jobs)
    _print_banner(total_jobs, dry_run=args.dry_run)

    if total_jobs == 0:
        print("No jobs to run — check that CYC-007 scores were computed.")
        sys.exit(1)

    # ── GPU batch ─────────────────────────────────────────────────────────────
    t_run     = time.time()
    ok_count  = 0
    err_count = 0
    log_lines = []
    run_results = []

    for i, job in enumerate(jobs, start=1):
        result = _run_single_job(
            factor_col=job['factor_col'],
            direction=job['direction'],
            display=job['display'],
            pack=pack,
            sector_id=job['sector_id'],
            sector_name=job['sector_name'],
            save=not args.dry_run,
            job_num=i,
            total_jobs=total_jobs,
        )
        msg = result.get('_msg', f"[{i}/{total_jobs}] {job['factor_col']}")
        if result.get('status') == 'complete':
            ok_count += 1
            fm = result.get('factor_metrics', {})
            run_results.append({
                'col':      job['factor_col'],
                'sector':   job['sector_name'],
                'obq':      fm.get('obq_fund_score', 0.0),
                'icir':     fm.get('icir', 0.0),
                'baseline': job['baseline_obq'],
            })
        elif result.get('status') == 'skip':
            log.info(f"  SKIP [{i}/{total_jobs}] {job['factor_col']} — {result.get('reason')}")
            continue
        else:
            err_count += 1
        log_lines.append(msg)
        log.info(msg)

    run_s   = time.time() - t_run
    total_s = time.time() - t0

    # ── Summary table ─────────────────────────────────────────────────────────
    print(f"\n{'='*75}")
    print(f"  CYC-007 COMPLETE  |  {ok_count}/{total_jobs} OK  |  {err_count} errors")
    print(f"  Load: {load_s:.1f}s  |  Run: {run_s:.1f}s  |  Total: {total_s:.1f}s")
    print(f"{'='*75}")
    if run_results:
        print(f"\n  {'Composite':<30} {'Sector':<25} {'OBQ':>6}  {'Baseline':>8}  {'Delta':>7}  {'Beat?':>5}")
        print(f"  {'-'*30} {'-'*25} {'-'*6}  {'-'*8}  {'-'*7}  {'-'*5}")
        beat_count = 0
        for r in run_results:
            delta = r['obq'] - r['baseline']
            beat  = "YES" if r['obq'] > r['baseline'] else "no"
            if beat == "YES":
                beat_count += 1
            print(f"  {r['col']:<30} {r['sector']:<25} {r['obq']:>6.3f}  {r['baseline']:>8.3f}  {delta:>+7.3f}  {beat:>5}")
        avg_delta = sum(r['obq'] - r['baseline'] for r in run_results) / len(run_results)
        print(f"\n  Beat baseline: {beat_count}/{len(run_results)}  |  Avg delta: {avg_delta:+.3f}")
    print(f"{'='*75}\n")

    # ── Save run log ──────────────────────────────────────────────────────────
    if not args.dry_run:
        log_path = 'cyc007_gpu_results.log'
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"CYC-007 Run — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total: {ok_count}/{total_jobs} ok | {err_count} errors | {total_s:.1f}s\n\n")
            f.write('\n'.join(log_lines))
        log.info(f"Results saved to {log_path}")
