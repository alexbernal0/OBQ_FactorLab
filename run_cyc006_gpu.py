# -*- coding: utf-8 -*-
"""
run_cyc006_gpu.py — CYC-006 Rebalance Timing Research
=====================================================
Tests all 150 unique factor baselines across 10 rebalance timing variants
to definitively answer: WHEN should each factor type be rebalanced?

Timing Variants (vs semi-annual Jun/Dec baseline in CYC-003/004/005):

  Component A — Quarterly rebalancing (3-month hold):
    A1  Q-STD     Mar/Jun/Sep/Dec   quarter-end standard
    A2  Q-OFF1    Jan/Apr/Jul/Oct   1-month pre-quarter
    A3  Q-OFF2    Feb/May/Aug/Nov   2-months offset

  Component B — Semi-annual, different start months (6-month hold):
    B1  SA-MAR-SEP  Mar + Sep      shifted 3mo from baseline Jun/Dec
    B2  SA-JAN-JUL  Jan + Jul      shifted 5mo
    B3  SA-APR-OCT  Apr + Oct      shifted 4mo
    (B0 = Jun/Dec is the CYC-003 baseline — already tested, skip)

  Component C — Annual rebalancing (12-month hold), test all 4 quarter-ends:
    C1  A-Q4   December 31    year-end (conventional wisdom)
    C2  A-Q1   March 31       post-Q4-earnings-season
    C3  A-Q2   June 30        post-Q1-earnings
    C4  A-Q3   September 30   post-Q2-earnings

Total: 10 variants × 150 factors × 1 tier (all-cap R3K) = 1,500 GPU jobs

Post-run analysis: aggregates OBQ/ICIR by timing variant × factor family
to produce definitive "optimal timing per factor type" recommendation.

Usage:
    python run_cyc006_gpu.py --dry-run --factor jcn_qarp   # dry run, 1 factor, 10 timings
    python run_cyc006_gpu.py --dry-run                      # dry run all factors, no bank writes
    python run_cyc006_gpu.py                                 # full run
    python run_cyc006_gpu.py --component a                  # quarterly only
    python run_cyc006_gpu.py --component c                  # annual timing only
    python run_cyc006_gpu.py --analyze-only                 # just print timing analysis from existing results
"""
from __future__ import annotations

import os, sys, time, argparse, logging
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
from engine.gpu_factor_compute import run_factor_on_gpu
from engine.strategy_bank import save_factor_model

FUND_DB    = os.environ.get("OBQ_FUND_DB", r"D:/OBQ_AI/obq_fundamentals.duckdb")
START_DATE = '1995-03-31'
END_DATE   = '2024-12-31'

# ── 10 Timing variants ────────────────────────────────────────────────────────
# Each: (variant_id, rebal_months, hold_months, label, component)
TIMING_VARIANTS = [
    # Component A — Quarterly (3mo hold)
    ('Q-STD',    [3,6,9,12], 3,  'Quarterly Standard (Mar/Jun/Sep/Dec, 3mo hold)', 'A'),
    ('Q-OFF1',   [1,4,7,10], 3,  'Quarterly Offset-1 (Jan/Apr/Jul/Oct, 3mo hold)', 'A'),
    ('Q-OFF2',   [2,5,8,11], 3,  'Quarterly Offset-2 (Feb/May/Aug/Nov, 3mo hold)', 'A'),
    # Component B — Semi-annual rotations (6mo hold) — baseline Jun/Dec already in bank
    ('SA-MAR-SEP', [3,9],   6,  'Semi-Annual Mar+Sep (shifted 3mo, 6mo hold)', 'B'),
    ('SA-JAN-JUL', [1,7],   6,  'Semi-Annual Jan+Jul (shifted 5mo, 6mo hold)', 'B'),
    ('SA-APR-OCT', [4,10],  6,  'Semi-Annual Apr+Oct (shifted 4mo, 6mo hold)', 'B'),
    # Component C — Annual (12mo hold), 4 quarter-end rebalance points
    ('A-Q4',     [12],      12, 'Annual Q4 Dec 31 (12mo hold)',                  'C'),
    ('A-Q1',     [3],       12, 'Annual Q1 Mar 31 (12mo hold)',                  'C'),
    ('A-Q2',     [6],       12, 'Annual Q2 Jun 30 (12mo hold)',                  'C'),
    ('A-Q3',     [9],       12, 'Annual Q3 Sep 30 (12mo hold)',                  'C'),
]

# ── All factors to test (from CYC-003 + CYC-004 + CYC-005b cross-sectional) ──
def _get_all_factors(bank_path: str) -> list[tuple]:
    """Pull all unique (score_column, direction) from the bank."""
    import duckdb
    con = duckdb.connect(bank_path, read_only=True)
    rows = con.execute("""
        SELECT DISTINCT
            score_column,
            -- Infer direction from known lower_better signals
            CASE
              WHEN score_column IN (
                    'cyc2_ps','cyc2_pe','cyc2_pb','cyc2_ev_ebitda','cyc2_pfcf',
                    'cyc2_nd_ebitda','cyc2_nd_ebit','cyc2_debt_assets','cyc2_capex_ocf',
                    'cyc2_share_chg','cyc2_int_pct_op','fundsmith_rank','rulebreaker_rank',
                    'cyc4_accruals_ratio','cyc4_eps_stability','cyc4_sales_stability',
                    'cyc4_market_beta','cyc4_intangibles_pb','cyc4_cash_conv_cycle',
                    'cyc4_change_ar_assets','cyc4_change_inv_assets','cyc4_debt_equity',
                    'cyc4_idio_vol','cyc4_realized_vol','cyc4_wc_assets',
                    'cyc5_materials_ccc'
                ) THEN 'lower_better'
              ELSE 'higher_better'
            END AS direction
        FROM factor_models
        WHERE run_label LIKE '%CYC-003-GPU%'
           OR run_label LIKE '%CYC-004-GPU%'
           OR (run_label LIKE '%CYC-005b-NOVEL%' AND cap_tier='R3K-All-Cap')
        GROUP BY score_column
        ORDER BY score_column
    """).fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]


def _gpu_warmup(pack) -> None:
    d_s = cp.zeros((5, 50), dtype=cp.float64)
    d_r = cp.zeros((5, 50), dtype=cp.float64)
    d_v = cp.ones((5, 50),  dtype=bool)
    d_n = cp.full(5, 50,    dtype=cp.int32)
    try:
        run_factor_on_gpu(d_s, d_r, d_v, d_n,
                          [f'2000-0{i}-30' for i in range(1,6)], score_column='_warmup')
    except Exception:
        pass


def run_timing_variant(
    variant_id: str,
    rebal_months: list,
    hold_months: int,
    label: str,
    factors: list,
    result_buffer: Optional[list] = None,  # if set, append results here (bulk-insert mode)
    save_immediate: bool = False,           # write each result to bank immediately
    start: str = START_DATE,
    end: str = END_DATE,
) -> dict:
    """
    Load data for this timing variant and run all factors.

    result_buffer: if provided, completed result dicts are appended here instead of
                   written to bank. Caller does a single bulk INSERT at the end.
    save_immediate: if True, write each result to bank as it completes (safer but slower).
    Returns summary dict.
    """
    t0 = time.time()
    log.info(f"\n{'─'*60}")
    log.info(f"  Timing: [{variant_id}] {label}")
    log.info(f"  Months: {rebal_months} | Hold: {hold_months}mo")
    log.info(f"{'─'*60}")

    pack = load_all_data(
        mirror_db=MIRROR_DB,
        fund_db=FUND_DB,
        start_date=start,
        end_date=end,
        rebal_freq='semi-annual',
        hold_months=hold_months,
        min_price=5.0,
        min_adv_usd=1_000_000.0,
        custom_months=rebal_months,
    )
    load_s = time.time() - t0
    log.info(f"  Data loaded: {load_s:.1f}s | {pack.gpu_status()} | {len(pack.dates)} periods")

    if len(pack.dates) < 3:
        log.warning(f"  Only {len(pack.dates)} periods — skipping variant {variant_id}")
        return {'variant_id': variant_id, 'ok': 0, 'err': 0, 'skipped': True}

    _gpu_warmup(pack)

    ok_count = 0
    err_count = 0
    results_log = []

    for i, (sc, direction) in enumerate(factors, start=1):
        if sc not in pack.score_columns:
            continue
        try:
            result = run_factor_on_gpu(
                scores_gpu=pack.score_columns[sc],
                returns_gpu=pack.returns_gpu,
                valid_gpu=pack.valid_gpu,
                n_valid=pack.n_valid,
                dates=pack.dates,
                n_buckets=5,
                lower_better=(direction == 'lower_better'),
                hold_months=hold_months,
                score_column=sc,
                sector_gpu=pack.sector_gpu,
            )
            if result.get('status') == 'complete':
                result['run_label'] = (f"{sc} | 5Q | {hold_months}mo | R3K All-Cap | "
                                       f"{start[:4]}-{end[:4]} [CYC-006-{variant_id}]")
                result['config'] = {
                    'score_column':  sc,
                    'n_buckets':     5,
                    'hold_months':   hold_months,
                    'start_date':    start,
                    'end_date':      end,
                    'cap_tier':      f'all-{variant_id}',
                    'rebalance_freq': variant_id,
                    'rebal_months':  str(rebal_months),
                    'min_price':     5.0,
                    'cost_bps':      15.0,
                }
                result['score_column'] = sc

                if save_immediate:
                    save_factor_model(result, overwrite=True)
                elif result_buffer is not None:
                    result_buffer.append(result)   # accumulate for bulk insert

                fm = result.get('factor_metrics', {})
                ok_count += 1
                msg = (f"  [{i}/{len(factors)}] {sc} | {variant_id} | "
                       f"OBQ={fm.get('obq_fund_score',0):.3f} "
                       f"ICIR={fm.get('icir',0):.3f} "
                       f"Spread={fm.get('quintile_spread_cagr',0)*100:+.1f}%")
                results_log.append(msg)
                if i % 30 == 0:
                    elapsed = round(time.time() - t0)
                    log.info(f"  [{variant_id}] [{i}/{len(factors)}] ok={ok_count} "
                             f"elapsed={elapsed//60}m{elapsed%60}s")
            else:
                err_count += 1
        except Exception as exc:
            err_count += 1
            log.warning(f"  Exception {sc}: {str(exc)[:60]}")

    elapsed = time.time() - t0
    log.info(f"  [{variant_id}] DONE: {ok_count} ok | {err_count} err | {elapsed:.1f}s")
    return {'variant_id': variant_id, 'ok': ok_count, 'err': err_count,
            'load_s': load_s, 'total_s': elapsed, 'log': results_log}


def analyze_timing_results(bank_path: str) -> None:
    """
    Post-run analysis: for each factor and factor family,
    which timing variant produced the highest OBQ/ICIR?
    Produces the definitive timing recommendation table.
    """
    import duckdb
    from collections import defaultdict

    con = duckdb.connect(bank_path, read_only=True)
    log.info("\n" + "=" * 70)
    log.info("CYC-006 TIMING ANALYSIS — Optimal Rebalance Timing by Factor Family")
    log.info("=" * 70)

    # Pull all CYC-006 results
    rows = con.execute("""
        SELECT score_column, cap_tier, obq_fund_score, icir, staircase_score,
               quintile_spread_cagr, alpha_win_rate, bear_score, n_obs
        FROM factor_models
        WHERE run_label LIKE '%CYC-006%'
        ORDER BY score_column, cap_tier
    """).fetchall()

    if not rows:
        log.warning("No CYC-006 results found in bank yet.")
        con.close()
        return

    # Index by (score_column, variant)
    data = defaultdict(dict)  # data[sc][variant_id] = row
    for r in rows:
        sc = r[0]
        # cap_tier encodes timing as 'all-{variant_id}'
        variant = r[1].replace('all-', '') if r[1].startswith('all-') else r[1]
        data[sc][variant] = r

    # Per-factor: best timing
    log.info("\n--- PER-FACTOR OPTIMAL TIMING (by OBQ) ---")
    log.info(f"{'Factor':<35} {'Best Timing':>15} {'Best OBQ':>9} {'SA6 OBQ':>9} {'Delta':>8}")
    log.info("-" * 80)

    # Get SA6 (baseline) from CYC-003 for comparison
    baseline = {}
    baseline_rows = con.execute("""
        SELECT score_column, obq_fund_score FROM factor_models
        WHERE run_label LIKE '%CYC-003-GPU%' AND cap_tier='all'
        ORDER BY score_column
    """).fetchall()
    for r in baseline_rows:
        baseline[r[0]] = r[1]

    variant_wins = defaultdict(int)   # count how often each variant is best
    family_results = defaultdict(list)  # family → list of (variant, obq_delta)

    FACTOR_FAMILIES = {
        'momentum': ['cyc2_mom_3m','cyc2_mom_6m','cyc2_mom_12m','cyc4_skip_month_mom',
                     'cyc2_fip_6m','cyc2_fip_12m','momentum_score','momentum_sys_score',
                     'momentum_af_score','momentum_fip_score'],
        'quality':  ['cyc4_ocf_assets','cyc4_ebit_assets','cyc4_fscore','cyc4_accruals_ratio',
                     'quality_score','quality_score_universe','cyc2_roic','cyc2_roa','cyc2_roce',
                     'cyc2_gpa','cyc4_roe','cyc4_retained_earnings_ta'],
        'value':    ['cyc2_ps','cyc2_pe','cyc2_pb','cyc2_ev_ebitda','cyc2_pfcf','cyc2_fcf_yield',
                     'cyc4_ebit_ev','cyc4_fcf_ev','cyc4_sales_ev'],
        'growth':   ['cyc2_rev_cagr_3y','cyc2_rev_cagr_5y','cyc2_eps_cagr_3y','cyc2_eps_cagr_5y',
                     'cyc2_fcf_cagr_3y','growth_score'],
        'stability':['cyc4_eps_stability','cyc4_sales_stability','cyc4_dividend_yield',
                     'cyc4_net_payout_yield'],
        'composite':['jcn_full_composite','jcn_qarp','jcn_garp','jcn_alpha_trifecta',
                     'jcn_fortress','jcn_quality_momentum','jcn_value_momentum'],
        'risk':     ['cyc4_realized_vol','cyc4_market_beta','cyc4_idio_vol'],
    }
    sc_to_family = {}
    for fam, scs in FACTOR_FAMILIES.items():
        for sc in scs:
            sc_to_family[sc] = fam

    for sc in sorted(data.keys()):
        variants = data[sc]
        if not variants:
            continue
        best_v = max(variants, key=lambda v: variants[v][2] or -99)
        best_obq = variants[best_v][2]
        sa6_obq  = baseline.get(sc)
        delta    = (best_obq - sa6_obq) if (best_obq and sa6_obq) else None

        log.info(f"  {sc:<33} {best_v:>15} {best_obq:>9.3f} "
                 f"{(sa6_obq or 0):>9.3f} {('+' if delta and delta >= 0 else '')+f'{delta:.3f}' if delta else '—':>8}")

        variant_wins[best_v] += 1
        fam = sc_to_family.get(sc, 'other')
        family_results[fam].append((best_v, delta or 0))

    # Per-family summary
    log.info("\n--- PER-FAMILY OPTIMAL TIMING ---")
    log.info(f"{'Factor Family':<18} {'Best Timing':>15} {'Avg Delta vs SA6':>18} {'Count':>7}")
    log.info("-" * 65)
    for fam, results in sorted(family_results.items()):
        if not results: continue
        from collections import Counter
        timing_counts = Counter(v for v, _ in results)
        best_timing = timing_counts.most_common(1)[0][0]
        avg_delta = sum(d for _, d in results) / len(results)
        log.info(f"  {fam:<16} {best_timing:>15} {avg_delta:>+18.3f} {len(results):>7}")

    # Overall winner
    log.info(f"\n--- VARIANT WIN COUNTS (how often each timing was best per factor) ---")
    for v, n in sorted(variant_wins.items(), key=lambda x: -x[1]):
        bar = "█" * n
        log.info(f"  {v:<15} {n:>4}x  {bar}")

    con.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='CYC-006 Rebalance Timing Research')
    ap.add_argument('--dry-run',      action='store_true', help='No bank writes')
    ap.add_argument('--factor',       type=str, default=None, help='Test single factor only')
    ap.add_argument('--component',    type=str, default='all',
                    choices=['a','b','c','all'], help='Run only one component')
    ap.add_argument('--start',        default=START_DATE)
    ap.add_argument('--end',          default=END_DATE)
    ap.add_argument('--analyze-only', action='store_true', help='Just print analysis')
    ap.add_argument('--variant',      type=str, default=None, help='Run single variant only (e.g. Q-STD)')
    args = ap.parse_args()

    if args.analyze_only:
        analyze_timing_results(r'D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb')
        sys.exit(0)

    # Get factor list
    all_factors = _get_all_factors(r'D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb')
    if args.factor:
        # Single factor mode (dry run / smoke test)
        all_factors = [(sc, d) for sc, d in all_factors if sc == args.factor]
        if not all_factors:
            print(f"Factor '{args.factor}' not found in bank")
            sys.exit(1)
        log.info(f"Single-factor mode: {args.factor}")

    # Filter timing variants by component
    comp_filter = {'a': 'A', 'b': 'B', 'c': 'C', 'all': None}[args.component]
    variants = TIMING_VARIANTS
    if comp_filter:
        variants = [v for v in TIMING_VARIANTS if v[4] == comp_filter]
    if args.variant:
        variants = [v for v in TIMING_VARIANTS if v[0] == args.variant]

    total_jobs = len(variants) * len(all_factors)
    log.info(f"\n{'='*60}")
    log.info(f"CYC-006 Rebalance Timing Research")
    log.info(f"  Factors:  {len(all_factors)}")
    log.info(f"  Variants: {len(variants)} timing configs")
    log.info(f"  Jobs:     {total_jobs} total")
    log.info(f"  Dry-run:  {args.dry_run}")
    log.info(f"{'='*60}")

    t_global = time.time()
    all_summaries = []
    result_buffer = [] if not args.dry_run else None  # collect all results for bulk insert

    for vid, months, hold, label, component in variants:
        summary = run_timing_variant(
            variant_id=vid,
            rebal_months=months,
            hold_months=hold,
            label=label,
            factors=all_factors,
            result_buffer=result_buffer,   # accumulate in memory
            save_immediate=False,          # no per-factor writes during the run
            start=args.start,
            end=args.end,
        )
        all_summaries.append(summary)

    elapsed_total = time.time() - t_global
    total_ok  = sum(s['ok']  for s in all_summaries)
    total_err = sum(s['err'] for s in all_summaries)

    # ── Bulk INSERT — reuse ONE connection for all 1199 records ──────────────
    # Monkey-patch _get_bank to return a cached connection so all save_factor_model()
    # calls reuse the same open connection instead of open/close per record.
    if result_buffer:
        log.info(f"\nBulk inserting {len(result_buffer):,} results (single-connection batch)...")
        t_insert = time.time()
        try:
            from engine.strategy_bank import save_factor_model as _sfm, _get_bank

            # Open ONE shared connection — passed to every save_factor_model call.
            # The _shared_con parameter tells save_factor_model NOT to close after saving.
            _shared = _get_bank()
            saved = 0
            for result in result_buffer:
                try:
                    _sfm(result, overwrite=True, _shared_con=_shared)
                    saved += 1
                except Exception as exc:
                    log.warning(f"  Save failed: {str(exc)[:60]}")
                if saved % 300 == 0 and saved > 0:
                    log.info(f"  Inserted {saved}/{len(result_buffer)} ...")
            _shared.close()

            insert_s = time.time() - t_insert
            log.info(f"  Bulk insert complete: {saved}/{len(result_buffer)} in {insert_s:.1f}s "
                     f"({len(result_buffer)/max(insert_s,0.1):.0f} rec/s)")

        except Exception as exc:
            log.error(f"  Bulk insert failed: {exc}")
            import traceback; traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"  CYC-006 COMPLETE")
    print(f"  Results:  {total_ok} ok | {total_err} errors")
    print(f"  Variants: {len(variants)}")
    print(f"  Factors:  {len(all_factors)}")
    print(f"  TOTAL:    {elapsed_total:.1f}s ({elapsed_total/60:.1f}min)")
    print(f"{'='*60}\n")

    # Save results log
    if not args.dry_run:
        with open('cyc006_gpu_results.log', 'w', encoding='utf-8') as f:
            f.write(f"CYC-006 Run — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total: {total_ok} ok | {total_err} errors | {elapsed_total:.1f}s\n\n")
            for s in all_summaries:
                f.write(f"\n[{s['variant_id']}] {s['ok']} ok | {s['err']} err\n")
                for line in s.get('log', []):
                    f.write(line + '\n')
        log.info("Results saved to cyc006_gpu_results.log")

        # Auto-run analysis after full run
        log.info("\nGenerating timing analysis...")
        analyze_timing_results(r'D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb')
