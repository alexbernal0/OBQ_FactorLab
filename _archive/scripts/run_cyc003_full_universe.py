# -*- coding: utf-8 -*-
"""
run_cyc003_full_universe.py
============================
CYC-003 Full Universe Rerun — All 91 factors × 4 cap tiers
Uses parallel workers to maximize GPU/CPU utilization.

Universe: R3000 survivorship-bias free (via v_backtest_scores + price filter)
Cap tiers: all / mega ($200B+) / large ($10B-$200B) / mid ($2B-$10B)
Combos:    all / large only (slower)
New OBQ Master Factor Score applied to every run.

Usage:
    python run_cyc003_full_universe.py [--workers N] [--dry-run]
    python run_cyc003_full_universe.py --tier all      # single tier
    python run_cyc003_full_universe.py --cycle cyc001  # single cycle
"""
import sys, time, argparse, traceback, logging
from concurrent.futures import ProcessPoolExecutor, as_completed
sys.path.insert(0, '.')
sys.stdout.reconfigure(line_buffering=True)

from engine.cyc002_factors import CYC002_FACTORS, CYC002_COMBOS
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig, run_combo_backtest
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig, run_combo_portfolio_backtest
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model, get_all_portfolio_models

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Cap tier definitions ───────────────────────────────────────────────────────
CAP_TIERS_TO_RUN = {
    # tier_key: (cap_tier_arg, min_market_cap, label)
    'all':   ('all',   0,       'R3K All-Cap'),
    'mega':  ('mega',  200e9,   'Mega-Cap $200B+'),
    'large': ('large', 10e9,    'Large-Cap $10B+'),
    'mid':   ('mid',   2e9,     'Mid-Cap $2B-$10B'),
}

# ── Common config per tier ─────────────────────────────────────────────────────
def make_common(tier_key, start='1995-03-31', end='2024-12-31'):
    _, min_mc, _ = CAP_TIERS_TO_RUN[tier_key]
    return dict(
        start_date=start, end_date=end,
        n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
        cap_tier=CAP_TIERS_TO_RUN[tier_key][0],
        min_price=5.0, min_adv_usd=1_000_000.0,
        min_market_cap=min_mc,
        transaction_cost_bps=15.0,
    )

# ── CYC-001 composite score columns ───────────────────────────────────────────
CYC001_SCORES = [
    # (score_column, direction, display_name)
    ('jcn_full_composite',        'higher_better', 'JCN Full Composite'),
    ('jcn_qarp',                  'higher_better', 'JCN QARP'),
    ('jcn_garp',                  'higher_better', 'JCN GARP'),
    ('jcn_quality_momentum',      'higher_better', 'JCN Quality-Momentum'),
    ('jcn_value_momentum',        'higher_better', 'JCN Value-Momentum'),
    ('jcn_growth_quality_momentum','higher_better','JCN GQM'),
    ('jcn_fortress',              'higher_better', 'JCN Fortress'),
    ('jcn_alpha_trifecta',        'higher_better', 'JCN Alpha Trifecta'),
    ('quality_score',             'higher_better', 'OBQ Quality Score'),
    ('quality_score_universe',    'higher_better', 'OBQ Quality (Universe)'),
    ('value_score',               'higher_better', 'OBQ Value Score'),
    ('value_score_universe',      'higher_better', 'OBQ Value (Universe)'),
    ('growth_score',              'higher_better', 'OBQ Growth Score'),
    ('growth_score_universe',     'higher_better', 'OBQ Growth (Universe)'),
    ('finstr_score',              'higher_better', 'OBQ FinStr Score'),
    ('finstr_score_universe',     'higher_better', 'OBQ FinStr (Universe)'),
    ('momentum_score',            'higher_better', 'OBQ Momentum Score'),
    ('momentum_sys_score',        'higher_better', 'OBQ Momentum Systematic'),
    ('momentum_af_score',         'higher_better', 'OBQ Momentum AF'),
    ('momentum_fip_score',        'higher_better', 'OBQ Momentum FIP'),
    ('af_universe_score',         'higher_better', 'AF Universe Score'),
    ('moat_score',                'higher_better', 'Moat Score'),
    ('moat_rank',                 'higher_better', 'Moat Rank'),
    ('fundsmith_rank',            'lower_better',  'Fundsmith Rank'),
    ('longeq_rank',               'higher_better', 'LongEQ Rank'),
    ('rulebreaker_rank',          'lower_better',  'RuleBreaker Rank'),
]

TAG = "[CYC-003-R3K]"

def _silent_cb(m): pass

def run_single_factor(args):
    """
    Worker: COMPUTE only — no DB writes.
    Returns raw result dicts so main thread saves sequentially (avoids DuckDB write conflicts).
    """
    score_col, direction, display, tier_key, cycle, run_type = args
    sys.path.insert(0, r'C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab')
    from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
    from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig

    _, _, tier_label = CAP_TIERS_TO_RUN[tier_key]
    common = make_common(tier_key)

    results = {'score_col': score_col, 'tier': tier_key, 'cycle': cycle,
               'display': display, 'direction': direction, 'tier_label': tier_label}

    try:
        rf = run_factor_backtest(FactorBacktestConfig(
            score_column=score_col, score_direction=direction,
            run_label=f"{display} | 5Q | 6mo | {tier_label} | 1995-2024 {TAG}",
            **common
        ), cb=_silent_cb)
        results['factor_result'] = rf  # full result for saving
    except Exception as e:
        results['factor_result'] = {'status': 'error', 'error': str(e)[:120]}

    try:
        port_common = {k:v for k,v in common.items() if k not in ('n_buckets','hold_months')}
        rp = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score_col, score_direction=direction,
            run_label=f"{display} | Top-20 | {tier_label} | 1995-2024 {TAG}",
            top_n=20, sector_max=5, **port_common
        ), cb=_silent_cb)
        results['portfolio_result'] = rp
    except Exception as e:
        results['portfolio_result'] = {'status': 'error', 'error': str(e)[:120]}

    return results


def run_combo_pair(args):
    """Worker: COMPUTE combo only — no DB writes."""
    combo_id, fa, fb, display, source, tier_key = args
    sys.path.insert(0, r'C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab')
    from engine.factor_backtest import run_combo_backtest, FactorBacktestConfig
    from engine.portfolio_backtest import run_combo_portfolio_backtest, PortfolioBacktestConfig

    _, _, tier_label = CAP_TIERS_TO_RUN[tier_key]
    common_f = make_common(tier_key)
    common_p = {k:v for k,v in common_f.items() if k not in ('n_buckets','hold_months')}

    results = {'score_col': f"combo_{combo_id}", 'tier': tier_key,
               'cycle': 'cyc002-combo', 'display': display, 'tier_label': tier_label}

    try:
        rf = run_combo_backtest(
            combo_id=combo_id, factor_a_id=fa, factor_b_id=fb,
            display_name=display, source=source,
            cfg_override=FactorBacktestConfig(**common_f), cb=_silent_cb
        )
        rf['run_label'] = f"{display} | 5Q | 6mo | {tier_label} | 1995-2024 {TAG}"
        results['factor_result'] = rf
    except Exception as e:
        results['factor_result'] = {'status': 'error', 'error': str(e)[:120]}

    try:
        base_p = PortfolioBacktestConfig(
            score_column=f"combo_{combo_id}", top_n=20, sector_max=5, **common_p
        )
        rp = run_combo_portfolio_backtest(
            combo_id=combo_id, factor_a_id=fa, factor_b_id=fb,
            display_name=display, source=source, cfg=base_p, cb=_silent_cb
        )
        results['portfolio_result'] = rp
    except Exception as e:
        results['portfolio_result'] = {'status': 'error', 'error': str(e)[:120]}

    return results


def build_job_list(tier_filter=None, cycle_filter=None):
    """Build complete list of (args, label) tuples for all runs."""
    jobs = []
    tiers_composite = ['all','mega','large','mid']
    tiers_singles   = ['all','mega','large','mid']
    tiers_combos    = ['all','large']

    if tier_filter:
        tiers_composite = [t for t in tiers_composite if t == tier_filter]
        tiers_singles   = [t for t in tiers_singles   if t == tier_filter]
        tiers_combos    = [t for t in tiers_combos    if t == tier_filter]

    # CYC-001
    if not cycle_filter or cycle_filter == 'cyc001':
        for sc, direction, display in CYC001_SCORES:
            for tier in tiers_composite:
                jobs.append((
                    (sc, direction, display, tier, 'cyc001', 'factor'),
                    f"{display} [{tier}]",
                    'single'
                ))

    # CYC-002 singles
    if not cycle_filter or cycle_filter == 'cyc002':
        for sc, src_tbl, src_col, direction, display, group in CYC002_FACTORS:
            for tier in tiers_singles:
                jobs.append((
                    (sc, direction, display, tier, 'cyc002', 'factor'),
                    f"{display} [{tier}]",
                    'single'
                ))

        # CYC-002 combos
        for combo_id, fa, fb, display, source in CYC002_COMBOS:
            for tier in tiers_combos:
                jobs.append((
                    (combo_id, fa, fb, display, source, tier),
                    f"Combo {combo_id} [{tier}]",
                    'combo'
                ))

    return jobs


def run_all(workers=8, dry_run=False, tier_filter=None, cycle_filter=None):
    jobs = build_job_list(tier_filter=tier_filter, cycle_filter=cycle_filter)
    log.info(f"Total jobs: {len(jobs)} | Workers: {workers} | Dry run: {dry_run}")

    if dry_run:
        for args, label, jtype in jobs[:10]:
            log.info(f"  [{jtype}] {label}")
        log.info(f"  ... and {len(jobs)-10} more")
        return

    total      = len(jobs)
    ok_count   = 0
    err_count  = 0
    t_start    = time.time()
    results_log = []

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for args, label, jtype in jobs:
            if jtype == 'combo':
                fut = pool.submit(run_combo_pair, args)
            else:
                fut = pool.submit(run_single_factor, args)
            futures[fut] = (label, jtype)

        for i, fut in enumerate(as_completed(futures), 1):
            label, jtype = futures[fut]
            try:
                res = fut.result(timeout=600)
                sc    = res.get('score_col','?')
                tier  = res.get('tier','?')
                tlabel= res.get('tier_label','')
                fr    = res.get('factor_result',{})
                pr    = res.get('portfolio_result',{})

                f_ok = fr.get('status') == 'complete'
                p_ok = pr.get('status') == 'complete'

                if f_ok:
                    save_factor_model(fr, overwrite=True)
                    fm = fr.get('factor_metrics',{})
                if p_ok:
                    save_portfolio_model(pr, overwrite=True)
                    pm = pr.get('portfolio_metrics',{})

                if f_ok and p_ok:
                    ok_count += 1
                    msg = (f"OK  {sc} [{tier}] "
                           f"ICIR={fm.get('icir',0):.3f} "
                           f"OBQ={fm.get('obq_fund_score',0):.3f} "
                           f"CAGR={pm.get('cagr',0)*100:.1f}%")
                else:
                    err_count += 1
                    ferr = fr.get('error','')[:50] if not f_ok else ''
                    perr = pr.get('error','')[:50] if not p_ok else ''
                    msg = f"ERR {sc} [{tier}] F:{ferr} P:{perr}"
                results_log.append(msg)
            except Exception as e:
                err_count += 1
                msg = f"EXC {label}: {str(e)[:80]}"
                results_log.append(msg)

            if i % 20 == 0 or i == total:
                elapsed = round(time.time() - t_start)
                rate = i / max(elapsed, 1)
                eta = round((total - i) / max(rate, 0.1))
                log.info(f"Progress {i}/{total} | OK={ok_count} ERR={err_count} | "
                         f"{elapsed//60}m elapsed | ETA {eta//60}m{eta%60}s")

    elapsed_total = round(time.time() - t_start)
    log.info(f"\n{'='*65}")
    log.info(f"CYC-003 RERUN COMPLETE: {total} jobs in {elapsed_total//60}m{elapsed_total%60}s")
    log.info(f"  OK: {ok_count} | ERRORS: {err_count}")
    log.info(f"{'='*65}")

    # Save results log
    log_path = r'C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab\cyc003_results.log'
    with open(log_path, 'w') as f:
        f.write(f"CYC-003 Run completed {time.strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"Total: {total} | OK: {ok_count} | ERR: {err_count}\n\n")
        f.write('\n'.join(results_log))
    log.info(f"Results log: {log_path}")


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()

    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=8, help='Parallel workers (default 8)')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--tier', type=str, default=None, help='Single tier: all/mega/large/mid')
    ap.add_argument('--cycle', type=str, default=None, help='Single cycle: cyc001/cyc002')
    args = ap.parse_args()

    run_all(
        workers=args.workers,
        dry_run=args.dry_run,
        tier_filter=args.tier,
        cycle_filter=args.cycle,
    )
