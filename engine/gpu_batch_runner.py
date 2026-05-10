# -*- coding: utf-8 -*-
"""
engine/gpu_batch_runner.py
==========================
GPU Factor Engine — Batch Runner

Iterates all 326 CYC-003 factor runs on pre-loaded VRAM data.
No DuckDB queries inside the loop — data is already in GPU memory.

Architecture:
  1. data_pack = load_all_data(...)          # ~6s, ONE DuckDB query
  2. For each factor × cap_tier:
       scores_gpu = data_pack.score_columns[factor_name]   # pointer, ~0 cost
       cap_mask = data_pack.cap_mask(tier)                  # GPU mask, ~0 cost
       result = run_factor_on_gpu(scores_gpu, ...)          # ~200ms ALL on GPU
       save_factor_model(result)                            # ~5ms CPU
  3. For each combo × cap_tier:
       combo_scores = rank_average_gpu(factor_a, factor_b)  # GPU rank-average
       result = run_factor_on_gpu(combo_scores, ...)        # ~200ms
       save_factor_model(result)
  4. Optionally run portfolio backtest per job (can run after factor batch)

Target: 326 factor×tier runs in < 2 minutes on RTX 3090.
"""
from __future__ import annotations

import os
import time
import logging
from typing import Optional, Callable

import numpy as np

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import cupy as cp

from engine.gpu_data_loader import GPUDataPack, load_all_data, CYC002_COMBOS_DEF, MIRROR_DB
from engine.gpu_factor_compute import run_factor_on_gpu, run_combo_on_gpu
from engine.strategy_bank import save_factor_model

log = logging.getLogger(__name__)

# ── Job definitions ────────────────────────────────────────────────────────────

# CYC-001 composite scores (from v_backtest_scores)
CYC001_FACTORS = [
    ('jcn_full_composite',         'higher_better', 'JCN Full Composite',       'cyc001'),
    ('jcn_qarp',                   'higher_better', 'JCN QARP',                 'cyc001'),
    ('jcn_garp',                   'higher_better', 'JCN GARP',                 'cyc001'),
    ('jcn_quality_momentum',       'higher_better', 'JCN Quality-Momentum',     'cyc001'),
    ('jcn_value_momentum',         'higher_better', 'JCN Value-Momentum',       'cyc001'),
    ('jcn_growth_quality_momentum','higher_better', 'JCN GQM',                  'cyc001'),
    ('jcn_fortress',               'higher_better', 'JCN Fortress',             'cyc001'),
    ('jcn_alpha_trifecta',         'higher_better', 'JCN Alpha Trifecta',       'cyc001'),
    ('quality_score',              'higher_better', 'OBQ Quality Score',        'cyc001'),
    ('quality_score_universe',     'higher_better', 'OBQ Quality (Universe)',   'cyc001'),
    ('value_score',                'higher_better', 'OBQ Value Score',          'cyc001'),
    ('value_score_universe',       'higher_better', 'OBQ Value (Universe)',     'cyc001'),
    ('growth_score',               'higher_better', 'OBQ Growth Score',         'cyc001'),
    ('growth_score_universe',      'higher_better', 'OBQ Growth (Universe)',    'cyc001'),
    ('finstr_score',               'higher_better', 'OBQ FinStr Score',         'cyc001'),
    ('finstr_score_universe',      'higher_better', 'OBQ FinStr (Universe)',    'cyc001'),
    ('momentum_score',             'higher_better', 'OBQ Momentum Score',       'cyc001'),
    ('momentum_sys_score',         'higher_better', 'OBQ Momentum Systematic',  'cyc001'),
    ('momentum_af_score',          'higher_better', 'OBQ Momentum AF',          'cyc001'),
    ('momentum_fip_score',         'higher_better', 'OBQ Momentum FIP',         'cyc001'),
    ('af_universe_score',          'higher_better', 'AF Universe Score',        'cyc001'),
    ('moat_score',                 'higher_better', 'Moat Score',               'cyc001'),
    ('moat_rank',                  'higher_better', 'Moat Rank',                'cyc001'),
    ('fundsmith_rank',             'lower_better',  'Fundsmith Rank',           'cyc001'),
    ('longeq_rank',                'higher_better', 'LongEQ Rank',              'cyc001'),
    ('rulebreaker_rank',           'lower_better',  'RuleBreaker Rank',         'cyc001'),
]

# CYC-002 singles (from PROD_* tables, loaded by gpu_data_loader)
CYC002_SINGLES = [
    ('cyc2_roic',         'higher_better', 'ROIC',                    'cyc002'),
    ('cyc2_cash_roc',     'higher_better', 'Cash ROIC',               'cyc002'),
    ('cyc2_roce',         'higher_better', 'ROCE (Fundsmith)',         'cyc002'),
    ('cyc2_roa',          'higher_better', 'ROA',                     'cyc002'),
    ('cyc2_gpa',          'higher_better', 'GPA (Novy-Marx)',          'cyc002'),
    ('cyc2_op_margin',    'higher_better', 'Operating Margin',         'cyc002'),
    ('cyc2_fcf_margin',   'higher_better', 'FCF Margin',               'cyc002'),
    ('cyc2_gross_margin', 'higher_better', 'Gross Margin',             'cyc002'),
    ('cyc2_earn_quality', 'higher_better', 'Earnings Quality',         'cyc002'),
    ('cyc2_int_cov',      'higher_better', 'Interest Coverage',        'cyc002'),
    ('cyc2_fcf_debt',     'higher_better', 'FCF / Debt',               'cyc002'),
    ('cyc2_nd_ebitda',    'lower_better',  'Net Debt / EBITDA',        'cyc002'),
    ('cyc2_debt_assets',  'lower_better',  'Debt / Assets',            'cyc002'),
    ('cyc2_cash_assets',  'higher_better', 'Cash / Assets',            'cyc002'),
    ('cyc2_int_pct_op',   'lower_better',  'Interest % of Op Income',  'cyc002'),
    ('cyc2_capex_ocf',    'lower_better',  'CapEx % of OCF',           'cyc002'),
    ('cyc2_share_chg',    'lower_better',  'Share Count Change 5yr',   'cyc002'),
    ('cyc2_cash_conv',    'higher_better', 'Cash Conversion',          'cyc002'),
    ('cyc2_nd_ebit',      'lower_better',  'Net Debt / EBIT',          'cyc002'),
    ('cyc2_ev_ebitda',    'lower_better',  'EV/EBITDA',                'cyc002'),
    ('cyc2_pfcf',         'lower_better',  'P/FCF',                    'cyc002'),
    ('cyc2_fcf_yield',    'higher_better', 'FCF Yield',                'cyc002'),
    ('cyc2_ps',           'lower_better',  'P/Sales',                  'cyc002'),
    ('cyc2_pb',           'lower_better',  'P/Book',                   'cyc002'),
    ('cyc2_pe',           'lower_better',  'P/Earnings',               'cyc002'),
    ('cyc2_rev_growth_3y','higher_better', 'Revenue Growth 3yr',       'cyc002'),
    ('cyc2_rev_cagr_1y',  'higher_better', 'Revenue/Shr CAGR 1yr',     'cyc002'),
    ('cyc2_rev_cagr_3y',  'higher_better', 'Revenue/Shr CAGR 3yr',     'cyc002'),
    ('cyc2_rev_cagr_5y',  'higher_better', 'Revenue/Shr CAGR 5yr',     'cyc002'),
    ('cyc2_eps_cagr_1y',  'higher_better', 'EPS CAGR 1yr',             'cyc002'),
    ('cyc2_eps_cagr_3y',  'higher_better', 'EPS CAGR 3yr',             'cyc002'),
    ('cyc2_eps_cagr_5y',  'higher_better', 'EPS CAGR 5yr',             'cyc002'),
    ('cyc2_fcf_cagr_3y',  'higher_better', 'FCF/Shr CAGR 3yr',         'cyc002'),
    ('cyc2_fcf_cagr_5y',  'higher_better', 'FCF CAGR 5yr',             'cyc002'),
    ('cyc2_mom_3m',        'higher_better', '3-Month Momentum',         'cyc002'),
    ('cyc2_mom_6m',        'higher_better', '6-Month Momentum',         'cyc002'),
    ('cyc2_mom_12m',       'higher_better', '12-Month Momentum',        'cyc002'),
    ('cyc2_fip_6m',        'higher_better', 'FIP 6-Month',              'cyc002'),
    ('cyc2_fip_12m',       'higher_better', 'FIP 12-Month',             'cyc002'),
    ('cyc2_sys_score',     'higher_better', 'Systematic Score',         'cyc002'),
    ('cyc2_rd_ratio',      'higher_better', 'R&D Ratio',                'cyc002'),
    ('cyc2_moat_intangible','higher_better','Intangible Assets Moat',   'cyc002'),
    ('cyc2_moat_switching', 'higher_better','Switching Cost Moat',      'cyc002'),
    ('cyc2_moat_network',   'higher_better','Network Effect Moat',      'cyc002'),
    ('cyc2_moat_cost',      'higher_better','Cost Advantage Moat',      'cyc002'),
    ('cyc2_moat_scale',     'higher_better','Efficient Scale Moat',     'cyc002'),
]

# Cap tier definitions for CYC-003
# Each tuple: (cap_tier_arg, min_market_cap, max_market_cap, label)
#   cap_tier_arg: passed to GPUDataPack.cap_mask() — usually 'all'
#   min_market_cap: floor in USD (0 = no floor)
#   max_market_cap: ceiling in USD (0 = no ceiling)
DEFAULT_CAP_TIERS = {
    'all':   ('all',  0,    0,        'R3K All-Cap'),
    'large': ('all',  10e9, 0,        'Large-Cap $10B+'),
    'mid':   ('all',  2e9,  10e9,     'Mid-Cap $2B-$10B'),
}

# Combo tiers (all three for full encyclopedia v2 coverage)
COMBO_TIERS = {'all', 'large', 'mid'}


# ── Internal helpers ───────────────────────────────────────────────────────────

def _make_run_label(display: str, tier_label: str, cycle: str, tag: str = '[CYC-003-GPU]') -> str:
    return f"{display} | 5Q | 6mo | {tier_label} | 1995-2024 {tag}"


def _build_config_dict(score_col: str, cap_tier: str, tier_key: str) -> dict:
    # cap_tier in the bank schema MUST be the tier_key (all/large/mid) so each
    # tier produces a distinct strategy_id. Previously this was set to
    # cap_tier_arg ('all') which collided across all three tiers.
    return {
        "score_column":   score_col,
        "n_buckets":      5,
        "hold_months":    6,
        "start_date":     "1995-03-31",
        "end_date":       "2024-12-31",
        "min_price":      5.0,
        "min_adv_usd":    1_000_000.0,
        "cap_tier":       tier_key,
        "rebalance_freq": "semi-annual",
        "cost_bps":       15.0,
    }


# ── Main batch runner ──────────────────────────────────────────────────────────

def run_cyc003_gpu(
    data_pack: GPUDataPack,
    factor_list: Optional[list] = None,
    cap_tiers: Optional[dict] = None,
    combo_list: Optional[list] = None,
    run_label_tag: str = '[CYC-003-GPU]',
    save_to_bank: bool = True,
    cb: Optional[Callable] = None,
    skip_missing_scores: bool = True,
) -> dict:
    """
    Main entry point for CYC-003 GPU batch run.

    Args:
        data_pack:    GPUDataPack from load_all_data() — all VRAM data
        factor_list:  list of (score_col, direction, display, cycle) — default: all 72 singles
        cap_tiers:    dict of tier_key → (cap_tier_arg, min_mc, label) — default: all+large
        combo_list:   list of (combo_id, fa, fb) — default: CYC002_COMBOS_DEF
        run_label_tag: suffix for run labels
        save_to_bank: if True, save each result to strategy bank
        cb:           optional callback(msg) for progress
        skip_missing_scores: skip factors not in data_pack.score_columns (log warning)

    Returns:
        dict: ok_count, err_count, total, elapsed_s, results_log
    """
    if factor_list is None:
        factor_list = CYC001_FACTORS + CYC002_SINGLES
    if cap_tiers is None:
        cap_tiers = DEFAULT_CAP_TIERS
    if combo_list is None:
        combo_list = [(cid, fa, fb, f"combo_{cid}", 'CYC002') for cid, fa, fb in CYC002_COMBOS_DEF]

    def _cb(msg):
        log.info(msg)
        if cb:
            try: cb(msg)
            except Exception: pass

    t_start = time.time()
    ok_count  = 0
    err_count = 0
    results_log = []

    total_singles = len(factor_list) * len(cap_tiers)
    combo_tiers_active = {k: v for k, v in cap_tiers.items() if k in COMBO_TIERS}
    total_combos = len(combo_list) * len(combo_tiers_active)
    total_jobs = total_singles + total_combos

    _cb(f"[BatchRunner] CYC-003 GPU Run")
    _cb(f"  Singles: {len(factor_list)} x {len(cap_tiers)} tiers = {total_singles}")
    _cb(f"  Combos:  {len(combo_list)} x {len(combo_tiers_active)} tiers = {total_combos}")
    _cb(f"  Total jobs: {total_jobs}")
    _cb(f"  VRAM pack: {data_pack.gpu_status()}")

    # ── GPU JIT warmup ────────────────────────────────────────────────────────
    # CuPy compiles CUDA kernels on first use (~1.3s). Run a tiny dummy factor
    # now so all kernels are cached before the real batch starts.
    _cb("  Warming up CUDA JIT kernels...")
    t_warm = time.time()
    _dummy_s = cp.zeros((5, 50), dtype=cp.float64)
    _dummy_r = cp.zeros((5, 50), dtype=cp.float64)
    _dummy_v = cp.ones((5, 50),  dtype=bool)
    _dummy_n = cp.full(5, 50,    dtype=cp.int32)
    _dummy_dates = [f'2000-0{i}-30' for i in range(1, 6)]
    try:
        run_factor_on_gpu(_dummy_s, _dummy_r, _dummy_v, _dummy_n,
                          _dummy_dates, score_column='_warmup')
    except Exception:
        pass  # warmup errors are non-fatal
    _cb(f"  JIT warmup done in {(time.time()-t_warm)*1000:.0f}ms — kernels cached")

    job_num = 0

    # ── Single factor jobs ────────────────────────────────────────────────────
    for score_col, direction, display, cycle in factor_list:
        # Get pre-loaded VRAM array (pointer swap, zero cost)
        if score_col not in data_pack.score_columns:
            if skip_missing_scores:
                _cb(f"  SKIP {score_col} — not in data_pack (loaded={list(data_pack.score_columns.keys())[:3]}...)")
                for _ in cap_tiers:
                    err_count += 1
                    results_log.append(f"SKIP {score_col} [not loaded]")
                    job_num += 1
                continue
            else:
                raise KeyError(f"Score column '{score_col}' not found in data_pack")

        scores_gpu = data_pack.score_columns[score_col]
        lower_better = (direction == 'lower_better')

        for tier_key, tier_tuple in cap_tiers.items():
            # Backward-compat: accept 3-tuple (arg,min,label) or 4-tuple (arg,min,max,label)
            if len(tier_tuple) == 4:
                cap_tier_arg, min_mc, max_mc, tier_label = tier_tuple
            else:
                cap_tier_arg, min_mc, tier_label = tier_tuple
                max_mc = 0
            job_num += 1

            try:
                # Build cap mask: base cap tier + min_mc floor + max_mc ceiling
                cap_mask = data_pack.cap_mask(cap_tier_arg)
                if min_mc > 0:
                    cap_mask = cap_mask & (data_pack.market_cap_gpu >= min_mc)
                if max_mc > 0:
                    cap_mask = cap_mask & (data_pack.market_cap_gpu <= max_mc)

                run_label = _make_run_label(display, tier_label, cycle, run_label_tag)
                config = _build_config_dict(score_col, cap_tier_arg, tier_key)

                # GPU compute — ALL 13 steps + sector attribution
                result = run_factor_on_gpu(
                    scores_gpu=scores_gpu,
                    returns_gpu=data_pack.returns_gpu,
                    valid_gpu=data_pack.valid_gpu,
                    n_valid=data_pack.n_valid,
                    dates=data_pack.dates,
                    n_buckets=5,
                    lower_better=lower_better,
                    hold_months=6,
                    score_column=score_col,
                    cap_mask_gpu=cap_mask,
                    sector_gpu=getattr(data_pack, 'sector_gpu', None),
                )

                if result.get('status') == 'complete':
                    result['run_label'] = run_label
                    result['config']    = config
                    result['score_column'] = score_col

                    if save_to_bank:
                        sid = save_factor_model(result, overwrite=True)
                    else:
                        sid = f"DRY-{score_col}-{tier_key}"

                    fm = result.get('factor_metrics', {})
                    ok_count += 1
                    msg = (f"OK [{job_num}/{total_jobs}] {score_col} [{tier_key}] "
                           f"ICIR={fm.get('icir', 0):.3f} OBQ={fm.get('obq_fund_score', 0):.3f} "
                           f"GPU={result.get('elapsed_gpu_ms', 0):.0f}ms")
                else:
                    err_count += 1
                    msg = f"ERR [{job_num}/{total_jobs}] {score_col} [{tier_key}]: {result.get('error', 'unknown')}"

            except Exception as exc:
                err_count += 1
                msg = f"EXC [{job_num}/{total_jobs}] {score_col} [{tier_key}]: {str(exc)[:80]}"
                log.exception(f"Exception in {score_col} [{tier_key}]")

            results_log.append(msg)

            if job_num % 20 == 0 or job_num <= 3:
                elapsed = round(time.time() - t_start)
                rate = job_num / max(elapsed, 0.1)
                eta  = round((total_jobs - job_num) / max(rate, 0.01))
                _cb(f"  [{job_num}/{total_jobs}] ok={ok_count} err={err_count} "
                    f"elapsed={elapsed//60}m{elapsed%60}s "
                    f"ETA={eta//60}m{eta%60}s")

    # ── Combo jobs ────────────────────────────────────────────────────────────
    # Build direction lookup from factor_list
    dir_map = {sc: d for sc, d, _, _ in factor_list}
    # Also from CYC001 defaults
    for sc, d, _, _ in CYC001_FACTORS:
        dir_map.setdefault(sc, d)
    for sc, d, _, _ in CYC002_SINGLES:
        dir_map.setdefault(sc, d)

    for combo_entry in combo_list:
        if len(combo_entry) == 5:
            combo_id, fa, fb, display, source = combo_entry
        elif len(combo_entry) == 3:
            combo_id, fa, fb = combo_entry
            display = f"combo_{combo_id}"
            source  = 'CYC002'
        else:
            err_count += 1
            results_log.append(f"ERR combo: unexpected tuple length {len(combo_entry)}")
            continue

        # Get VRAM arrays for both factors
        if fa not in data_pack.score_columns or fb not in data_pack.score_columns:
            for _ in combo_tiers_active:
                err_count += 1
                results_log.append(f"SKIP combo_{combo_id} — {fa} or {fb} not in data_pack")
                job_num += 1
            continue

        S_a_gpu = data_pack.score_columns[fa]
        S_b_gpu = data_pack.score_columns[fb]
        dir_a   = dir_map.get(fa, 'higher_better')
        dir_b   = dir_map.get(fb, 'higher_better')

        for tier_key, tier_tuple in combo_tiers_active.items():
            # Backward-compat: accept 3-tuple or 4-tuple
            if len(tier_tuple) == 4:
                cap_tier_arg, min_mc, max_mc, tier_label = tier_tuple
            else:
                cap_tier_arg, min_mc, tier_label = tier_tuple
                max_mc = 0
            job_num += 1

            try:
                cap_mask = data_pack.cap_mask(cap_tier_arg)
                if min_mc > 0:
                    cap_mask = cap_mask & (data_pack.market_cap_gpu >= min_mc)
                if max_mc > 0:
                    cap_mask = cap_mask & (data_pack.market_cap_gpu <= max_mc)

                run_label = _make_run_label(display, tier_label, 'cyc002_combo', run_label_tag)
                config = _build_config_dict(f"combo_{combo_id}", cap_tier_arg, tier_key)
                config['combo_id'] = combo_id
                config['factor_a'] = fa
                config['factor_b'] = fb

                result = run_combo_on_gpu(
                    S_a_gpu=S_a_gpu,
                    S_b_gpu=S_b_gpu,
                    dir_a=dir_a,
                    dir_b=dir_b,
                    returns_gpu=data_pack.returns_gpu,
                    valid_gpu=data_pack.valid_gpu,
                    n_valid=data_pack.n_valid,
                    dates=data_pack.dates,
                    combo_id=combo_id,
                    n_buckets=5,
                    hold_months=6,
                    cap_mask_gpu=cap_mask,
                    sector_gpu=getattr(data_pack, 'sector_gpu', None),
                )

                if result.get('status') == 'complete':
                    result['run_label'] = run_label
                    result['config']    = config
                    result['score_column'] = f"combo_{combo_id}"

                    if save_to_bank:
                        save_factor_model(result, overwrite=True)

                    fm = result.get('factor_metrics', {})
                    ok_count += 1
                    msg = (f"OK [{job_num}/{total_jobs}] combo_{combo_id} [{tier_key}] "
                           f"ICIR={fm.get('icir', 0):.3f} OBQ={fm.get('obq_fund_score', 0):.3f} "
                           f"GPU={result.get('elapsed_gpu_ms', 0):.0f}ms")
                else:
                    err_count += 1
                    msg = f"ERR [{job_num}/{total_jobs}] combo_{combo_id} [{tier_key}]"

            except Exception as exc:
                err_count += 1
                msg = f"EXC [{job_num}/{total_jobs}] combo_{combo_id} [{tier_key}]: {str(exc)[:80]}"
                log.exception(f"Exception in combo_{combo_id} [{tier_key}]")

            results_log.append(msg)

            if job_num % 10 == 0:
                elapsed = round(time.time() - t_start)
                _cb(f"  [combo {job_num}/{total_jobs}] ok={ok_count} err={err_count} elapsed={elapsed//60}m{elapsed%60}s")

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed_total = round(time.time() - t_start)
    _cb(f"\n[BatchRunner] COMPLETE: {ok_count}/{total_jobs} ok | {err_count} errors | "
        f"{elapsed_total//60}m{elapsed_total%60}s | {elapsed_total/max(total_jobs,1):.1f}s/job")

    return {
        "ok_count":    ok_count,
        "err_count":   err_count,
        "total":       total_jobs,
        "elapsed_s":   elapsed_total,
        "results_log": results_log,
    }


# ── Parity test helper ─────────────────────────────────────────────────────────

def run_parity_check(
    data_pack: GPUDataPack,
    parity_factors: Optional[list] = None,
) -> dict:
    """
    Run 5 parity factors through GPU engine.
    Returns results dict for external comparison with CPU engine.

    Default parity factors: jcn_full_composite, cyc2_ps, cyc2_roic,
    quality_score_universe, combo_T4
    """
    if parity_factors is None:
        parity_factors = [
            ('jcn_full_composite', 'higher_better'),
            ('cyc2_ps',            'lower_better'),
            ('cyc2_roic',          'higher_better'),
            ('quality_score_universe', 'higher_better'),
        ]

    results = {}
    for score_col, direction in parity_factors:
        if score_col not in data_pack.score_columns:
            log.warning(f"Parity: {score_col} not in data_pack — skip")
            continue
        result = run_factor_on_gpu(
            scores_gpu=data_pack.score_columns[score_col],
            returns_gpu=data_pack.returns_gpu,
            valid_gpu=data_pack.valid_gpu,
            n_valid=data_pack.n_valid,
            dates=data_pack.dates,
            lower_better=(direction == 'lower_better'),
            hold_months=6,
            score_column=score_col,
        )
        results[score_col] = result

    # Parity combo T4: cyc2_ps + cyc2_fcf_margin
    if ('cyc2_ps' in data_pack.score_columns and
            'cyc2_fcf_margin' in data_pack.score_columns):
        result = run_combo_on_gpu(
            S_a_gpu=data_pack.score_columns['cyc2_ps'],
            S_b_gpu=data_pack.score_columns['cyc2_fcf_margin'],
            dir_a='lower_better',
            dir_b='higher_better',
            returns_gpu=data_pack.returns_gpu,
            valid_gpu=data_pack.valid_gpu,
            n_valid=data_pack.n_valid,
            dates=data_pack.dates,
            combo_id='T4',
            hold_months=6,
        )
        results['combo_T4'] = result

    return results


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    db = sys.argv[1] if len(sys.argv) > 1 else MIRROR_DB

    print(f"\n[BatchRunner smoke test] DB: {db}")
    import cupy as _cp
    dev = _cp.cuda.Device()
    free, total = dev.mem_info
    print(f"GPU: cc={dev.compute_capability}  VRAM {free/1e9:.1f}/{total/1e9:.1f}GB")

    # Load data
    print("\n--- Loading data pack ---")
    pack = load_all_data(mirror_db=db, start_date='1995-03-31', end_date='2024-12-31')
    print(pack.gpu_status())

    # Run a small subset (3 factors × 2 tiers = 6 jobs)
    print("\n--- Running 3 factors × 2 tiers ---")
    t0 = time.time()
    result = run_cyc003_gpu(
        data_pack=pack,
        factor_list=[
            ('jcn_full_composite',    'higher_better', 'JCN Full Composite',    'cyc001'),
            ('cyc2_roic',             'higher_better', 'ROIC',                  'cyc002'),
            ('quality_score_universe','higher_better', 'Quality (Universe)',    'cyc001'),
        ],
        cap_tiers=DEFAULT_CAP_TIERS,
        combo_list=[],
        save_to_bank=True,
    )
    elapsed = time.time() - t0

    print(f"\nResult: {result['ok_count']}/{result['total']} ok | {result['err_count']} errors | {elapsed:.1f}s")
    for msg in result['results_log']:
        print(f"  {msg}")
