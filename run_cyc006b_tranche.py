# -*- coding: utf-8 -*-
"""
run_cyc006b_tranche.py — CYC-006b Staggered Tranche Rebalancing Research
=========================================================================
Tests 9 tranche configurations (2-tranche and 4-tranche) across all 139 factors.

NO new GPU runs needed — blends existing CYC-006 per-period returns directly.

Method:
  For each factor and tranche config:
  1. Load bucket_equity_json for each constituent variant from CYC-006 bank
  2. Reconstruct per-period returns: r[t] = equity[t+1] / equity[t] - 1
  3. Align dates across tranches on a common semi-annual observation grid
  4. Blend: blended_return[t] = weighted average of each tranche's 6-month return
  5. Compound into blended equity curve
  6. Compute OBQ/ICIR/Staircase/Alpha metrics on blended series
  7. Save to bank with cap_tier encoding the tranche config

Labelling:
  run_label: "{factor} | 5Q | 12mo | {tranche_id} | 1995-2024 [CYC-006b-TRANCHE]"
  cap_tier:  "2T-JUN-DEC" / "4T-MAR-JUN-SEP-DEC" etc.

Usage:
    python run_cyc006b_tranche.py --dry-run    # test 1 factor
    python run_cyc006b_tranche.py              # full run
    python run_cyc006b_tranche.py --analyze    # print results analysis
"""
from __future__ import annotations

import os, sys, time, json, math, logging, argparse
from typing import Optional
from collections import defaultdict

import numpy as np

sys.path.insert(0, '.')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

import duckdb
from engine.strategy_bank import save_factor_model, _get_bank

BANK = r'D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb'

# ── Tranche configurations ─────────────────────────────────────────────────────
# Format: (config_id, weights_list, constituent_variants_list, description)
# Constituent variants must exist in CYC-006 bank (cap_tier = 'all-{variant}')
TRANCHE_CONFIGS = [
    # 2-Tranche, 6-month offset (4 configs)
    ('2T-JUN-DEC',   [0.5, 0.5], ['A-Q2', 'A-Q4'],   '2-Tranche 50/50: Jun30+Dec31 annual (6mo offset)'),
    ('2T-MAR-SEP',   [0.5, 0.5], ['A-Q1', 'A-Q3'],   '2-Tranche 50/50: Mar31+Sep30 annual (6mo offset)'),
    ('2T-JAN-JUL',   [0.5, 0.5], ['SA-JAN-JUL', 'SA-JAN-JUL'],  # use SA variant
                     '2-Tranche 50/50: Jan31+Jul31 annual (6mo offset)'),
    ('2T-APR-OCT',   [0.5, 0.5], ['SA-APR-OCT', 'SA-APR-OCT'],
                     '2-Tranche 50/50: Apr30+Oct31 annual (6mo offset)'),
    # 2-Tranche, 3-month offset (2 configs)
    ('2T-JUN-SEP',   [0.5, 0.5], ['A-Q2', 'A-Q3'],   '2-Tranche 50/50: Jun30+Sep30 annual (3mo offset)'),
    ('2T-MAR-JUN',   [0.5, 0.5], ['A-Q1', 'A-Q2'],   '2-Tranche 50/50: Mar31+Jun30 annual (3mo offset)'),
    # 4-Tranche, quarterly spacing (3 configs)
    ('4T-MAR-JUN-SEP-DEC', [0.25]*4, ['A-Q1','A-Q2','A-Q3','A-Q4'],
                     '4-Tranche 25/25/25/25: Mar+Jun+Sep+Dec annual'),
    ('4T-JAN-APR-JUL-OCT', [0.25]*4, ['SA-JAN-JUL','SA-APR-OCT','SA-JAN-JUL','SA-APR-OCT'],
                     '4-Tranche 25/25/25/25: Jan+Apr+Jul+Oct (1mo pre-quarter)'),
    ('4T-FEB-MAY-AUG-NOV', [0.25]*4, ['SA-MAR-SEP','Q-OFF2','SA-MAR-SEP','Q-OFF2'],
                     '4-Tranche 25/25/25/25: Feb+May+Aug+Nov (2mo offset)'),
]

# ── OBQ metric computation ─────────────────────────────────────────────────────

def _cagr(equity: list, periods_per_year: float = 2.0) -> float:
    if len(equity) < 2 or equity[0] <= 0: return 0.0
    years = (len(equity) - 1) / periods_per_year
    return float((equity[-1] / equity[0]) ** (1.0 / max(years, 0.1)) - 1.0) if years > 0 else 0.0

def _sharpe(rets: list, periods_per_year: float = 2.0, rf: float = 0.04) -> float:
    r = np.array(rets)
    if len(r) < 4: return 0.0
    excess = r - rf / periods_per_year
    std = float(r.std())
    return float(excess.mean() / std * np.sqrt(periods_per_year)) if std > 1e-10 else 0.0

def _max_dd(equity: list) -> float:
    eq = np.array(equity)
    peak = np.maximum.accumulate(eq)
    dd = eq / np.where(peak > 0, peak, 1.0) - 1.0
    return float(dd.min())

def _ic_from_quintiles(returns_by_bucket: dict, n_buckets: int = 5) -> list:
    """
    Pearson correlation of return-rank vs bucket return per period.
    Convention matches GPU engine: POSITIVE IC when Q1 (best rank) has highest return.
    Uses inverted bucket numbers so rank 1 (best) → high correlation value.
    """
    n_periods = max(len(v) for v in returns_by_bucket.values())
    ics = []
    for t in range(n_periods):
        # Inverted: bucket 1 (Q1, best) gets weight n_buckets, bucket 5 gets weight 1
        # This makes correlation POSITIVE when Q1 outperforms Q5
        inv_ranks = list(range(n_buckets, 0, -1))   # [5,4,3,2,1] for n=5
        bucket_rets = [returns_by_bucket.get(str(b), [0]*n_periods)[t] for b in range(1, n_buckets+1)]
        if all(r == 0 for r in bucket_rets): continue
        try:
            ic = float(np.corrcoef(inv_ranks, bucket_rets)[0, 1])
            if not math.isnan(ic): ics.append(ic)
        except Exception: pass
    return ics

def compute_obq(ic_vals: list, staircase: float, alpha_win: float,
                avg_alpha: float, ic_hit: float) -> float:
    if not ic_vals: return -0.1
    icir = float(np.mean(ic_vals) / max(np.std(ic_vals), 1e-6))
    return float(np.clip(
        0.25 * math.tanh(icir / 1.5)
      + 0.25 * math.tanh(staircase / 0.10)
      + 0.20 * alpha_win
      + 0.20 * math.tanh(avg_alpha / 0.05)
      + 0.10 * np.clip((ic_hit - 0.5) * 2.0, -1.0, 1.0),
        -1.0, 1.0))

# ── Load CYC-006 data ──────────────────────────────────────────────────────────

def load_cyc006_results(con: duckdb.DuckDBPyConnection) -> dict:
    """Load all CYC-006 bucket_equity + dates per (factor, variant)."""
    rows = con.execute("""
        SELECT score_column, cap_tier,
               bucket_equity_json, dates_json, n_obs,
               obq_fund_score, icir, quintile_spread_cagr
        FROM factor_models
        WHERE run_label LIKE '%CYC-006%'
    """).fetchall()

    data = {}   # data[(sc, variant)] = {equity, dates, meta}
    for r in rows:
        sc      = r[0]
        variant = r[1].replace('all-', '')
        be_json = r[2]
        dt_json = r[3]
        if not be_json or not dt_json: continue
        try:
            be    = json.loads(be_json)   # {'1': [1.0, ...], '2': ...}
            dates = json.loads(dt_json)   # ['1995-06-30', ...]
        except Exception: continue
        data[(sc, variant)] = {
            'equity': be, 'dates': dates, 'n_obs': r[4],
            'baseline_obq': r[5], 'baseline_icir': r[6], 'baseline_spread': r[7],
        }
    return data

def get_bucket_period_returns(equity_dict: dict, n_buckets: int = 5) -> dict:
    """Convert cumulative equity curves to per-period returns."""
    result = {}
    for b in range(1, n_buckets + 1):
        eq = equity_dict.get(str(b), [])
        if len(eq) < 2:
            result[str(b)] = []
            continue
        rets = [float(eq[t+1] / eq[t] - 1.0) if eq[t] > 0 else 0.0
                for t in range(len(eq) - 1)]
        result[str(b)] = rets
    return result

# ── Core blending function ─────────────────────────────────────────────────────

def blend_tranches(
    constituents: list,   # list of (equity_dict, dates_list, weight)
    n_buckets: int = 5,
    periods_per_year: float = 2.0,
) -> Optional[dict]:
    """
    Blend multiple tranche results into a single portfolio-level result.

    For each calendar period, the blended portfolio holds each tranche
    with the specified weight. The 6-month return is the weighted average
    of each tranche's 6-month return in that period.

    Returns: dict with OBQ metrics on the blended equity curve.
    """
    if not constituents: return None

    # Build a unified date set — union of all constituent dates
    all_date_sets = [set(c[1]) for c in constituents]
    all_dates = sorted(set().union(*all_date_sets))

    if len(all_dates) < 6: return None

    # For each bucket, build per-period blended return time series
    blended_bucket_rets: dict = {str(b): [] for b in range(1, n_buckets + 1)}
    valid_date_count = 0

    for date_str in all_dates:
        # For each bucket, get weighted average return across constituents
        bucket_blend = {}
        total_weight = 0.0

        for equity_dict, dates, weight in constituents:
            # Find the most recent date in this constituent at or before date_str
            avail = [d for d in dates if d <= date_str]
            if not avail: continue

            # Get period index for this date
            constituent_dates = dates
            if date_str in constituent_dates:
                t_idx = constituent_dates.index(date_str)
            else:
                # Use most recent available date's return
                most_recent = max(avail)
                t_idx = constituent_dates.index(most_recent)

            # Get per-period returns
            per_period = get_bucket_period_returns(equity_dict, n_buckets)

            for b in range(1, n_buckets + 1):
                bid = str(b)
                rets = per_period.get(bid, [])
                if t_idx < len(rets):
                    bucket_blend[bid] = bucket_blend.get(bid, 0.0) + weight * rets[t_idx]
                    if bid == '1': total_weight += weight

        if total_weight < 0.4: continue  # insufficient data for this period

        # Normalize and record
        valid_date_count += 1
        for b in range(1, n_buckets + 1):
            bid = str(b)
            blended_bucket_rets[bid].append(
                bucket_blend.get(bid, 0.0) / max(total_weight, 1e-6)
            )

    if valid_date_count < 6: return None

    # Build equity curves from blended returns
    blended_equity = {}
    for bid, rets in blended_bucket_rets.items():
        eq = [1.0]
        for r in rets:
            eq.append(eq[-1] * (1 + r))
        blended_equity[bid] = eq

    # Compute metrics
    n_periods = valid_date_count
    n_buckets_actual = 5
    q1_eq  = blended_equity.get('1', [1.0])
    q5_eq  = blended_equity.get(str(n_buckets_actual), [1.0])
    q1_rets = blended_bucket_rets.get('1', [])

    # CAGR per bucket
    bucket_cagrs = {}
    for b in range(1, n_buckets_actual + 1):
        bid = str(b)
        eq = blended_equity.get(bid, [1.0, 1.0])
        bucket_cagrs[bid] = _cagr(eq, periods_per_year)

    q1_cagr = bucket_cagrs.get('1', 0.0)
    qn_cagr = bucket_cagrs.get(str(n_buckets_actual), 0.0)
    spread  = q1_cagr - qn_cagr

    # Monotonicity + staircase
    cagrs_list = [bucket_cagrs.get(str(b), 0.0) for b in range(1, n_buckets_actual + 1)]
    steps = [cagrs_list[i] - cagrs_list[i+1] for i in range(len(cagrs_list) - 1)]
    monotonicity = float(sum(1 for s in steps if s > 0) / max(len(steps), 1))
    step_std  = float(np.std(steps)) if steps else 0.0
    step_mean = float(np.mean([abs(s) for s in steps])) if steps else 0.0
    step_unif = float(1.0 / (1.0 + (step_std / max(step_mean, 1e-6)) * 0.5)) if step_mean > 0 else 1.0
    staircase = spread * monotonicity * step_unif

    # IC from quintile rank vs return correlation
    ic_vals = _ic_from_quintiles(blended_bucket_rets, n_buckets_actual)
    ic_mean  = float(np.mean(ic_vals)) if ic_vals else 0.0
    ic_std   = float(np.std(ic_vals))  if ic_vals else 1.0
    ic_hit   = float(np.mean([v > 0 for v in ic_vals])) if ic_vals else 0.5
    icir     = ic_mean / max(ic_std, 1e-6)

    # Universe returns (equal weight of all 5 buckets)
    univ_rets = []
    for t in range(n_periods):
        r = np.mean([blended_bucket_rets.get(str(b), [0]*n_periods)[t]
                     for b in range(1, n_buckets_actual + 1)
                     if t < len(blended_bucket_rets.get(str(b), []))])
        univ_rets.append(float(r))
    univ_eq = [1.0]
    for r in univ_rets:
        univ_eq.append(univ_eq[-1] * (1 + r))

    # Annual alpha and win rate (group 6-month periods into years)
    pairs_per_yr = max(1, round(periods_per_year))
    yr_q1   = [float(np.prod([1+q1_rets[t] for t in range(i, min(i+pairs_per_yr, n_periods))]))-1
               for i in range(0, n_periods, pairs_per_yr) if i < n_periods]
    yr_univ = [float(np.prod([1+univ_rets[t] for t in range(i, min(i+pairs_per_yr, n_periods))]))-1
               for i in range(0, n_periods, pairs_per_yr) if i < n_periods]
    common_yrs = min(len(yr_q1), len(yr_univ))
    alpha_win  = float(sum(1 for i in range(common_yrs) if yr_q1[i] > yr_univ[i]) / max(common_yrs, 1))
    avg_alpha  = float(np.mean([yr_q1[i] - yr_univ[i] for i in range(common_yrs)])) if common_yrs > 0 else 0.0

    obq = compute_obq(ic_vals, staircase, alpha_win, avg_alpha, ic_hit)

    q1_sharpe = _sharpe(q1_rets, periods_per_year)
    q1_mdd    = _max_dd(q1_eq)
    q1_calmar = q1_cagr / max(abs(q1_mdd), 0.001)

    # Actual periods-per-year from date span
    # all_dates spans from earliest to latest — use actual range
    actual_ppy = float(n_periods) / max(len(set(d[:4] for d in all_dates)), 1)

    # Per-bucket full metrics + tortoriello fields (needed by tearsheet)
    bucket_metrics_full = {}
    tortoriello_full    = {}

    for b in range(1, n_buckets_actual + 1):
        bid   = str(b)
        b_eq  = blended_equity.get(bid, [1.0])
        b_rets= blended_bucket_rets.get(bid, [])
        b_cagr= bucket_cagrs.get(bid, 0.0)
        b_sharpe = _sharpe(b_rets, actual_ppy)
        b_mdd    = _max_dd(b_eq)
        b_calmar = b_cagr / max(abs(b_mdd), 0.001)

        # Annualized std dev
        r_arr = np.array(b_rets)
        std_period = float(r_arr.std()) if len(r_arr) > 1 else 0.0
        std_ann    = std_period * np.sqrt(actual_ppy)

        # Beta & alpha vs universe
        univ_arr = np.array(univ_rets[:len(b_rets)])
        b_arr    = np.array(b_rets[:len(univ_arr)])
        if len(b_arr) > 3 and univ_arr.std() > 1e-8:
            cov  = float(np.cov(b_arr, univ_arr)[0, 1])
            beta = cov / float(univ_arr.var())
            alpha_period = float(b_arr.mean()) - beta * float(univ_arr.mean())
            alpha_ann    = (1 + alpha_period) ** actual_ppy - 1
        else:
            beta = None; alpha_ann = None

        # % periods beats universe
        pairs = min(len(b_rets), len(univ_rets))
        pct_1y = sum(1 for i in range(pairs) if b_rets[i] > univ_rets[i]) / max(pairs, 1)

        # Avg excess return per period (annualized)
        avg_excess = float(np.mean([b_rets[i] - univ_rets[i] for i in range(pairs)])) * actual_ppy if pairs > 0 else None

        # Max single-period gain / loss
        max_gain = float(max(b_rets)) if b_rets else None
        max_loss = float(min(b_rets)) if b_rets else None

        bucket_metrics_full[bid] = {
            'cagr':            round(b_cagr, 4),
            'sharpe':          round(b_sharpe, 3),
            'max_dd':          round(b_mdd, 4),
            'calmar':          round(b_calmar, 3),
            'n_obs':           n_periods,
            'terminal_wealth': round(b_eq[-1] * 10000, 0) if b_eq else None,
            'ann_vol':         round(float(std_ann), 4),
        }
        tortoriello_full[bid] = {
            'terminal_wealth':   round(b_eq[-1] * 10000, 0) if b_eq else None,
            'avg_excess_vs_univ': round(avg_excess, 4) if avg_excess is not None else None,
            'pct_1y_beats_univ': round(pct_1y, 4),
            'pct_3y_beats_univ': None,   # would need 3yr rolling windows
            'max_gain':          round(max_gain, 4) if max_gain is not None else None,
            'max_loss':          round(max_loss, 4) if max_loss is not None else None,
            'std_dev_ann':       round(float(std_ann), 4),
            'beta_vs_univ':      round(beta, 3) if beta is not None else None,
            'alpha_vs_univ':     round(alpha_ann, 4) if alpha_ann is not None else None,
            'avg_portfolio_size': None,
            'avg_beat_universe': None,
            'avg_lag_universe':  None,
            'median_factor_score': None,
            'avg_market_cap':    None,
        }

    # Universe metrics
    univ_cagr   = _cagr(univ_eq, actual_ppy)
    univ_arr    = np.array(univ_rets)
    univ_std    = float(univ_arr.std()) * np.sqrt(actual_ppy) if len(univ_arr) > 1 else 0.0
    univ_best   = float(max(univ_rets)) if univ_rets else None
    univ_worst  = float(min(univ_rets)) if univ_rets else None
    n_years_actual = n_periods / actual_ppy
    universe_metrics = {
        'cagr':            round(univ_cagr, 4),
        'sharpe':          round(_sharpe(univ_rets, actual_ppy), 3),
        'max_dd':          round(_max_dd(univ_eq), 4),
        'ann_vol':         round(univ_std, 4),
        'best_month':      round(univ_best, 4) if univ_best is not None else None,
        'worst_month':     round(univ_worst, 4) if univ_worst is not None else None,
        'terminal_wealth': round(univ_eq[-1] * 10000, 0),
        'n_years':         round(n_years_actual, 2),
        'calmar':          round(univ_cagr / max(abs(_max_dd(univ_eq)), 0.001), 3),
    }

    # Dates: use the union of all constituent dates that had valid data
    # all_dates was built as the sorted union of all constituent dates
    dates_out = sorted(set(all_dates))[:n_periods]

    # IC data list for tearsheet
    ic_data_out = [{'date': dates_out[i] if i < len(dates_out) else '', 'ic_value': round(v, 4)}
                   for i, v in enumerate(ic_vals)]

    # Period data for heatmap: [{date, q1_ret, q2_ret, ...}, ...]
    period_data_out = []
    for i, d in enumerate(dates_out):
        entry = {'date': d}
        for b in range(1, n_buckets_actual + 1):
            rets_b = blended_bucket_rets.get(str(b), [])
            entry['q'+str(b)+'_ret'] = round(float(rets_b[i]), 6) if i < len(rets_b) else None
        period_data_out.append(entry)

    return {
        'n_obs':           n_periods,
        'icir':            round(icir, 4),
        'ic_mean':         round(ic_mean, 4),
        'ic_std':          round(ic_std, 4),
        'ic_hit_rate':     round(ic_hit, 4),
        'staircase_score': round(staircase, 4),
        'monotonicity_score': round(monotonicity, 4),
        'quintile_spread_cagr': round(spread, 4),
        'q1_cagr':         round(q1_cagr, 4),
        'qn_cagr':         round(qn_cagr, 4),
        'q1_sharpe':       round(q1_sharpe, 3),
        'q1_max_dd':       round(q1_mdd, 4),
        'q1_calmar':       round(q1_calmar, 3),
        'alpha_win_rate':  round(alpha_win, 3),
        'avg_annual_alpha': round(avg_alpha, 4),
        'obq_fund_score':  round(float(np.clip(obq, -1.0, 1.0)), 4),
        'n_stocks_avg':    0.0,
        'bucket_equity':   {bid: [round(v, 6) for v in eq] for bid, eq in blended_equity.items()},
        # Full data for tearsheet
        'bucket_metrics':   bucket_metrics_full,
        'tortoriello':      tortoriello_full,
        'universe_metrics': universe_metrics,
        'universe_equity':  [round(v, 6) for v in univ_eq],
        'dates':            dates_out,
        'ic_data':          ic_data_out,
        'period_data':      period_data_out,
    }

# ── Main ───────────────────────────────────────────────────────────────────────

def run_tranche_analysis(dry_run: bool = False, factor_filter: Optional[str] = None):
    t0 = time.time()
    con_r = duckdb.connect(BANK, read_only=True)

    log.info("Loading CYC-006 results...")
    cyc6_data = load_cyc006_results(con_r)
    con_r.close()

    all_factors = sorted(set(sc for sc, _ in cyc6_data.keys()))
    if factor_filter:
        all_factors = [f for f in all_factors if factor_filter in f]
        log.info(f"Filtered to {len(all_factors)} factors matching '{factor_filter}'")

    log.info(f"Loaded {len(cyc6_data)} (factor, variant) pairs for {len(all_factors)} unique factors")
    log.info(f"Running {len(TRANCHE_CONFIGS)} tranche configs × {len(all_factors)} factors = "
             f"{len(TRANCHE_CONFIGS)*len(all_factors)} blended backtests")

    results = []
    ok = err = skipped = 0
    job_num = 0
    total_jobs = len(TRANCHE_CONFIGS) * len(all_factors)

    for config_id, weights, variant_ids, description in TRANCHE_CONFIGS:
        log.info(f"\n  Config: [{config_id}] {description}")
        config_ok = 0

        for sc in all_factors:
            job_num += 1
            # Build constituent list
            constituents = []
            for i, vid in enumerate(variant_ids):
                key = (sc, vid)
                if key not in cyc6_data:
                    break
                d = cyc6_data[key]
                constituents.append((d['equity'], d['dates'], weights[i]))

            if len(constituents) < len(variant_ids):
                skipped += 1
                continue

            try:
                metrics = blend_tranches(constituents, n_buckets=5, periods_per_year=2.0)
                if metrics is None:
                    skipped += 1
                    continue

                # Build result dict matching save_factor_model schema
                run_label = (f"{sc} | 5Q | 12mo | R3K All-Cap | 1995-2024 "
                             f"[CYC-006b-TRANCHE-{config_id}]")
                config_dict = {
                    'score_column':  sc, 'n_buckets': 5, 'hold_months': 12,
                    'start_date': '1995-03-31', 'end_date': '2024-12-31',
                    'cap_tier': config_id,   # encodes tranche structure
                    'rebalance_freq': config_id,
                    'tranche_config': config_id,
                    'tranche_description': description,
                    'constituent_variants': variant_ids,
                    'tranche_weights': weights,
                    'min_price': 5.0, 'cost_bps': 15.0,
                }

                result = {
                    'status':         'complete',
                    'score_column':   sc,
                    'run_label':      run_label,
                    'config':         config_dict,
                    'factor_metrics': metrics,
                    'bucket_equity':  metrics.pop('bucket_equity', {}),
                    'bucket_metrics': metrics.pop('bucket_metrics', {}),
                    'ic_data':        metrics.pop('ic_data', []),
                    'dates':          metrics.pop('dates', []),
                    'universe_metrics': metrics.pop('universe_metrics', {}),
                    'universe_equity':  metrics.pop('universe_equity', []),
                    'tortoriello':    metrics.pop('tortoriello', {}),
                    'period_data':    metrics.pop('period_data', []),
                    'annual_ret_by_bucket': {},
                    'universe_rets':  [],
                    'fitness': {
                        'staircase_score':  metrics.get('staircase_score', 0),
                        'alpha_win_rate':   metrics.get('alpha_win_rate', 0),
                        'avg_annual_alpha': metrics.get('avg_annual_alpha', 0),
                        'obq_fund_score':   metrics.get('obq_fund_score', 0),
                    },
                    'sector_attribution': [],
                    'spy_metrics': {},
                    'trade_log': [],
                    'n_obs': metrics.get('n_obs', 0),
                    'n_stocks_avg': 0,
                    'elapsed_s': 0.0,
                    'elapsed_gpu_ms': 0.0,
                }
                results.append(result)
                ok += 1
                config_ok += 1

            except Exception as exc:
                err += 1
                log.warning(f"  Error {sc}/{config_id}: {str(exc)[:60]}")

        log.info(f"  [{config_id}] {config_ok}/{len(all_factors)} ok")

    elapsed = time.time() - t0
    log.info(f"\nBlending complete: {ok} ok | {err} err | {skipped} skipped | {elapsed:.1f}s")

    # ── Bulk save ──────────────────────────────────────────────────────────────
    if not dry_run and results:
        log.info(f"\nSaving {len(results)} results to bank (single connection)...")
        t_save = time.time()
        con_w  = _get_bank()
        saved  = 0
        from engine.strategy_bank import save_factor_model as _sfm, _get_bank as _gb
        import engine.strategy_bank as _sb
        _sb._get_bank = lambda: con_w   # share connection
        for r in results:
            try:
                _sfm(r, overwrite=True, _shared_con=con_w)
                saved += 1
            except Exception: pass
        con_w.close()
        _sb._get_bank = _gb.__wrapped__ if hasattr(_gb, '__wrapped__') else _get_bank

        log.info(f"  Saved {saved}/{len(results)} in {time.time()-t_save:.1f}s")

    elif dry_run:
        log.info(f"\nDRY RUN — {len(results)} results computed, not saved")
        if results:
            r = results[0]
            fm = r['factor_metrics']
            log.info(f"  Sample: {r['score_column']} [{r['config']['cap_tier']}]")
            log.info(f"    OBQ={fm.get('obq_fund_score'):.3f} ICIR={fm.get('icir'):.3f} "
                     f"Spread={fm.get('quintile_spread_cagr',0)*100:+.1f}%")

    return results

def analyze_results():
    """Print tranche analysis comparing each config's OBQ vs CYC-006 single-timing best."""
    con = duckdb.connect(BANK, read_only=True)

    # CYC-006b results
    rows = con.execute("""
        SELECT score_column, cap_tier, obq_fund_score, icir,
               quintile_spread_cagr, alpha_win_rate, bear_score
        FROM factor_models WHERE run_label LIKE '%CYC-006b-TRANCHE%'
        ORDER BY score_column, cap_tier
    """).fetchall()

    # CYC-006 best single timing (for comparison)
    best_single = con.execute("""
        SELECT score_column, MAX(obq_fund_score) AS best_obq
        FROM factor_models WHERE run_label LIKE '%CYC-006%'
          AND run_label NOT LIKE '%TRANCHE%'
        GROUP BY score_column
    """).fetchall()
    best_single_map = {r[0]: r[1] for r in best_single}
    con.close()

    # Index by (sc, config)
    data = defaultdict(dict)
    for r in rows:
        sc  = r[0]
        cfg = r[1]
        data[sc][cfg] = {'obq': r[2], 'icir': r[3], 'spread': r[4], 'aw': r[5], 'bear': r[6]}

    print("=" * 80)
    print("CYC-006b TRANCHE ANALYSIS — Does Staggered Rebalancing Improve Alpha?")
    print("=" * 80)

    config_wins = defaultdict(int)
    config_deltas = defaultdict(list)

    # Per config: average OBQ gain vs best single timing
    print(f"\n{'Config':<22} {'Avg OBQ':>8} {'Avg vs Best Single':>20} {'Factors Improved':>17}")
    print("-" * 75)
    configs = list({r[1] for r in rows})
    config_summary = {}
    for cfg in sorted(configs):
        obqs = [data[sc][cfg]['obq'] for sc in data if cfg in data[sc] and data[sc][cfg]['obq'] is not None]
        best_singles = [best_single_map.get(sc, 0) for sc in data if cfg in data[sc] and best_single_map.get(sc) is not None]
        deltas = [(data[sc][cfg].get('obq') or 0) - (best_single_map.get(sc) or 0)
                  for sc in data if cfg in data[sc]]
        avg_obq    = np.mean(obqs) if obqs else 0
        avg_delta  = np.mean(deltas) if deltas else 0
        n_improved = sum(1 for d in deltas if d > 0.005)
        config_summary[cfg] = (avg_obq, avg_delta, n_improved, len(deltas))
        print(f"  {cfg:<20} {avg_obq:>8.3f} {avg_delta:>+20.3f} {n_improved:>12}/{len(deltas)}")

    print(f"\n--- Best tranche config per factor ---")
    print(f"{'Factor':<35} {'Best Tranche':>20} {'OBQ':>7} {'vs Best Single':>15}")
    print("-" * 80)
    for sc in sorted(data.keys())[:30]:  # first 30
        sc_data = data[sc]
        if not sc_data: continue
        best_cfg = max(sc_data, key=lambda c: sc_data[c].get('obq') or -99)
        best_obq = sc_data[best_cfg].get('obq')
        vs_single = (best_obq or 0) - (best_single_map.get(sc) or 0)
        config_wins[best_cfg] += 1
        print(f"  {sc:<33} {best_cfg:>20} {best_obq:>7.3f} {vs_single:>+15.3f}")

    print(f"\n--- Winner count per tranche config ---")
    for cfg, wins in sorted(config_wins.items(), key=lambda x: -x[1]):
        bar = "█" * wins
        print(f"  {cfg:<22} {wins:>4}x  {bar}")


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='CYC-006b Staggered Tranche Rebalancing')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--factor',  type=str, default=None, help='Single factor filter')
    ap.add_argument('--analyze', action='store_true')
    args = ap.parse_args()

    if args.analyze:
        analyze_results()
        sys.exit(0)

    results = run_tranche_analysis(dry_run=args.dry_run, factor_filter=args.factor)

    print(f"\n{'='*60}")
    print(f"CYC-006b COMPLETE: {len(results)} blended backtests")
    print(f"{'='*60}")

    if not args.dry_run:
        print("\nRunning analysis...")
        analyze_results()
