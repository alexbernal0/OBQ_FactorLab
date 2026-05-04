# -*- coding: utf-8 -*-
"""
CYC-002 Batch Runner â€” Comprehensive Factor Validation
=======================================================
Tests all 50 single factors + 19 two-factor combos from
Tortoriello, O'Shaughnessy, and academic literature.

Universe: Large-cap $10B+, 1990-2024, semi-annual, 6mo hold
Objective: Validate against published results + extract intelligence

USAGE:
  python batch_cyc002.py --batch A      # Group A: Profitability only
  python batch_cyc002.py --batch B      # Group B: Financial Strength
  python batch_cyc002.py --batch C      # Group C: Valuation
  python batch_cyc002.py --batch D      # Group D: Growth
  python batch_cyc002.py --batch E      # Group E: Momentum
  python batch_cyc002.py --batch FG     # Groups F+G: Capital + Moat
  python batch_cyc002.py --batch combos # All 19 combos
  python batch_cyc002.py --batch all    # Everything (3.5 hours)
  python batch_cyc002.py --dry-run      # Print plan, don't run
"""
import sys, time, traceback, argparse
sys.path.insert(0, '.')

from engine.cyc002_factors import CYC002_FACTORS, CYC002_COMBOS, TOTAL_SINGLES, TOTAL_COMBOS
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model
from engine.portfolio_bank import save_portfolio_model

# â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
COMMON = dict(
    start_date           = "1990-07-31",
    end_date             = "2024-12-31",
    n_buckets            = 5,
    hold_months          = 6,
    rebalance_freq       = "semi-annual",
    cap_tier             = "all",
    min_price            = 5.0,
    min_adv_usd          = 1_000_000.0,
    min_market_cap       = 10_000_000_000.0,  # $10B+ large-cap
    transaction_cost_bps = 15.0,
)

CYCLE_TAG = "[CYC-002-BASELINE]"

def _cb(msg):
    if any(k in msg.lower() for k in ['trade log', 'complete', 'error', 'r3000', 'russell']):
        print(f"    {msg}")


def run_single_factor(score_col, direction, display_name, group, dry_run=False):
    """Run one factor: factor backtest + portfolio backtest."""
    f_label = f"{display_name} | 5Q | 6mo | Large-Cap | 1990-2024 {CYCLE_TAG}"
    p_label = f"{display_name} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 {CYCLE_TAG}"

    if dry_run:
        print(f"  [DRY] FACTOR: {score_col} ({direction}) -> {display_name}")
        return None, None

    # Factor backtest
    f_result = None
    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score_col, score_direction=direction,
            run_label=f_label, **COMMON
        ), cb=_cb)
        if r.get('status') == 'complete':
            sid = save_factor_model(r, overwrite=True)
            fm  = r.get('factor_metrics', {})
            f_result = {
                'sid': sid, 'icir': fm.get('icir', 0),
                'spread': fm.get('quintile_spread_cagr', 0) * 100,
                'staircase': fm.get('staircase_score', 0) * 100,
                'q1_cagr': fm.get('q1_cagr', 0) * 100,
            }
            print(f"  [F] {sid} | ICIR={f_result['icir']:.3f} | Spread={f_result['spread']:.2f}% | Stair={f_result['staircase']:.2f}%")
        else:
            print(f"  [F] ERR {score_col}: {r.get('error','')[:80]}")
    except Exception as e:
        print(f"  [F] EXCEPTION {score_col}: {str(e)[:80]}")
        traceback.print_exc()

    # Portfolio backtest
    p_result = None
    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score_col, score_direction=direction,
            top_n=20, sector_max=5,
            run_label=p_label, **{k:v for k,v in COMMON.items() if k not in ('n_buckets','hold_months')}
        ), cb=_cb)
        if r2.get('status') == 'complete':
            sid2 = save_portfolio_model(r2, overwrite=True)
            pm   = r2.get('portfolio_metrics', {})
            p_result = {
                'sid': sid2, 'cagr': pm.get('cagr', 0) * 100,
                'sharpe': pm.get('sharpe', 0), 'max_dd': pm.get('max_dd', 0) * 100,
            }
            print(f"  [P] {sid2} | CAGR={p_result['cagr']:.2f}% | Sharpe={p_result['sharpe']:.3f} | MaxDD={p_result['max_dd']:.1f}%")
        else:
            print(f"  [P] ERR {score_col}: {r2.get('error','')[:80]}")
    except Exception as e:
        print(f"  [P] EXCEPTION {score_col}: {str(e)[:80]}")

    return f_result, p_result


def run_combo(combo_id, factor_a, factor_b, display_name, source, dry_run=False):
    """
    Two-factor combo: equal-weight percentile rank of both factors combined.
    Method: load both score series, compute combined_score = (rank_a + rank_b) / 2
    This requires a custom score computation not yet in the engine.
    For now, log as planned and implement in CYC-002 execution phase.
    """
    if dry_run:
        print(f"  [DRY] COMBO {combo_id}: {display_name} | {source}")
        return None
    # TODO: implement combo backtest
    # The combo engine needs to:
    # 1. Load factor_a scores (as percentile rank within universe)
    # 2. Load factor_b scores (as percentile rank within universe)
    # 3. Average the ranks: combined = (rank_a_pct + rank_b_pct) / 2
    # 4. Use combined as the sorting score
    print(f"  [COMBO] {combo_id}: {display_name} â€” engine implementation pending")
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', default='all',
                        choices=['A','B','C','D','E','FG','combos','all'])
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    dry = args.dry_run
    batch = args.batch

    # Filter factors by group
    group_map = {
        'A': 'A_Profitability', 'B': 'B_FinancialStrength',
        'C': 'C_Valuation',     'D': 'D_Growth',
        'E': 'E_Momentum',
        'FG': ['F_Capital', 'G_Moat'],
    }

    if batch == 'combos':
        factors_to_run = []
    elif batch == 'all':
        factors_to_run = CYC002_FACTORS
    else:
        target = group_map[batch]
        if isinstance(target, list):
            factors_to_run = [f for f in CYC002_FACTORS if f[5] in target]
        else:
            factors_to_run = [f for f in CYC002_FACTORS if f[5] == target]

    combos_to_run = CYC002_COMBOS if batch in ('combos', 'all') else []

    print(f"{'='*70}")
    print(f"CYC-002 BATCH RUNNER")
    print(f"Batch: {batch} | Factors: {len(factors_to_run)} | Combos: {len(combos_to_run)}")
    print(f"Total runs: {(len(factors_to_run) + len(combos_to_run)) * 2}")
    print(f"{'='*70}")

    results = []
    errors  = []
    t_start = time.time()

    # â”€â”€ Run single factors â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    current_group = None
    for score_col, src_table, src_col, direction, display_name, group in factors_to_run:
        if group != current_group:
            current_group = group
            print(f"\n{'â”€'*50}")
            print(f"GROUP: {group}")
            print(f"{'â”€'*50}")

        print(f"\n[{direction.upper()[:1]}] {score_col} â€” {display_name}")
        t0 = time.time()
        f_res, p_res = run_single_factor(score_col, direction, display_name, group, dry_run=dry)
        elapsed = round(time.time() - t0, 1)

        if f_res:
            results.append({
                'type': 'factor', 'score': score_col, 'group': group,
                'display': display_name, 'elapsed': elapsed, **f_res
            })
        elif not dry:
            errors.append({'type': 'factor', 'score': score_col})

    # â”€â”€ Run combos â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if combos_to_run:
        print(f"\n{'â”€'*50}")
        print(f"TWO-FACTOR COMBOS ({len(combos_to_run)})")
        print(f"{'â”€'*50}")
        for combo_id, fa, fb, name, source in combos_to_run:
            print(f"\n{combo_id}: {name} ({source})")
            run_combo(combo_id, fa, fb, name, source, dry_run=dry)

    # â”€â”€ Summary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elapsed_total = round(time.time() - t_start, 0)
    print(f"\n{'='*70}")
    print(f"BATCH COMPLETE: {len(results)} saved | {len(errors)} errors | {elapsed_total:.0f}s")
    print(f"{'='*70}")

    if results:
        print(f"\n{'FACTOR':<42} {'ICIR':>6} {'SPREAD':>8} {'STAIR':>7} {'Q1CAGR':>8}")
        print("-"*75)
        for r in sorted(results, key=lambda x: x.get('icir',0), reverse=True):
            print(f"  {r['display']:<40} {r.get('icir',0):>6.3f} {r.get('spread',0):>7.2f}% {r.get('staircase',0):>6.2f}% {r.get('q1_cagr',0):>7.2f}%")

    if errors:
        print(f"\nERRORS: {[e['score'] for e in errors]}")


if __name__ == "__main__":
    main()

