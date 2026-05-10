# -*- coding: utf-8 -*-
"""
Factor data extraction for Encyclopedia v2.

Pulls cross-tier + sector + Tortoriello data for a single factor from the
strategy bank and shapes it into a unified dict ready for chapter rendering.

Per the v2 doctrine (ARCHITECTURE.md):
  - R3000 base case is the default lens
  - Cap-bin slicing (all/large/mid) on equal footing
  - Sector slicing per cap bin
  - Bear/bull window decomposition

Usage:
    from encyclopedia_v2.generator.factor_data import load_factor_bundle
    bundle = load_factor_bundle('jcn_full_composite')
    # bundle['tiers']['all'] = full record dict
    # bundle['tiers']['large'] = full record dict
    # bundle['tiers']['mid'] = full record dict
    # bundle['cross_tier_table'] = comparison table data
    # bundle['sector_matrix'] = (sector × tier × bucket) DataFrame-like
"""
from __future__ import annotations

import json
from typing import Optional

import duckdb

FACTOR_BANK = r'D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb'

# Stable cap tier ordering for tables
TIER_ORDER = ['all', 'large', 'mid']
TIER_LABELS = {
    'all':   'R3K All-Cap',
    'large': 'Large-Cap $10B+',
    'mid':   'Mid-Cap $2B-$10B',
}


# ── Core scalar fields surfaced in every chapter ──────────────────────────────
FACTOR_SCALAR_FIELDS = [
    # Identity
    'strategy_id', 'score_column', 'cap_tier', 'run_label',
    'start_date', 'end_date', 'rebalance_freq', 'hold_months', 'n_buckets',
    'n_obs', 'n_stocks_avg',
    # Signal quality
    'icir', 'ic_mean', 'ic_std', 'ic_hit_rate', 'spearman_rho', 'monotonicity_score',
    # Quintile performance
    'quintile_spread_cagr', 'q1_cagr', 'q1_sharpe', 'q1_max_dd', 'q1_calmar', 'q1_sortino',
    'qn_cagr', 'qn_sharpe',
    # Fitness
    'staircase_score', 'alpha_win_rate', 'avg_annual_alpha',
    'bear_score', 'bull_score', 'downside_capture', 'alpha_sharpe',
    'obq_fund_score',
]


def _safe_json(field):
    if field is None or field == '':
        return None
    try:
        return json.loads(field) if isinstance(field, str) else field
    except Exception:
        return None


def load_factor_bundle(score_column: str,
                        cycle_tag: str = 'CYC-003-GPU',
                        db_path: str = FACTOR_BANK) -> Optional[dict]:
    """
    Load all 3 cap-tier records for a single factor, plus parsed JSON blobs.
    Tries cycle_tag first; if not found, auto-detects across all cycles.
    Returns None if the factor is not found in any tier.
    """
    con = duckdb.connect(db_path, read_only=True)

    # Build SELECT for scalar fields + json blobs
    scalar_cols = ', '.join(FACTOR_SCALAR_FIELDS)
    json_cols = ('bucket_metrics_json, bucket_equity_json, ic_data_json, '
                 'period_data_json, annual_ret_json, tortoriello_json, '
                 'sector_attribution_json, universe_metrics_json, '
                 'universe_equity_json, fitness_json')

    # Try given cycle_tag first; fall back to any matching record
    rows = con.execute(f"""
        SELECT {scalar_cols}, {json_cols}
        FROM factor_models
        WHERE score_column = ?
          AND run_label LIKE ?
        ORDER BY cap_tier
    """, [score_column, f'%[{cycle_tag}]%']).fetchall()

    # Auto-detect: if not found under given cycle_tag, try all cycles
    # For CYC-005 novel factors, prefer R3K-All-Cap cross-sectional run
    if not rows:
        rows = con.execute(f"""
            SELECT {scalar_cols}, {json_cols}
            FROM factor_models
            WHERE score_column = ?
            ORDER BY
                CASE WHEN cap_tier = 'R3K-All-Cap' THEN 0 ELSE 1 END,
                cap_tier
        """, [score_column]).fetchall()

    cols = [d[0] for d in con.description]
    con.close()

    if not rows:
        return None

    # Index by cap_tier
    by_tier: dict = {}
    for r in rows:
        rec: dict = dict(zip(cols, r))
        tier = rec['cap_tier']
        # Parse JSON blobs in place for downstream rendering
        for jc in ['bucket_metrics_json', 'bucket_equity_json', 'ic_data_json',
                   'period_data_json', 'annual_ret_json', 'tortoriello_json',
                   'sector_attribution_json', 'universe_metrics_json',
                   'universe_equity_json', 'fitness_json']:
            rec[jc.replace('_json', '')] = _safe_json(rec.pop(jc))
        by_tier[tier] = rec

    # Determine tier order: prefer standard tiers, fall back to whatever is present
    std_present = [t for t in TIER_ORDER if t in by_tier]
    all_present = list(by_tier.keys())
    tiers_present = std_present if std_present else all_present

    bundle: dict = {
        'score_column': score_column,
        'cycle_tag':    cycle_tag,
        'tiers':        by_tier,
        'tiers_present': tiers_present,
    }

    # Build cross-tier comparison table
    bundle['cross_tier_table'] = _build_cross_tier_table(by_tier)
    bundle['sector_matrix'] = _build_sector_matrix(by_tier)

    return bundle


def _build_cross_tier_table(by_tier: dict) -> list:
    """Return a list of dicts: one row per metric, columns per tier.
    Works with both standard tiers (all/large/mid) and CYC-005 sector tiers."""
    # Use actual tier keys present in the bundle
    tier_keys = list(by_tier.keys())
    metrics = [
        # OBQ score is a 0-1 dimensionless score (NOT a percentage), use num3
        ('OBQ Master',           'obq_fund_score',      'num3'),
        ('ICIR',                 'icir',                'num3'),
        ('IC Hit Rate',          'ic_hit_rate',         'pct'),
        ('Q1-Q5 Spread CAGR',    'quintile_spread_cagr','pct'),
        ('Q1 CAGR',              'q1_cagr',             'pct'),
        ('Q1 Sharpe',            'q1_sharpe',           'num2'),
        ('Q1 Max DD',            'q1_max_dd',           'pct'),
        # Staircase is a dimensionless score
        ('Staircase',            'staircase_score',     'num4'),
        ('Alpha Win Rate',       'alpha_win_rate',      'pct'),
        ('Avg Annual Alpha',     'avg_annual_alpha',    'pct'),
        ('Bear Score',           'bear_score',          'pct'),
        ('Bull Score',           'bull_score',          'pct'),
        ('Avg Universe Size',    'n_stocks_avg',        'int'),
    ]
    rows = []
    for label, key, fmt in metrics:
        row: dict = {'metric': label, 'fmt': fmt}
        for tier in tier_keys:
            row[tier] = by_tier.get(tier, {}).get(key)
        rows.append(row)
    return rows


def _build_sector_matrix(by_tier: dict) -> dict:
    """
    Returns: {sector_name: {tier: {q1, q5, spread}, ...}, ...}
    Sectors only included if they appear in at least one tier.
    """
    matrix: dict = {}
    for tier, rec in by_tier.items():
        sa = rec.get('sector_attribution', []) or []
        for row in sa:
            sec = row.get('sector', 'Unknown')
            if sec not in matrix:
                matrix[sec] = {}
            matrix[sec][tier] = {
                'q1':     row.get('q1'),
                'q5':     row.get('q5'),
                'spread': row.get('spread'),
            }
    return matrix


def list_available_factors(cycle_tag: str = 'ALL',
                           db_path: str = FACTOR_BANK) -> list:
    """List all distinct factor score_columns in the bank across all cycles."""
    con = duckdb.connect(db_path, read_only=True)
    # Load all factors from ALL cycles (CYC-003-GPU + CYC-004-GPU)
    rows = con.execute("""
        SELECT DISTINCT score_column,
               COUNT(*) AS n_tiers,
               MAX(obq_fund_score) AS best_obq,
               ANY_VALUE(run_label) AS sample_label
        FROM factor_models
        WHERE run_label LIKE '%CYC-003-GPU%'
           OR run_label LIKE '%CYC-004-GPU%'
           OR run_label LIKE '%CYC-005b-NOVEL%'
        GROUP BY score_column
        ORDER BY best_obq DESC NULLS LAST
    """).fetchall()
    con.close()
    return [{'score_column': r[0], 'n_tiers': r[1], 'best_obq': r[2],
             'cycle': 'CYC-004' if 'CYC-004' in (r[3] or '') else 'CYC-003'}
            for r in rows]


# ── Self-test ─────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 70)
    print("Encyclopedia v2 Generator — Factor Data Extraction")
    print("=" * 70)

    factors = list_available_factors()
    print(f"\nAvailable factors: {len(factors)}")
    print("\nTop 10 by OBQ Master Score:")
    print(f"  {'score_column':<32} {'n_tiers':>8} {'best_obq':>10}")
    for f in factors[:10]:
        print(f"  {f['score_column']:<32} {f['n_tiers']:>8} {f['best_obq']:>10.3f}")

    # Test extraction on top factor
    top = factors[0]['score_column']
    print(f"\n=== Bundle for {top} ===")
    bundle = load_factor_bundle(top)
    if bundle is None:
        print("  ERROR: factor not found")
    else:
        print(f"  Tiers present: {bundle['tiers_present']}")
        print(f"\n  Cross-tier table:")
        for row in bundle['cross_tier_table'][:8]:
            vals = [row.get(t) for t in TIER_ORDER]
            vstr = '  '.join([f"{v:>8.3f}" if isinstance(v, (int, float))
                              else f"{str(v):>8}" for v in vals])
            print(f"    {row['metric']:<22} {vstr}")
        print(f"\n  Sector matrix ({len(bundle['sector_matrix'])} sectors):")
        for sec, tier_data in list(bundle['sector_matrix'].items())[:5]:
            print(f"    {sec}:")
            for t, vals in tier_data.items():
                print(f"      {t:<6} q1={vals['q1']*100 if vals['q1'] else 0:+5.1f}%  "
                      f"q5={vals['q5']*100 if vals['q5'] else 0:+5.1f}%  "
                      f"spread={vals['spread']*100 if vals['spread'] else 0:+5.1f}%")
