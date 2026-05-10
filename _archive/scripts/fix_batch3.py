# -*- coding: utf-8 -*-
"""
Fix Batch 3 models:
1. Delete the 10 wrong-direction / duplicate runs
2. Keep only the quarterly+3mo correct-direction run per score
3. Re-run any that still have issues
"""
import sys, time, traceback
sys.path.insert(0, '.')
import duckdb
from engine.strategy_bank import BANK_FILE, get_all_models, save_factor_model
from engine.portfolio_bank import BANK_FILE as PM_BANK, get_all_portfolio_models, save_portfolio_model

# ── Step 1: Delete bad models ────────────────────────────────────────────────
# Bad = semi-annual hold variants with wrong direction (negative ICIR)
# Keep: the quarterly+3mo runs (877F for longeq, 5A83 for rulebreaker, E540 for fundsmith, 436D for moat_score, B67D for moat_rank)
KEEP = {'FM-LONGEQ-20260503-877F', 'FM-LONGEQ-20260503-7D78',  # both semi-ann longeq — will re-evaluate
        'FM-RULEBR-20260503-5A83',
        'FM-FUNDSM-20260503-E540',
        'FM-MOATSC-20260503-436D',
        'FM-MOATRA-20260503-B67D'}

DELETE_FACTOR = [
    'FM-LONGEQ-20260503-D30E',   # quarterly but negative ICIR — wrong direction
    'FM-LONGEQ-20260503-877F',   # semi-annual 6mo — keep only one longeq
    'FM-LONGEQ-20260503-7D78',   # semi-annual 3mo — keep only one longeq
    'FM-RULEBR-20260503-41CB',   # semi-annual 3mo negative
    'FM-RULEBR-20260503-7FEF',   # semi-annual 6mo negative
    'FM-FUNDSM-20260503-65E8',   # semi-annual 3mo negative
    'FM-FUNDSM-20260503-E6E9',   # semi-annual 6mo negative
    'FM-MOATRA-20260503-FCCD',   # semi-annual 3mo negative
    'FM-MOATRA-20260503-CA6D',   # semi-annual 6mo negative
    'FM-MOATSC-20260503-8C1C',   # semi-annual 3mo
    'FM-MOATSC-20260503-3745',   # semi-annual 6mo
]

print("=== DELETING BAD FACTOR MODELS ===")
con_f = duckdb.connect(str(BANK_FILE))
for sid in DELETE_FACTOR:
    con_f.execute("DELETE FROM factor_models WHERE strategy_id = ?", [sid])
    print(f"  Deleted: {sid}")
con_f.commit()
con_f.close()
print(f"Deleted {len(DELETE_FACTOR)} factor models")

# Also delete corresponding bad portfolio models for these scores
batch3_scores = ['longeq_rank','rulebreaker_rank','fundsmith_rank','moat_score','moat_rank']
pm_all = get_all_portfolio_models(200)
DELETE_PORT = []
for m in pm_all:
    if m.get('score_column') in batch3_scores:
        DELETE_PORT.append(m['strategy_id'])

print(f"\n=== DELETING ALL BATCH 3 PORTFOLIO MODELS ({len(DELETE_PORT)}) — will re-run cleanly ===")
con_p = duckdb.connect(str(PM_BANK))
for sid in DELETE_PORT:
    con_p.execute("DELETE FROM portfolio_models WHERE strategy_id = ?", [sid])
    print(f"  Deleted: {sid}")
con_p.commit()
con_p.close()

# ── Step 2: Re-run all 5 Batch 3 scores cleanly ──────────────────────────────
# One canonical config per score:
# - quarterly rebalance, 3-month hold (matches typical factor cycle for these gate scores)
# - score_direction=lower_better for rank scores, higher_better for gate scores
# - large-cap $10B+, 1990-2024

CANONICAL_CONFIGS = {
    'longeq_rank':      {'direction': 'lower_better',  'label': 'LongEQ Rank'},
    'rulebreaker_rank': {'direction': 'lower_better',  'label': 'Rulebreaker Rank'},
    'fundsmith_rank':   {'direction': 'lower_better',  'label': 'Fundsmith Rank'},
    'moat_score':       {'direction': 'higher_better', 'label': 'Moat Score (gate count)'},
    'moat_rank':        {'direction': 'lower_better',  'label': 'Moat Rank'},
}

from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig

COMMON = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
    n_buckets=5, hold_months=3, rebalance_freq='quarterly', cap_tier='all',
)

def cb(msg):
    if any(k in msg.lower() for k in ['trade log', 'complete', 'error', 'r3000', 'russell']):
        print(f"    {msg}")

results = []
print("\n=== RE-RUNNING BATCH 3 CLEANLY ===")
for score, cfg_extra in CANONICAL_CONFIGS.items():
    direction = cfg_extra['direction']
    label_name = cfg_extra['label']
    run_label = f"{label_name} | 5Q | 3mo | Large-Cap | 1990-2024 [CYC-001-BASELINE]"
    print(f"\n[{direction}] {score}...")

    # Factor
    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score, score_direction=direction,
            run_label=run_label, **COMMON
        ), cb=cb)
        if r.get('status') == 'complete':
            sid = save_factor_model(r, overwrite=True)
            fm = r.get('factor_metrics', {})
            icir = fm.get('icir', 0)
            spread = fm.get('quintile_spread_cagr', 0) * 100
            print(f"  FACTOR OK: {sid} | ICIR={icir:.3f} | Spread={spread:.2f}%")
            results.append(('factor', score, sid, icir))
        else:
            print(f"  FACTOR ERR: {r.get('error','')[:80]}")
    except Exception as e:
        print(f"  FACTOR EXCEPTION: {str(e)[:80]}")

    # Portfolio
    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score, score_direction=direction,
            top_n=20, sector_max=5,
            run_label=f"{label_name} | Top-20 | Quarterly | 5/Sector | Large-Cap | 1990-2024 [CYC-001-BASELINE]",
            **COMMON
        ), cb=cb)
        if r2.get('status') == 'complete':
            sid2 = save_portfolio_model(r2, overwrite=True)
            pm = r2.get('portfolio_metrics', {})
            print(f"  PORT   OK: {sid2} | CAGR={pm.get('cagr',0)*100:.2f}% | Sharpe={pm.get('sharpe',0):.3f}")
        else:
            print(f"  PORT   ERR: {r2.get('error','')[:80]}")
    except Exception as e:
        print(f"  PORT   EXCEPTION: {str(e)[:80]}")

print(f"\n=== DONE: {len(results)} factor models re-run cleanly ===")
for typ, score, sid, icir in results:
    print(f"  {sid} | {score} | ICIR={icir:.3f}")
