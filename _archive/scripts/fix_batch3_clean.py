# -*- coding: utf-8 -*-
"""
Clean final re-run of all Batch 3 scores with CORRECT directions verified from data.
longeq_rank:      higher_better (rank 1=worst, high rank=best — ascending convention)
rulebreaker_rank: lower_better  (rank 1=best)
fundsmith_rank:   lower_better  (rank 1=best)
moat_score:       higher_better (gate count 0-7, more=better)
moat_rank:        lower_better  (rank 1=best)
"""
import sys, time, traceback
sys.path.insert(0, '.')
import duckdb
from engine.strategy_bank import BANK_FILE, save_factor_model, get_all_models
from engine.portfolio_bank import BANK_FILE as PM_BANK, save_portfolio_model, get_all_portfolio_models
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig

# Delete ALL existing Batch 3 models first (clean slate)
BATCH3_SCORES = ['longeq_rank','rulebreaker_rank','fundsmith_rank','moat_score','moat_rank']

print("=== DELETING ALL EXISTING BATCH 3 MODELS ===")
con_f = duckdb.connect(str(BANK_FILE))
for score in BATCH3_SCORES:
    r = con_f.execute("DELETE FROM factor_models WHERE score_column = ?", [score])
    print(f"  Deleted factor models for {score}")
con_f.commit(); con_f.close()

con_p = duckdb.connect(str(PM_BANK))
for score in BATCH3_SCORES:
    r = con_p.execute("DELETE FROM portfolio_models WHERE score_column = ?", [score])
    print(f"  Deleted portfolio models for {score}")
con_p.commit(); con_p.close()

# Correct directions verified from PROD_* table data
CANONICAL = [
    # (score_col,           direction,          label)
    ('longeq_rank',      'higher_better', 'LongEQ Rank (ascending: high rank = best)'),
    ('rulebreaker_rank', 'lower_better',  'Rulebreaker Rank (rank 1 = best)'),
    ('fundsmith_rank',   'lower_better',  'Fundsmith Rank (rank 1 = best)'),
    ('moat_score',       'higher_better', 'Moat Score (gate count 0-7, more = better)'),
    ('moat_rank',        'lower_better',  'Moat Rank (rank 1 = best)'),
]

COMMON_F = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
    n_buckets=5, hold_months=3, rebalance_freq='quarterly', cap_tier='all',
)
COMMON_P = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
    top_n=20, sector_max=5, rebalance_freq='quarterly', cap_tier='all',
)

def cb(msg):
    if any(k in msg.lower() for k in ['trade log','complete','error','r3000']):
        print(f"    {msg}")

print("\n=== RUNNING CLEAN BATCH 3 ===")
factor_results = []
for score, direction, label in CANONICAL:
    f_label = f"{label} | 5Q | 3mo | Large-Cap | 1990-2024 [CYC-001-BASELINE]"
    p_label = f"{label} | Top-20 | Quarterly | 5/Sector | Large-Cap | 1990-2024 [CYC-001-BASELINE]"
    print(f"\n[{direction}] {score}...")

    # Factor
    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score, score_direction=direction,
            run_label=f_label, **COMMON_F
        ), cb=cb)
        if r.get('status') == 'complete':
            sid = save_factor_model(r, overwrite=True)
            fm  = r.get('factor_metrics', {})
            icir   = fm.get('icir', 0)
            spread = fm.get('quintile_spread_cagr', 0) * 100
            print(f"  FACTOR OK: {sid} | ICIR={icir:.3f} | Spread={spread:.2f}%")
            factor_results.append((score, sid, icir, spread))
        else:
            print(f"  FACTOR ERR: {r.get('error','')[:100]}")
    except Exception as e:
        traceback.print_exc()

    # Portfolio
    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score, score_direction=direction,
            run_label=p_label, **COMMON_P
        ), cb=cb)
        if r2.get('status') == 'complete':
            sid2 = save_portfolio_model(r2, overwrite=True)
            pm   = r2.get('portfolio_metrics', {})
            print(f"  PORT   OK: {sid2} | CAGR={pm.get('cagr',0)*100:.2f}% | Sharpe={pm.get('sharpe',0):.3f}")
        else:
            print(f"  PORT   ERR: {r2.get('error','')[:100]}")
    except Exception as e:
        traceback.print_exc()

print("\n=== BATCH 3 CLEAN RESULTS ===")
for score, sid, icir, spread in factor_results:
    quality = "GOOD" if icir > 0.5 else ("WEAK" if icir > 0 else "BAD - check direction")
    print(f"  {sid} | {score} | ICIR={icir:.3f} | Spread={spread:.2f}% | {quality}")
