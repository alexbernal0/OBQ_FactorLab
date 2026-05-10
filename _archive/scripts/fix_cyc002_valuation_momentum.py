# -*- coding: utf-8 -*-
"""
Fix CYC-002:
1. Re-run valuation factors with corrected lower_better direction
2. Fix and run 6 momentum factors (PROD_OBQ_Momentum_Scores join issue)
"""
import sys, time, traceback
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig, SEPARATE_SCORE_TABLES
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model

COMMON = dict(
    start_date='1990-07-31', end_date='2024-12-31',
    n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
    cap_tier='all', min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
)
TAG = "[CYC-002-BASELINE]"

def cb(m):
    if any(k in m.lower() for k in ['complete','error','r3000','trade log']):
        print(f"    {m}", flush=True)

results = []

def run_one(score_col, direction, display):
    print(f"\n{score_col} ({direction})...", flush=True)
    t0 = time.time()
    row = {'score': score_col, 'display': display}

    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score_col, score_direction=direction,
            run_label=f"{display} | 5Q | 6mo | Large-Cap | 1990-2024 {TAG}",
            **COMMON
        ), cb=cb)
        if r.get('status') == 'complete':
            sid = save_factor_model(r, overwrite=True)
            fm = r['factor_metrics']
            row.update({'icir': fm.get('icir',0), 'spread': fm.get('quintile_spread_cagr',0)*100})
            print(f"  FACTOR {sid} ICIR={row['icir']:.3f} Spread={row['spread']:.2f}%", flush=True)
        else:
            print(f"  FACTOR ERR: {r.get('error','')[:100]}", flush=True)
    except Exception as e:
        print(f"  FACTOR EX: {str(e)[:100]}", flush=True)
        traceback.print_exc()

    try:
        r2 = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score_col, score_direction=direction,
            top_n=20, sector_max=5,
            run_label=f"{display} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 {TAG}",
            **{k:v for k,v in COMMON.items() if k not in ('n_buckets','hold_months')}
        ), cb=cb)
        if r2.get('status') == 'complete':
            sid2 = save_portfolio_model(r2, overwrite=True)
            pm = r2['portfolio_metrics']
            row.update({'sharpe': pm.get('sharpe',0), 'cagr': pm.get('cagr',0)*100})
            print(f"  PORT   {sid2} CAGR={row['cagr']:.2f}% Sharpe={row['sharpe']:.3f}", flush=True)
        else:
            print(f"  PORT   ERR: {r2.get('error','')[:100]}", flush=True)
    except Exception as e:
        print(f"  PORT   EX: {str(e)[:100]}", flush=True)

    elapsed = round(time.time()-t0)
    print(f"  {elapsed}s", flush=True)
    results.append(row)

# ── 1. Re-run valuation factors with correct lower_better ─────────────────────
print("=== FIXING VALUATION FACTORS (direction was wrong) ===", flush=True)
valuation_fixes = [
    ('cyc2_ev_ebitda', 'lower_better', 'EV/EBITDA'),
    ('cyc2_pfcf',      'lower_better', 'P/FCF'),
    ('cyc2_ps',        'lower_better', 'P/Sales'),
    ('cyc2_pb',        'lower_better', 'P/Book'),
    ('cyc2_pe',        'lower_better', 'P/Earnings'),
    ('cyc2_fcf_yield', 'higher_better','FCF Yield'),
]
for score_col, direction, display in valuation_fixes:
    run_one(score_col, direction, display)

# ── 2. Fix momentum — check why PROD_OBQ_Momentum_Scores fails ───────────────
print("\n=== FIXING MOMENTUM FACTORS ===", flush=True)

# The issue: PROD_OBQ_Momentum_Scores uses 'gics_sector' not 'gic_sector'
# and the ASOF JOIN in _load_scores tries to join with v_backtest_scores which
# doesn't have these momentum columns. Need to verify the join works.
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

# Check a sample momentum join
try:
    r = con.execute("""
        SELECT COUNT(*) as n, COUNT(DISTINCT s.symbol) as nsym,
               MIN(s.month_date)::VARCHAR as min_d
        FROM PROD_OBQ_Momentum_Scores s
        JOIN v_backtest_prices p
          ON s.symbol = p.symbol
         AND p.price_date BETWEEN s.month_date::DATE - INTERVAL '5 days'
                               AND s.month_date::DATE + INTERVAL '5 days'
        WHERE s.month_date >= '1990-07-31'
          AND s.af_r12m IS NOT NULL
          AND p.adjusted_close >= 5
          AND p.market_cap >= 10000000000
        LIMIT 1
    """).fetchone()
    print(f"Momentum join test: n={r[0]}, nsym={r[1]}, min={r[2]}", flush=True)
except Exception as e:
    print(f"Momentum join test FAILED: {e}", flush=True)

# Check if AS OF join works for momentum
try:
    r2 = con.execute("""
        SELECT COUNT(*) as n
        FROM PROD_OBQ_Momentum_Scores s
        ASOF JOIN v_backtest_prices p
          ON s.symbol = p.symbol AND p.price_date <= s.month_date
        WHERE s.month_date >= '2010-06-30' AND s.month_date <= '2010-12-31'
          AND s.af_r12m IS NOT NULL
          AND p.adjusted_close >= 5
          AND p.market_cap >= 10000000000
          AND p.price_date >= (s.month_date::DATE - INTERVAL '10 days')
    """).fetchone()
    print(f"Momentum ASOF join: n={r2[0]}", flush=True)
except Exception as e:
    print(f"Momentum ASOF failed: {e}", flush=True)

con.close()

# Run momentum
momentum_factors = [
    ('cyc2_mom_3m',    'higher_better', '3-Month Momentum'),
    ('cyc2_mom_6m',    'higher_better', '6-Month Momentum'),
    ('cyc2_mom_12m',   'higher_better', '12-Month Momentum (12-1)'),
    ('cyc2_fip_6m',    'higher_better', 'FIP 6-Month (smooth path)'),
    ('cyc2_fip_12m',   'higher_better', 'FIP 12-Month (Gray/Vogel)'),
    ('cyc2_sys_score', 'higher_better', 'Systematic Score (R2)'),
]
for score_col, direction, display in momentum_factors:
    run_one(score_col, direction, display)

# ── Final summary ─────────────────────────────────────────────────────────────
print("\n=== FIX COMPLETE ===", flush=True)
for r in sorted(results, key=lambda x: x.get('icir',0), reverse=True):
    print(f"  {r['display']:<42} ICIR={r.get('icir',0):.3f} Sharpe={r.get('sharpe',0):.3f}", flush=True)
