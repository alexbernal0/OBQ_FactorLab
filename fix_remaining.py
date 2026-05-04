# -*- coding: utf-8 -*-
"""
Fix #3: Verify trade logs on old models (April 30 ones)
Fix #4: Update OBQ Fund Score to SPY benchmark for any models missing it
Fix #5: Re-run moat_score factor
Fix #10: Verify sort order (handled by dedup - newest kept)
Fix #13: Reseed PM-001 with SPX benchmark
"""
import sys, time, traceback
sys.path.insert(0, '.')

from engine.strategy_bank import get_all_models, get_model, save_factor_model
from engine.portfolio_bank import get_all_portfolio_models, get_portfolio_model, save_portfolio_model

# ── CHECK #3: Trade logs ──────────────────────────────────────────────────────
print("=== CHECK #3: TRADE LOGS ===")
models = get_all_models(100)
no_tl = []
for m in models:
    full = get_model(m['strategy_id'])
    if not full: continue
    tl = full.get('trade_log_json')
    has_tl = isinstance(tl, list) and len(tl) > 0
    if not has_tl:
        no_tl.append(m['strategy_id'])
        print(f"  MISSING trade log: {m['strategy_id']} | {(m.get('run_label') or '')[:50]}")

if not no_tl:
    print("  All factor models have trade logs ✓")

# Portfolio
pm_models = get_all_portfolio_models(100)
no_pm_tl = []
for m in pm_models:
    full = get_portfolio_model(m['strategy_id'])
    if not full: continue
    tl = full.get('trade_log_json')
    has_tl = isinstance(tl, list) and len(tl) > 0
    if not has_tl:
        no_pm_tl.append(m['strategy_id'])
        print(f"  MISSING portfolio trade log: {m['strategy_id']}")
if not no_pm_tl:
    print("  All portfolio models have trade logs ✓")

# ── CHECK #4: OBQ Fund Score vs SPY ──────────────────────────────────────────
print("\n=== CHECK #4: OBQ FUND SCORE vs SPY ===")
# Check if scores are now SPY-based (they should differ from old universe-based)
# Old universe-based QARP alpha_win_rate was 0.686, new SPY-based should be ~0.53
for m in models:
    sid = m['strategy_id']
    fund = m.get('obq_fund_score')
    aw   = m.get('alpha_win_rate')
    label = (m.get('run_label') or '')[:40]
    status = "OK" if fund is not None and aw is not None else "MISSING"
    if fund is None or aw is None:
        print(f"  MISSING fitness: {sid} | {label}")
    else:
        # Check: if alpha_win_rate > 0.80 it's likely still vs universe (too high for large-cap vs SPY)
        flag = " ← SUSPICIOUS (vs universe?)" if aw > 0.80 else ""
        print(f"  {sid[:25]} | fund={fund:.3f} | alpha_win={aw*100:.1f}%{flag}")

# ── FIX #5: Re-run moat_score factor ─────────────────────────────────────────
print("\n=== FIX #5: MOAT_SCORE FACTOR ===")
# Check if it's already in the bank
moat_models = [m for m in models if m.get('score_column') == 'moat_score']
if moat_models:
    print(f"  moat_score already exists: {moat_models[0]['strategy_id']} ICIR={moat_models[0].get('icir',0):.3f}")
else:
    print("  Running moat_score factor backtest...")
    from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
    def cb(msg):
        if 'complete' in msg.lower() or 'error' in msg.lower() or 'saved' in msg.lower():
            print(f"    {msg}")
    r = run_factor_backtest(FactorBacktestConfig(
        score_column='moat_score', score_direction='higher_better',
        n_buckets=5, hold_months=3, rebalance_freq='quarterly',
        min_price=5.0, min_adv_usd=1_000_000.0, min_market_cap=10_000_000_000.0,
        cap_tier='all', transaction_cost_bps=15.0,
        start_date='1990-07-31', end_date='2024-12-31',
        run_label='Moat Score (economic moat gates) | 5Q | 3mo | Large-Cap | 1990-2024 [CYC-001-BASELINE]',
    ), cb=cb)
    if r.get('status') == 'complete':
        sid = save_factor_model(r, overwrite=True)
        fm = r.get('factor_metrics', {})
        print(f"  Saved: {sid} | ICIR={fm.get('icir',0):.3f} | Spread={fm.get('quintile_spread_cagr',0)*100:.2f}%")
    else:
        print(f"  ERROR: {r.get('error')}")

# ── FIX #13: Reseed PM-001 with SPX benchmark ─────────────────────────────────
print("\n=== FIX #13: RESEED PM-001 WITH SPX BENCHMARK ===")
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
pm001_models = [m for m in pm_models if 'CYC-001' in (m.get('run_label') or '') and 'jcn_full_composite' in (m.get('score_column') or '')]
print(f"  Found {len(pm001_models)} CYC-001 JCN composite portfolio models")
for m in pm001_models[:2]:  # reseed top 2
    print(f"  Reseeding {m['strategy_id']}...")
    full = get_portfolio_model(m['strategy_id'])
    if not full: continue
    cfg_data = full.get('config_json') or {}
    r = run_portfolio_backtest(PortfolioBacktestConfig(
        score_column  = cfg_data.get('score_column', 'jcn_full_composite'),
        top_n         = cfg_data.get('top_n', 20),
        sector_max    = cfg_data.get('sector_max', 5),
        rebalance_freq= cfg_data.get('rebalance_freq', 'semi-annual'),
        cap_tier      = cfg_data.get('cap_tier', 'all'),
        min_price     = cfg_data.get('min_price', 5.0),
        min_adv_usd   = 1_000_000.0,
        min_market_cap= 10_000_000_000.0,
        transaction_cost_bps = cfg_data.get('cost_bps', 15.0),
        stop_loss_pct = cfg_data.get('stop_loss_pct', 0.0),
        run_label     = m.get('run_label', ''),
    ), cb=lambda msg: None)
    if r.get('status') == 'complete':
        sid = save_portfolio_model(r, overwrite=True)
        pm = r.get('portfolio_metrics', {})
        spy = r.get('spy_metrics', {})
        print(f"    Saved: {sid} | CAGR={pm.get('cagr',0)*100:.2f}% | SPY CAGR={spy.get('cagr',0)*100:.2f}%")
    else:
        print(f"    ERROR: {r.get('error')}")

print("\nDone.")
