# -*- coding: utf-8 -*-
"""
Bulk reseed all CYC-001 factor + portfolio models.
Saves trade logs to both:
  1. trade_log_json in strategy_bank (for tearsheet replay)
  2. trade_log.duckdb dedicated table (for fast SQL queries)
"""
import sys, time, traceback
sys.path.insert(0, '.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model, get_all_portfolio_models
from engine.trade_log_db import count_trades

COMMON_F = dict(start_date='1990-07-31', end_date='2024-12-31',
                min_price=5.0, min_adv_usd=1_000_000.0,
                min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0)
COMMON_P = dict(**COMMON_F)

def _cb(msg):
    if any(k in msg.lower() for k in ['trade log', 'complete', 'error', 'saved', 'factor backtest']):
        print(f"    {msg}")

print("=== BULK TRADE LOG RESEED ===")
print(f"Starting counts: {count_trades()}")

# Load all CYC-001 models
all_f = [m for m in get_all_models(200) if 'CYC-001' in (m.get('run_label') or '')]
all_p = [m for m in get_all_portfolio_models(200) if 'CYC-001' in (m.get('run_label') or '')]
print(f"Factor: {len(all_f)} models | Portfolio: {len(all_p)} models")

f_done, f_err, p_done, p_err = 0, 0, 0, 0

# ── FACTOR MODELS ─────────────────────────────────────────────────────────
for m in all_f:
    score = m.get('score_column')
    label = m.get('run_label', '')
    rebal = m.get('rebalance_freq', 'semi-annual')
    hold  = m.get('hold_months', 6)
    try:
        r = run_factor_backtest(FactorBacktestConfig(
            score_column=score, n_buckets=5, hold_months=hold,
            rebalance_freq=rebal, cap_tier='all',
            run_label=label, **COMMON_F
        ), cb=_cb)
        if r.get('status') == 'complete':
            tl = r.get('trade_log', [])
            sid = save_factor_model(r, overwrite=True)
            print(f"  [F] {sid} | {len(tl)} trades | {score}")
            f_done += 1
        else:
            print(f"  [F] ERROR {score}: {r.get('error','')[:60]}")
            f_err += 1
    except Exception as e:
        print(f"  [F] EXCEPTION {score}: {str(e)[:60]}")
        f_err += 1

# ── PORTFOLIO MODELS ───────────────────────────────────────────────────────
for m in all_p:
    score  = m.get('score_column')
    label  = m.get('run_label', '')
    rebal  = m.get('rebalance_freq', 'semi-annual')
    top_n  = m.get('top_n', 20)
    sec_max= m.get('sector_max', 5)
    try:
        r = run_portfolio_backtest(PortfolioBacktestConfig(
            score_column=score, top_n=top_n, sector_max=sec_max,
            rebalance_freq=rebal, cap_tier='all',
            run_label=label, **COMMON_P
        ), cb=_cb)
        if r.get('status') == 'complete':
            tl = r.get('trade_log', [])
            sid = save_portfolio_model(r, overwrite=True)
            print(f"  [P] {sid} | {len(tl)} trades | {score}")
            p_done += 1
        else:
            print(f"  [P] ERROR {score}: {r.get('error','')[:60]}")
            p_err += 1
    except Exception as e:
        print(f"  [P] EXCEPTION {score}: {str(e)[:60]}")
        p_err += 1

print(f"\n=== DONE ===")
print(f"Factor:    {f_done} saved, {f_err} errors")
print(f"Portfolio: {p_done} saved, {p_err} errors")
print(f"Final trade DB: {count_trades()}")
