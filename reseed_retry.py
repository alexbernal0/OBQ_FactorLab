# -*- coding: utf-8 -*-
"""Retry failed models from bulk reseed — sequential, with retry on DB lock."""
import sys, time, traceback
sys.path.insert(0, '.')
from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model, get_all_models
from engine.portfolio_bank import save_portfolio_model, get_all_portfolio_models
from engine.trade_log_db import count_trades, save_factor_trades, save_portfolio_trades

COMMON = dict(start_date='1990-07-31', end_date='2024-12-31',
              min_price=5.0, min_adv_usd=1_000_000.0,
              min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0)

def _cb(msg):
    if any(k in msg.lower() for k in ['trade log', 'complete', 'error']):
        print(f"    {msg}")

def run_with_retry(fn, cfg, max_tries=3):
    for attempt in range(max_tries):
        try:
            return fn(cfg, cb=_cb)
        except Exception as e:
            if 'cannot open file' in str(e).lower() or 'io error' in str(e).lower():
                print(f"    DB lock, retry {attempt+1}/{max_tries}...")
                time.sleep(5 * (attempt + 1))
            else:
                raise
    return {"status": "error", "error": "max retries exceeded"}

# ── Factor: 7 failed ─────────────────────────────────────────────────────────
FAILED_FACTOR = ['growth_score', 'finstr_score_universe', 'momentum_fip_score',
                 'rulebreaker_rank', 'longeq_rank', 'moat_rank', 'fundsmith_rank']

all_f = {m['score_column']: m for m in get_all_models(200) if 'CYC-001' in (m.get('run_label') or '')}
f_done, f_err = 0, 0
print(f"=== RETRY: {len(FAILED_FACTOR)} factor + all portfolio ===")
print(f"Starting: {count_trades()}")

for score in FAILED_FACTOR:
    m = all_f.get(score)
    if not m:
        print(f"  [F] {score}: not found in bank, skipping")
        continue
    print(f"  [F] {score}...")
    try:
        r = run_with_retry(run_factor_backtest, FactorBacktestConfig(
            score_column=score, n_buckets=5,
            hold_months=m.get('hold_months', 6),
            rebalance_freq=m.get('rebalance_freq', 'semi-annual'),
            cap_tier='all', run_label=m.get('run_label', ''), **COMMON
        ))
        if r.get('status') == 'complete':
            tl = r.get('trade_log', [])
            sid = save_factor_model(r, overwrite=True)
            print(f"    Saved {sid} | {len(tl)} trades")
            f_done += 1
        else:
            print(f"    ERROR: {r.get('error','')[:80]}")
            f_err += 1
    except Exception as e:
        print(f"    EXCEPTION: {str(e)[:80]}")
        f_err += 1

# ── Portfolio: all 31 ─────────────────────────────────────────────────────────
all_p = [m for m in get_all_portfolio_models(200) if 'CYC-001' in (m.get('run_label') or '')]
p_done, p_err = 0, 0
for m in all_p:
    score = m.get('score_column')
    print(f"  [P] {score}...")
    try:
        r = run_with_retry(run_portfolio_backtest, PortfolioBacktestConfig(
            score_column=score, top_n=m.get('top_n', 20),
            sector_max=m.get('sector_max', 5),
            rebalance_freq=m.get('rebalance_freq', 'semi-annual'),
            cap_tier='all', run_label=m.get('run_label', ''), **COMMON
        ))
        if r.get('status') == 'complete':
            tl = r.get('trade_log', [])
            sid = save_portfolio_model(r, overwrite=True)
            print(f"    Saved {sid} | {len(tl)} trades")
            p_done += 1
        else:
            print(f"    ERROR: {r.get('error','')[:80]}")
            p_err += 1
    except Exception as e:
        print(f"    EXCEPTION: {str(e)[:80]}")
        p_err += 1

print(f"\n=== RETRY DONE ===")
print(f"Factor:    {f_done} saved, {f_err} errors")
print(f"Portfolio: {p_done} saved, {p_err} errors")
print(f"Final: {count_trades()}")
