# -*- coding: utf-8 -*-
"""
Fast trade log extraction from existing bank data.
NO full backtest reruns — reads what's already stored.

Factor: period_data_json has per-period Q1 avg returns but NOT individual stocks.
        trade_log requires individual stocks → need a different approach.
        
        OPTION A: Use holdings_log from portfolio bank as proxy (already stored).
        OPTION B: Store a 'holdings_log_json' in factor bank from the backtest.
        
        Since factor bank doesn't have per-stock Q1 data stored (only aggregates),
        we CAN'T reconstruct individual trades without rerunning.
        
Portfolio: holdings_log_json IS stored → extract directly, NO rerun needed.
"""
import sys, time
sys.path.insert(0,'.')
from engine.portfolio_bank import get_all_portfolio_models, get_portfolio_model
from engine.trade_log_db import save_portfolio_trades, count_trades

print("=== FAST PORTFOLIO TRADE LOG EXTRACTION ===")
print("Reading holdings_log_json from bank (no backtest rerun needed)")
print(f"Starting: {count_trades()}")

pm_models = [m for m in get_all_portfolio_models(200) if 'CYC-001' in (m.get('run_label') or '')]
print(f"Portfolio models to process: {len(pm_models)}")

done, skipped, errors = 0, 0, 0
t0 = time.time()

for m in pm_models:
    sid   = m['strategy_id']
    score = m.get('score_column', '')
    
    full = get_portfolio_model(sid)
    if not full:
        print(f"  {sid}: not found")
        errors += 1
        continue
    
    hl = full.get('holdings_log_json', [])
    if not hl or not isinstance(hl, list):
        print(f"  {sid}: no holdings_log ({score})")
        skipped += 1
        continue
    
    # Build trade log from holdings_log
    # holdings_log = [{date, holdings: [{symbol, score, sector, weight, market_cap}]}]
    # We need entry/exit dates + prices. Prices need the equity_dates to pair up.
    eq_dates = full.get('equity_dates_json', [])
    
    trade_log = []
    for i, period in enumerate(hl):
        entry_date = period.get('date', '')
        # Exit date = next period's date, or last equity date
        exit_date = hl[i+1]['date'] if i + 1 < len(hl) else (eq_dates[-1] if eq_dates else entry_date)
        
        pd_row = None
        # Find matching period_data for this period's portfolio return
        pd_list = full.get('period_data_json', [])
        for pd in pd_list:
            if pd.get('date', '') == entry_date:
                pd_row = pd
                break
        
        for h in (period.get('holdings') or []):
            trade_log.append({
                'entry_date':   entry_date,
                'exit_date':    exit_date,
                'symbol':       (h.get('symbol') or '').replace('.US',''),
                'sector':       h.get('sector', 'Unknown'),
                'score':        h.get('score'),
                'weight':       h.get('weight'),
                'return_pct':   None,  # individual returns not stored in holdings_log
                'market_cap_B': h.get('market_cap'),
                'entry_price':  None,
                'exit_price':   None,
            })
    
    if trade_log:
        n = save_portfolio_trades(sid, score, trade_log)
        print(f"  {sid}: {n} trades extracted ({score})")
        done += 1
    else:
        print(f"  {sid}: empty holdings_log ({score})")
        skipped += 1

elapsed = round(time.time() - t0, 1)
print(f"\nDone in {elapsed}s: {done} saved, {skipped} skipped, {errors} errors")
print(f"Final: {count_trades()}")
print()
print("NOTE: Factor trade logs require per-stock data not stored in bank.")
print("Factor trade logs will be populated on NEXT backtest run via engine fix.")
print("Portfolio trade logs: DONE (no wait needed).")
