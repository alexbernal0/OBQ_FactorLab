# -*- coding: utf-8 -*-
"""
Deduplicate factor and portfolio banks.
Keep only the NEWEST run of each unique strategy configuration.
Unique key = score_column + n_buckets/top_n + hold_months/rebalance_freq + start_date + end_date + cap_tier + min_market_cap
"""
import sys, math
sys.path.insert(0, '.')
import duckdb
from engine.strategy_bank import BANK_FILE
from engine.portfolio_bank import BANK_FILE as PM_BANK_FILE

def dedup_bank(db_path, table, key_cols, id_col='strategy_id', ts_col='created_at'):
    con = duckdb.connect(str(db_path))
    # Get all rows ordered by created_at desc
    cols_select = f"{id_col}, {ts_col}::VARCHAR, " + ", ".join(key_cols)
    rows = con.execute(f"SELECT {cols_select} FROM {table} ORDER BY {ts_col} DESC").fetchall()
    
    seen = set()
    to_delete = []
    kept = []
    for row in rows:
        sid  = row[0]
        ts   = row[1]
        keys = tuple(str(v) for v in row[2:])
        if keys in seen:
            to_delete.append(sid)
        else:
            seen.add(keys)
            kept.append((sid, ts))
    
    print(f"\n{table}:")
    print(f"  Total: {len(rows)} | Keep: {len(kept)} | Delete: {len(to_delete)}")
    
    if to_delete:
        for sid in to_delete:
            con.execute(f"DELETE FROM {table} WHERE {id_col} = ?", [sid])
            print(f"  Deleted: {sid}")
        con.commit()
        print(f"  Done. {len(kept)} unique strategies remain.")
    else:
        print(f"  No duplicates found.")
    con.close()
    return len(to_delete)

# Factor bank — key = score config
factor_key_cols = ['score_column', 'n_buckets', 'hold_months', 'start_date', 'end_date', 'cap_tier', 'rebalance_freq']
n_factor = dedup_bank(BANK_FILE, 'factor_models', factor_key_cols)

# Portfolio bank — key = portfolio config
port_key_cols = ['score_column', 'top_n', 'sector_max', 'rebalance_freq', 'start_date', 'end_date', 'cap_tier', 'stop_loss_pct']
n_port = dedup_bank(PM_BANK_FILE, 'portfolio_models', port_key_cols)

print(f"\nTotal deleted: {n_factor} factor + {n_port} portfolio = {n_factor + n_port} duplicates removed")
