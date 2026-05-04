"""Check trade log storage state across both banks."""
import sys, json
sys.path.insert(0,'.')
import duckdb
from engine.strategy_bank import BANK_FILE, get_all_models
from engine.portfolio_bank import BANK_FILE as PM_BANK_FILE, get_all_portfolio_models, get_portfolio_model

# Check factor bank schema
con_f = duckdb.connect(str(BANK_FILE), read_only=True)
cols_f = [r[0] for r in con_f.execute(
    "SELECT column_name FROM information_schema.columns WHERE table_name='factor_models' ORDER BY ordinal_position"
).fetchall()]
print(f"Factor schema has trade_log_json: {'trade_log_json' in cols_f}")

# Check how many have populated trade_log_json
r = con_f.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN trade_log_json IS NOT NULL AND trade_log_json != '[]' AND LENGTH(trade_log_json) > 10 THEN 1 END) as has_trades,
        COUNT(CASE WHEN trade_log_json IS NULL OR trade_log_json = '[]' OR LENGTH(trade_log_json) <= 10 THEN 1 END) as no_trades
    FROM factor_models
""").fetchone()
print(f"Factor models: total={r[0]}, with_trades={r[1]}, missing_trades={r[2]}")

# Sample a model with trades vs without
sample_with = con_f.execute("SELECT strategy_id, LENGTH(trade_log_json) as tl_len FROM factor_models WHERE trade_log_json IS NOT NULL AND LENGTH(trade_log_json) > 100 LIMIT 3").fetchall()
sample_without = con_f.execute("SELECT strategy_id, trade_log_json FROM factor_models WHERE trade_log_json IS NULL OR LENGTH(trade_log_json) <= 10 LIMIT 3").fetchall()
print(f"\nWith trades (sample): {[(r[0], r[1]) for r in sample_with]}")
print(f"Without trades (sample): {[r[0] for r in sample_without]}")
con_f.close()

# Check portfolio bank
con_p = duckdb.connect(str(PM_BANK_FILE), read_only=True)
cols_p = [r[0] for r in con_p.execute(
    "SELECT column_name FROM information_schema.columns WHERE table_name='portfolio_models' ORDER BY ordinal_position"
).fetchall()]
print(f"\nPortfolio schema has trade_log_json: {'trade_log_json' in cols_p}")

r2 = con_p.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN trade_log_json IS NOT NULL AND trade_log_json != '[]' AND LENGTH(trade_log_json) > 10 THEN 1 END) as has_trades,
        COUNT(CASE WHEN trade_log_json IS NULL OR trade_log_json = '[]' OR LENGTH(trade_log_json) <= 10 THEN 1 END) as no_trades
    FROM portfolio_models
""").fetchone()
print(f"Portfolio models: total={r2[0]}, with_trades={r2[1]}, missing_trades={r2[2]}")
con_p.close()
