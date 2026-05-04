import sys; sys.path.insert(0,'.')
import duckdb
from engine.strategy_bank import BANK_FILE
con = duckdb.connect(str(BANK_FILE))
cols = sorted([r[0] for r in con.execute(
    "SELECT column_name FROM information_schema.columns WHERE table_name='factor_models'"
).fetchall()])
print("Existing columns:", cols)
con.close()
