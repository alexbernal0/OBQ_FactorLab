"""Delete old bad PM models, keep only the latest."""
import sys; sys.path.insert(0,'.')
import duckdb
from engine.portfolio_bank import BANK_FILE

con = duckdb.connect(str(BANK_FILE))
rows = con.execute("SELECT strategy_id, created_at FROM portfolio_models ORDER BY created_at").fetchall()
print("Current models:")
for r in rows:
    print(f"  {r[0]}  created: {r[1]}")

# Delete all except the newest
if len(rows) > 1:
    to_delete = [r[0] for r in rows[:-1]]
    for sid in to_delete:
        con.execute("DELETE FROM portfolio_models WHERE strategy_id = ?", [sid])
        print(f"Deleted: {sid}")
    con.commit()
    print("Done.")
else:
    print("Only one model, nothing to delete.")
con.close()
