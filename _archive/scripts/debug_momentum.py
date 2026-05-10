import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

# Check symbol format in momentum table
r = con.execute("SELECT symbol FROM PROD_OBQ_Momentum_Scores WHERE af_r12m IS NOT NULL LIMIT 5").fetchall()
print("Momentum symbols sample:", [x[0] for x in r])

r2 = con.execute("SELECT symbol FROM v_backtest_prices WHERE market_cap > 10e9 LIMIT 5").fetchall()
print("Price symbols sample:", [x[0] for x in r2])

# Check if momentum symbols match price symbols
r3 = con.execute("""
    SELECT COUNT(*) FROM PROD_OBQ_Momentum_Scores s
    JOIN v_backtest_prices p ON s.symbol = p.symbol
    WHERE s.month_date >= '2020-01-01' AND af_r12m IS NOT NULL
    LIMIT 1
""").fetchone()
print(f"Direct join count: {r3[0]}")

# Try with symbol format normalization
r4 = con.execute("""
    SELECT COUNT(*) FROM PROD_OBQ_Momentum_Scores s
    JOIN v_backtest_prices p ON CONCAT(s.symbol, '.US') = p.symbol
    WHERE s.month_date >= '2020-01-01' AND af_r12m IS NOT NULL
    LIMIT 1
""").fetchone()
print(f"With .US suffix join count: {r4[0]}")

# Check the gics_sector column name
r5 = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='PROD_OBQ_Momentum_Scores' ORDER BY ordinal_position").fetchall()
print(f"Momentum columns: {[c[0] for c in r5]}")

con.close()
