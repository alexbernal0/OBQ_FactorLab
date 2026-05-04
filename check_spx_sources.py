import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

# Check all tables for GSPC or index data
print("=== SEARCHING ALL TABLES FOR S&P 500 INDEX DATA ===")
tables = con.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema='main' ORDER BY table_name
""").fetchall()

for (tbl,) in tables:
    try:
        cols = [r[0] for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name='{tbl}'
        """).fetchall()]
        if 'symbol' in cols or 'Symbol' in cols:
            sym_col = 'symbol' if 'symbol' in cols else 'Symbol'
            r = con.execute(f"""
                SELECT COUNT(*) FROM {tbl} 
                WHERE {sym_col} IN ('GSPC.INDX','SP500TR.INDX','^GSPC','SPX','GSPC')
            """).fetchone()
            if r[0] > 0:
                print(f"  FOUND in {tbl}: {r[0]} rows")
    except: pass

# SPY pre-1993 gap — how much data do we miss?
spy_start = con.execute("SELECT MIN(date)::VARCHAR FROM PROD_EOD_ETFs WHERE symbol='SPY.US'").fetchone()[0]
print(f"\nSPY starts: {spy_start}")
print(f"Our backtest starts: 1990-07-31")
print(f"Gap without SPX: {spy_start} = about 2.5 years missing")

# Check if EODHD has index data in a different table
print("\n=== CHECKING FOR INDX SYMBOLS ===")
for tbl, in tables:
    try:
        cols = [r[0] for r in con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tbl}'").fetchall()]
        sym_col = next((c for c in cols if c.lower() == 'symbol'), None)
        if sym_col:
            r = con.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {sym_col} LIKE '%.INDX'").fetchone()
            if r[0] > 0:
                sample = con.execute(f"SELECT DISTINCT {sym_col} FROM {tbl} WHERE {sym_col} LIKE '%.INDX' LIMIT 5").fetchall()
                print(f"  {tbl}: {r[0]} INDX rows | samples: {[x[0] for x in sample]}")
    except: pass

con.close()
