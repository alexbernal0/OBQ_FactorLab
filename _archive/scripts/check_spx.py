import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

# Check what index/benchmark tickers we have
print("=== CHECKING BENCHMARK TICKERS IN PROD_EOD_ETFs ===")
tickers = ['SPY.US', 'GSPC.INDX', 'SP500TR.INDX', 'SPXTR.INDX', 
           '^GSPC', 'SPX.INDX', 'VFINX.US', 'IVV.US', 'VOO.US']
for t in tickers:
    try:
        r = con.execute(f"""
            SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR, COUNT(*) 
            FROM PROD_EOD_ETFs WHERE symbol='{t}'
        """).fetchone()
        if r[2] > 0:
            print(f"  {t:<20} {r[0]} -> {r[1]}  ({r[2]:,} rows)")
        else:
            print(f"  {t:<20} NOT FOUND")
    except: print(f"  {t:<20} ERROR")

# Search broadly
print("\n=== ALL SYMBOLS IN PROD_EOD_ETFs THAT LOOK LIKE S&P 500 ===")
r2 = con.execute("""
    SELECT DISTINCT symbol, MIN(date)::VARCHAR as first, MAX(date)::VARCHAR as last, COUNT(*) as n
    FROM PROD_EOD_ETFs 
    WHERE symbol ILIKE '%SP%' OR symbol ILIKE '%S&P%' OR symbol ILIKE '%500%' OR symbol ILIKE '%GSP%'
    GROUP BY symbol ORDER BY first
""").fetchall()
for r in r2:
    print(f"  {r[0]:<25} {r[1]} -> {r[2]}  ({r[3]:,} rows)")

# Check survivorship table
print("\n=== CHECKING PROD_EOD_survivorship FOR ^GSPC / SPX ===")
r3 = con.execute("""
    SELECT DISTINCT symbol, MIN(date)::VARCHAR, MAX(date)::VARCHAR, COUNT(*)
    FROM PROD_EOD_survivorship
    WHERE symbol ILIKE '%SP%' OR symbol ILIKE '%GSPC%' OR symbol ILIKE '%500%'
    GROUP BY symbol ORDER BY 2
    LIMIT 10
""").fetchall()
for r in r3:
    print(f"  {r[0]:<25} {r[1]} -> {r[2]}  ({r[3]:,} rows)")

con.close()
