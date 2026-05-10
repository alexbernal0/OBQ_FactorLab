"""Check what raw fundamental data we have available to compute factors."""
import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

# Check v_backtest_filings
print("=== v_backtest_filings COLUMNS ===")
cols = con.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'v_backtest_filings'
    ORDER BY ordinal_position
""").fetchall()
for c in cols:
    print(f"  {c[0]:<45} {c[1]}")

# Check coverage
r = con.execute("""
    SELECT MIN(filing_date)::VARCHAR, MAX(filing_date)::VARCHAR,
           COUNT(DISTINCT symbol) as n_symbols,
           COUNT(*) as n_rows
    FROM v_backtest_filings
    WHERE filing_date IS NOT NULL
""").fetchone()
print(f"\nCoverage: {r[0]} to {r[1]} | {r[2]:,} symbols | {r[3]:,} rows")

# Check PROD_EOD_Fundamentals
print("\n=== PROD_EOD_Fundamentals COLUMNS ===")
try:
    cols2 = con.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'PROD_EOD_Fundamentals'
        ORDER BY ordinal_position
    """).fetchall()
    for c in cols2:
        print(f"  {c[0]:<45} {c[1]}")
    r2 = con.execute("SELECT COUNT(*) FROM PROD_EOD_Fundamentals").fetchone()
    print(f"\nTotal rows: {r2[0]:,}")
except Exception as e:
    print(f"  Error: {e}")

# Check v_backtest_scores for raw ratio columns
print("\n=== v_backtest_scores RAW RATIO COLUMNS ===")
cols3 = con.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'v_backtest_scores'
    AND column_name NOT LIKE '%score%'
    AND column_name NOT LIKE '%jcn%'
    AND column_name NOT LIKE '%af_%'
    ORDER BY ordinal_position
""").fetchall()
for c in cols3:
    print(f"  {c[0]:<45} {c[1]}")

con.close()
