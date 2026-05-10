# -*- coding: utf-8 -*-
"""Check S&P 500 constituent data from Norgate and other sources."""
import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()

mirror_db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
pwb_db    = r'D:/OBQ_AI/pwb_data.duckdb'
eh_db     = r'D:/OBQ_AI/eh_research.duckdb'

print("=== CHECKING MIRROR DB FOR S&P 500 CONSTITUENT DATA ===")
con = duckdb.connect(mirror_db, read_only=True)

# Check all tables that might have constituent data
tables = [r[0] for r in con.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
).fetchall()]
print("All tables:", tables)

# Check v_backtest_prices for in_sp500 flag
print("\n--- v_backtest_prices.in_sp500 ---")
r = con.execute("""
    SELECT in_sp500, COUNT(DISTINCT symbol) as n_syms, MIN(price_date)::VARCHAR, MAX(price_date)::VARCHAR
    FROM v_backtest_prices
    WHERE in_sp500 IS NOT NULL
    GROUP BY in_sp500 ORDER BY in_sp500
""").fetchall()
for row in r:
    print(f"  in_sp500={row[0]}: {row[1]:,} symbols | {row[2]} to {row[3]}")

# Check PROD_Sector_Index_Membership
print("\n--- PROD_Sector_Index_Membership ---")
try:
    cols = [c[0] for c in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='PROD_Sector_Index_Membership' ORDER BY ordinal_position"
    ).fetchall()]
    print(f"  Columns: {cols}")
    r2 = con.execute("SELECT * FROM PROD_Sector_Index_Membership LIMIT 3").fetchdf()
    print(r2.to_string())
    r3 = con.execute("""
        SELECT COUNT(*) as n, MIN(date)::VARCHAR, MAX(date)::VARCHAR
        FROM PROD_Sector_Index_Membership
    """).fetchone()
    print(f"  Total rows: {r3[0]:,} | {r3[1]} to {r3[2]}")
except Exception as e:
    print(f"  Error: {e}")

# Check PROD_Symbol_Universe
print("\n--- PROD_Symbol_Universe ---")
try:
    cols2 = [c[0] for c in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='PROD_Symbol_Universe' ORDER BY ordinal_position"
    ).fetchall()]
    print(f"  Columns: {cols2}")
    r4 = con.execute("SELECT * FROM PROD_Symbol_Universe LIMIT 3").fetchdf()
    print(r4.to_string())
except Exception as e:
    print(f"  Error: {e}")

# Check OBQ Investable Universe
print("\n--- PROD_OBQ_Investable_Universe ---")
try:
    cols3 = [c[0] for c in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='PROD_OBQ_Investable_Universe' ORDER BY ordinal_position"
    ).fetchall()]
    print(f"  Columns: {cols3}")
    r5 = con.execute("""
        SELECT COUNT(*) as n, MIN(month_date)::VARCHAR, MAX(month_date)::VARCHAR,
               COUNT(DISTINCT symbol) as nsym
        FROM PROD_OBQ_Investable_Universe
    """).fetchone()
    print(f"  Total: {r5[0]:,} rows | {r5[1]} to {r5[2]} | {r5[3]:,} symbols")
    sample = con.execute("SELECT * FROM PROD_OBQ_Investable_Universe LIMIT 2").fetchdf()
    print(sample.to_string())
except Exception as e:
    print(f"  Error: {e}")

con.close()

# Check PWB database for constituent data
print("\n=== CHECKING PWB_DATA.DUCKDB ===")
try:
    con2 = duckdb.connect(pwb_db, read_only=True)
    tbls2 = [r[0] for r in con2.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]
    print(f"Tables: {tbls2}")
    for tbl in tbls2:
        try:
            r = con2.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
            print(f"  {tbl}: {r[0]:,} rows")
        except: pass
    con2.close()
except Exception as e:
    print(f"PWB error: {e}")

# Check EH database
print("\n=== CHECKING EH_RESEARCH.DUCKDB ===")
try:
    con3 = duckdb.connect(eh_db, read_only=True)
    tbls3 = [r[0] for r in con3.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]
    print(f"Tables: {tbls3}")
    # Look for constituent-like tables
    for tbl in tbls3:
        if any(k in tbl.lower() for k in ['constit','member','sp500','index','universe']):
            try:
                r = con3.execute(f"SELECT COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM {tbl}").fetchone()
                print(f"  {tbl}: {r[0]:,} rows | {r[1]} to {r[2]}")
            except: pass
    con3.close()
except Exception as e:
    print(f"EH error: {e}")

# Check Norgate-specific databases
print("\n=== LOOKING FOR NORGATE DBS ===")
import glob
norgate_dbs = glob.glob(r'D:/OBQ_AI/**/*norgate*.duckdb', recursive=True)
norgate_dbs += glob.glob(r'D:/**/*constituents*.duckdb', recursive=True)
norgate_dbs += glob.glob(r'D:/**/*sp500*.duckdb', recursive=True)
for db in norgate_dbs[:5]:
    print(f"  Found: {db}")
    try:
        cn = duckdb.connect(db, read_only=True)
        tbls = [r[0] for r in cn.execute("SELECT table_name FROM information_schema.tables").fetchall()]
        print(f"    Tables: {tbls[:5]}")
        cn.close()
    except Exception as e:
        print(f"    Error: {e}")
