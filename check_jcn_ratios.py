"""Check all computed ratios in JCN composite score tables."""
import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

tables_to_check = [
    'PROD_OBQ_Value_Scores',
    'PROD_OBQ_Quality_Scores',
    'PROD_OBQ_Growth_Scores',
    'PROD_OBQ_Momentum_Scores',
    'PROD_OBQ_FinStr_Scores',
    'PROD_LONGEQ_SCORES',
    'PROD_MOAT_SCORES',
    'PROD_RULEBREAKER_SCORES',
    'PROD_FUNDSMITH_SCORES',
]

for tbl in tables_to_check:
    try:
        cols = con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='{tbl}' ORDER BY ordinal_position
        """).fetchall()
        r = con.execute(f"SELECT MIN(month_date)::VARCHAR, MAX(month_date)::VARCHAR, COUNT(DISTINCT symbol) FROM {tbl}").fetchone()
        print(f"\n{tbl}")
        print(f"  {r[0]} to {r[1]} | {r[2]:,} symbols")
        print(f"  Columns: {[c[0] for c in cols]}")
    except Exception as e:
        print(f"\n{tbl}: {e}")

con.close()
