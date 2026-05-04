# -*- coding: utf-8 -*-
"""
Full inventory of every factor/score available in the mirror DB.
Checks date range and backfill coverage back to 1990.
"""
import sys, os
sys.path.insert(0, '.')
import duckdb
from dotenv import load_dotenv; load_dotenv()

db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

print("=" * 80)
print("FULL FACTOR INVENTORY — OBQ MIRROR DB")
print("=" * 80)

# 1. All tables that look like factor/score tables
print("\n--- ALL TABLES IN MIRROR DB ---")
tables = con.execute("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name
""").fetchall()
for t in tables:
    print(f"  {t[0]:<50} [{t[1]}]")

# 2. Columns in v_backtest_scores
print("\n--- v_backtest_scores COLUMNS ---")
cols = con.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'v_backtest_scores'
    ORDER BY ordinal_position
""").fetchall()
for c in cols:
    print(f"  {c[0]:<45} {c[1]}")

# 3. Check each score column for date range and 1990 coverage
print("\n--- SCORE COLUMN DATE RANGES ---")
score_cols = [c[0] for c in cols if c[0] not in ('symbol','month_date','gic_sector','sector_id')]

print(f"  {'COLUMN':<45} {'MIN DATE':>12} {'MAX DATE':>12} {'N_PERIODS':>10} {'1990 AVAIL':>10}")
print("  " + "-"*95)
for col in score_cols:
    try:
        r = con.execute(f"""
            SELECT
                MIN(month_date)::VARCHAR,
                MAX(month_date)::VARCHAR,
                COUNT(DISTINCT month_date),
                SUM(CASE WHEN month_date <= '1993-12-31' THEN 1 ELSE 0 END) > 0 as has_early
            FROM v_backtest_scores
            WHERE {col} IS NOT NULL
        """).fetchone()
        early = "YES" if r[3] else "NO "
        print(f"  {col:<45} {(r[0] or '')[:10]:>12} {(r[1] or '')[:10]:>12} {r[2]:>10} {early:>10}")
    except Exception as e:
        print(f"  {col:<45} ERROR: {str(e)[:40]}")

# 4. Check for separate factor tables (PROD_*, LONGEQ, etc.)
print("\n--- CHECKING SEPARATE FACTOR TABLES ---")
separate_tables = [
    'PROD_LONGEQ_SCORES', 'PROD_RULEBREAKER_SCORES', 'PROD_FUNDSMITH_SCORES',
    'PROD_MOAT_SCORES', 'PROD_NDR_SCORES', 'PROD_OBQ_Value_Scores',
    'PROD_OBQ_Quality_Scores', 'PROD_OBQ_Growth_Scores',
    'PROD_OBQ_Momentum_Scores', 'PROD_OBQ_FinStr_Scores',
    'PROD_JCN_Composite_Scores',
]
for tbl in separate_tables:
    try:
        r = con.execute(f"""
            SELECT COUNT(DISTINCT symbol), 
                   MIN(month_date)::VARCHAR, 
                   MAX(month_date)::VARCHAR
            FROM {tbl}
        """).fetchone()
        print(f"  {tbl:<45} {r[1][:10]} -> {r[2][:10]}  ({r[0]:,} symbols)")
    except Exception as e:
        print(f"  {tbl:<45} NOT FOUND / {str(e)[:30]}")

# 5. Look at PROD_MOAT_SCORES columns if it exists
print("\n--- PROD_MOAT_SCORES COLUMNS (if exists) ---")
try:
    cols2 = con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'PROD_MOAT_SCORES' ORDER BY ordinal_position
    """).fetchall()
    for c in cols2:
        print(f"  {c[0]}")
    r2 = con.execute("""
        SELECT MIN(month_date)::VARCHAR, MAX(month_date)::VARCHAR, COUNT(DISTINCT symbol)
        FROM PROD_MOAT_SCORES WHERE moat_score IS NOT NULL
    """).fetchone()
    print(f"  Date range: {r2[0]} -> {r2[1]}  Symbols: {r2[2]}")
except Exception as e:
    print(f"  {e}")

# 6. Check PROD_JCN for all available composite columns
print("\n--- PROD_JCN_Composite_Scores COLUMNS ---")
try:
    cols3 = con.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'PROD_JCN_Composite_Scores' ORDER BY ordinal_position
    """).fetchall()
    for c in cols3:
        print(f"  {c[0]:<45} {c[1]}")
    r3 = con.execute("""
        SELECT MIN(month_date)::VARCHAR, MAX(month_date)::VARCHAR
        FROM PROD_JCN_Composite_Scores
    """).fetchone()
    print(f"  Date range: {r3[0]} -> {r3[1]}")
except Exception as e:
    print(f"  {e}")

con.close()
print("\nDone.")
