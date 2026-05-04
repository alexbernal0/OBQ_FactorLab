# -*- coding: utf-8 -*-
"""Check columns and date coverage in separate factor tables."""
import sys, os; sys.path.insert(0,'.')
import duckdb
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

TABLES = ['PROD_LONGEQ_SCORES', 'PROD_RULEBREAKER_SCORES', 'PROD_FUNDSMITH_SCORES']

for tbl in TABLES:
    print(f"\n{'='*60}")
    print(f"TABLE: {tbl}")
    print(f"{'='*60}")
    cols = con.execute(f"""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = '{tbl}' ORDER BY ordinal_position
    """).fetchall()
    print("Columns:")
    for c in cols:
        print(f"  {c[0]:<40} {c[1]}")

    # Score columns only
    score_cols = [c[0] for c in cols if c[0] not in
                  ('symbol','month_date','gic_sector','calculated_at','metrics_available','factors_available')]

    print("\nDate coverage per score column:")
    print(f"  {'COLUMN':<40} {'MIN':>12} {'MAX':>12} {'N_MONTHS':>9} {'1990?':>6}")
    print("  " + "-"*75)
    for col in score_cols:
        try:
            r = con.execute(f"""
                SELECT MIN(month_date)::VARCHAR, MAX(month_date)::VARCHAR,
                       COUNT(DISTINCT month_date),
                       SUM(CASE WHEN month_date <= '1993-12-31' THEN 1 ELSE 0 END) > 0
                FROM {tbl} WHERE {col} IS NOT NULL
            """).fetchone()
            early = "YES" if r[3] else "NO"
            print(f"  {col:<40} {(r[0] or '')[:10]:>12} {(r[1] or '')[:10]:>12} {r[2]:>9} {early:>6}")
        except Exception as e:
            print(f"  {col:<40} ERROR: {str(e)[:30]}")

    # Sample a recent row
    print("\nSample row (most recent):")
    try:
        sample = con.execute(f"SELECT * FROM {tbl} ORDER BY month_date DESC LIMIT 1").fetchdf()
        for col2 in sample.columns[:12]:
            v = sample.iloc[0][col2]
            print(f"  {col2:<40} {v}")
    except Exception as e:
        print(f"  Error: {e}")

con.close()
