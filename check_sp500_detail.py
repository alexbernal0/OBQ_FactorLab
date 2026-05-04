# -*- coding: utf-8 -*-
"""Deep dive into PROD_Sector_Index_Membership constituent data."""
import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

print("=== PROD_Sector_Index_Membership DEEP DIVE ===")

# Date coverage
r = con.execute("""
    SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR,
           COUNT(DISTINCT date) as n_months,
           COUNT(DISTINCT symbol) as n_symbols,
           SUM(CASE WHEN in_sp500 THEN 1 END) as sp500_rows,
           SUM(CASE WHEN in_russell_3000 THEN 1 END) as r3000_rows
    FROM PROD_Sector_Index_Membership
""").fetchone()
print(f"Date range: {r[0]} to {r[1]}")
print(f"Months: {r[2]:,} | Symbols: {r[3]:,}")
print(f"S&P 500 member rows: {r[4]:,} | Russell 3000 rows: {r[5]:,}")

# How many S&P 500 members per month (should be ~500)
r2 = con.execute("""
    SELECT date::VARCHAR as d, COUNT(*) as n_members
    FROM PROD_Sector_Index_Membership
    WHERE in_sp500 = True
    GROUP BY date ORDER BY date
""").fetchdf()
print(f"\nS&P 500 members per month:")
print(f"  Min: {r2['n_members'].min()} | Max: {r2['n_members'].max()} | Avg: {r2['n_members'].mean():.0f}")
print(f"  First month ({r2.iloc[0]['d']}): {r2.iloc[0]['n_members']} members")
print(f"  1990-07-31: {r2[r2['d']=='1990-07-31']['n_members'].values[0] if '1990-07-31' in r2['d'].values else 'N/A'} members")
print(f"  2024-06-30: {r2[r2['d']=='2024-06-30']['n_members'].values[0] if '2024-06-30' in r2['d'].values else 'N/A'} members")

# Check semi-annual dates specifically
r3 = con.execute("""
    SELECT date::VARCHAR as d, COUNT(*) as n
    FROM PROD_Sector_Index_Membership
    WHERE in_sp500 = True
    AND MONTH(date) IN (6, 12)
    AND YEAR(date) BETWEEN 1990 AND 2024
    GROUP BY date ORDER BY date
    LIMIT 10
""").fetchall()
print(f"\nSemi-annual S&P 500 membership (sample):")
for row in r3:
    print(f"  {row[0]}: {row[1]} members")

# Cross-check: how many S&P 500 members also have scores in v_backtest_scores?
r4 = con.execute("""
    SELECT COUNT(DISTINCT s.symbol) as n_with_scores
    FROM PROD_Sector_Index_Membership m
    JOIN v_backtest_scores s
        ON m.symbol = s.Symbol
        AND m.date = s.month_date
    WHERE m.in_sp500 = True
    AND m.date BETWEEN '1990-07-31' AND '2024-12-31'
    AND s.jcn_full_composite IS NOT NULL
    LIMIT 1
""").fetchone()
print(f"\nS&P 500 members WITH JCN scores: {r4[0]:,}")

# What % of S&P 500 members have scores?
r5 = con.execute("""
    SELECT 
        COUNT(DISTINCT m.symbol) as total_sp500_members,
        COUNT(DISTINCT s.symbol) as members_with_scores
    FROM PROD_Sector_Index_Membership m
    LEFT JOIN v_backtest_scores s
        ON m.symbol = s.Symbol
        AND m.date = s.month_date
        AND s.jcn_full_composite IS NOT NULL
    WHERE m.in_sp500 = True
    AND m.date BETWEEN '1990-07-31' AND '2024-12-31'
""").fetchone()
pct = r5[1]/r5[0]*100 if r5[0] > 0 else 0
print(f"S&P 500 coverage: {r5[1]:,}/{r5[0]:,} = {pct:.1f}% have JCN scores")

con.close()
print("""
=== CONCLUSION ===
PROD_Sector_Index_Membership has point-in-time S&P 500 constituent data from Norgate.
This is exactly what we need to build a proper S&P 500 EW universe benchmark.

Current situation:
- Our EW universe = all stocks with jcn_full_composite score (no index filter)
- Proper Tortoriello-style universe = S&P 1500 EW (S&P 500 + 400 + 600)
- Best OBQ equivalent = S&P 500 EW from PROD_Sector_Index_Membership

Next step: Update factor_backtest.py to use S&P 500 constituent list 
as the universe benchmark calculation, not all scored stocks.
""")
