"""
Audit data integrity:
1. Are we using adjusted_close (not raw close)?
2. Is v_backtest_prices survivorship-bias free (includes delisted)?
3. Is v_backtest_scores point-in-time (no look-ahead)?
4. Are there any bad price spikes in the data?
"""
import sys; sys.path.insert(0,'.')
import duckdb, os
import numpy as np, pandas as pd
from dotenv import load_dotenv; load_dotenv()

db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

print("=" * 60)
print("1. PRICE TABLE STRUCTURE")
print("=" * 60)
cols = con.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name='v_backtest_prices'
    ORDER BY ordinal_position
""").fetchall()
for c in cols:
    print(f"  {c[0]}: {c[1]}")

print("\n" + "=" * 60)
print("2. ADJUSTED CLOSE vs RAW CLOSE CHECK")
print("=" * 60)
r = con.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(adjusted_close) as has_adj,
        COUNT(close) as has_raw,
        SUM(CASE WHEN adjusted_close != close THEN 1 ELSE 0 END) as adj_differs,
        AVG(adjusted_close / NULLIF(close,0)) as avg_adj_ratio
    FROM v_backtest_prices
    WHERE price_date = '2024-01-02'
      AND adjusted_close > 0
""").fetchone()
print(f"  Total rows (2024-01-02): {r[0]}")
print(f"  Has adjusted_close: {r[1]}")
print(f"  Adj != Raw (splits/divs): {r[3]}")
print(f"  Avg adj/raw ratio: {r[4]:.4f}")

print("\n" + "=" * 60)
print("3. SURVIVORSHIP BIAS CHECK")
print("=" * 60)
r2 = con.execute("""
    SELECT 
        COUNT(DISTINCT symbol) as total_symbols,
        SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_count,
        SUM(CASE WHEN is_active = false OR delisting_date IS NOT NULL THEN 1 ELSE 0 END) as delisted_count
    FROM (
        SELECT DISTINCT symbol, 
               MAX(is_active) as is_active,
               MAX(delisting_date) as delisting_date
        FROM v_backtest_prices
        GROUP BY symbol
    )
""").fetchone()
print(f"  Total unique symbols: {r2[0]}")
print(f"  Currently active: {r2[1]}")
print(f"  Delisted/inactive: {r2[2]}")
pct = r2[2] / r2[0] * 100 if r2[0] else 0
print(f"  Delisted %: {pct:.1f}%  (healthy if >20%)")

print("\n" + "=" * 60)
print("4. POINT-IN-TIME CHECK (score look-ahead)")
print("=" * 60)
r3 = con.execute("""
    SELECT COUNT(*) as n_scores,
           MIN(month_date) as min_date,
           MAX(month_date) as max_date
    FROM v_backtest_scores
    WHERE jcn_full_composite IS NOT NULL
""").fetchone()
print(f"  Score records: {r3[0]:,}")
print(f"  Score range: {r3[1]} to {r3[2]}")
print(f"  Scores are monthly snapshots — checking if they predate price data...")
r4 = con.execute("""
    SELECT COUNT(*) as n_future
    FROM v_backtest_scores s
    WHERE s.month_date > CURRENT_DATE
""").fetchone()
print(f"  Scores with future dates: {r4[0]}  (should be 0)")

print("\n" + "=" * 60)
print("5. EXTREME PRICE MOVEMENTS (potential bad data)")
print("=" * 60)
r5 = con.execute("""
    SELECT symbol, price_date, adjusted_close,
           LAG(adjusted_close) OVER (PARTITION BY symbol ORDER BY price_date) as prev_close,
           adjusted_close / NULLIF(LAG(adjusted_close) OVER (PARTITION BY symbol ORDER BY price_date), 0) - 1 as pct_chg
    FROM v_backtest_prices
    WHERE price_date >= '1990-01-01'
""").fetchdf()

extreme = r5[(r5['pct_chg'].abs() > 2.0) & (r5['pct_chg'].notna())]
print(f"  Rows with >200% single-day move: {len(extreme)}")
if len(extreme) > 0:
    print(f"  Sample extreme moves:")
    for _, row in extreme.head(10).iterrows():
        print(f"    {row['symbol']} on {row['price_date']}: {row['pct_chg']*100:.1f}%  (${row['prev_close']:.2f} -> ${row['adjusted_close']:.2f})")

print("\n" + "=" * 60)
print("6. PORTFOLIO BACKTEST PRICE USAGE")
print("=" * 60)
print("  Checking portfolio_backtest.py for price field used...")
import subprocess
result = subprocess.run(['findstr', '/n', 'adjusted_close\|raw_close\|p.close', 
                        r'C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab\engine\portfolio_backtest.py'],
                       capture_output=True, text=True)
for line in result.stdout.strip().split('\n')[:15]:
    if line.strip():
        print(f"  {line}")

con.close()
