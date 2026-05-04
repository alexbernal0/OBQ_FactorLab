import duckdb
import pandas as pd
import numpy as np

# Check PWB SPX data quality around 1990
con = duckdb.connect(r'D:/OBQ_AI/pwb_data.duckdb', read_only=True)
cols = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='PWB_indices'").fetchall()
print("PWB_indices columns:", [c[0] for c in cols])

sample = con.execute("""
    SELECT * FROM PWB_indices WHERE symbol='SPX' 
    AND date BETWEEN '1990-01-01' AND '1990-12-31'
    ORDER BY date LIMIT 5
""").fetchdf()
print("\nSPX sample 1990:")
print(sample.to_string())

# Compute annual returns from SPX close (price return only)
annual = con.execute("""
    SELECT 
        YEAR(date) as yr,
        LAST(close ORDER BY date) as year_end,
        FIRST(close ORDER BY date) as year_start
    FROM PWB_indices 
    WHERE symbol='SPX' AND date >= '1989-12-01'
    GROUP BY yr ORDER BY yr
    LIMIT 10
""").fetchdf()
annual['ret'] = annual['year_end'] / annual['year_start'].shift(-1).shift(1) - 1
print("\nSPX annual price returns (1990-1999):")
# Compute properly
prices = con.execute("""
    SELECT date::DATE as dt, close 
    FROM PWB_indices WHERE symbol='SPX' 
    ORDER BY date
""").fetchdf()
con.close()

prices['dt'] = pd.to_datetime(prices['dt'])
prices = prices.set_index('dt').sort_index()
monthly = prices['close'].resample('ME').last().dropna()
annual_ret = monthly.resample('YE').last().pct_change().dropna()

print("SPX Price Return (no dividends) 1990-2000:")
for yr, r in annual_ret['1990':'2000'].items():
    print(f"  {yr.year}: {r*100:.2f}%")

# Note: SPX is price-only. S&P 500 TOTAL return adds ~1.5-2% dividends per year.
# For proper comparison we should use SPY (which includes dividends via adj_close)
# OR add a dividend yield adjustment to SPX

# Check SPY for comparison
con2 = duckdb.connect(r'D:/OBQ_AI/obq_eodhd_mirror.duckdb', read_only=True)
spy_annual = con2.execute("""
    SELECT YEAR(date) as yr, 
           LAST(adjusted_close ORDER BY date)/FIRST(adjusted_close ORDER BY date)-1 as ret
    FROM PROD_EOD_ETFs WHERE symbol='SPY.US' AND date >= '1993-01-01'
    GROUP BY yr ORDER BY yr LIMIT 10
""").fetchdf()
con2.close()
print("\nSPY Total Return 1993-2002:")
for _, row in spy_annual.iterrows():
    print(f"  {int(row['yr'])}: {row['ret']*100:.2f}%")
