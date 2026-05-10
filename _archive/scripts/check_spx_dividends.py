"""
Verify whether SPX in PWB_indices is price-only or total return.
Compare annual returns vs SPY adj_close (known total return) and
vs published S&P 500 price-only returns.
"""
import duckdb
import pandas as pd
import numpy as np

# Known S&P 500 annual returns (price only, widely published):
PRICE_ONLY = {
    1990: -6.56, 1991: 26.31, 1992: 4.46, 1993: 7.06,
    1994: -1.54, 1995: 34.11, 1996: 20.26, 1997: 31.01,
    1998: 26.67, 1999: 19.53, 2000: -10.14,
}

# Known S&P 500 total return (price + dividends):
# Source: Shiller, Damodaran
TOTAL_RETURN = {
    1990: -3.10, 1991: 30.47, 1992: 7.62, 1993: 10.08,
    1994: 1.32,  1995: 37.58, 1996: 22.96, 1997: 33.36,
    1998: 28.58, 1999: 21.04, 2000: -9.10,
}

# Our SPX data
con = duckdb.connect(r'D:/OBQ_AI/pwb_data.duckdb', read_only=True)
prices = con.execute("""
    SELECT date::DATE as dt, adj_close as close
    FROM PWB_indices WHERE symbol='SPX'
    AND date >= '1989-12-01' ORDER BY dt
""").fetchdf()
con.close()

prices['dt'] = pd.to_datetime(prices['dt'])
prices = prices.set_index('dt').sort_index()
monthly = prices['close'].resample('ME').last().dropna()
annual = monthly.resample('YE').last().pct_change().dropna()

print(f"{'YEAR':<6} {'OUR SPX':>10} {'PRICE-ONLY':>12} {'TOTAL RET':>12} {'MATCH'}")
print("-" * 55)
for yr, r in annual['1990':'2000'].items():
    y = yr.year
    our = r * 100
    po  = PRICE_ONLY.get(y, 0)
    tr  = TOTAL_RETURN.get(y, 0)
    diff_po = abs(our - po)
    diff_tr = abs(our - tr)
    match = "PRICE" if diff_po < diff_tr else "TOTAL"
    print(f"  {y}  {our:>9.2f}%  {po:>10.2f}%  {tr:>10.2f}%   {match} (diff: {min(diff_po,diff_tr):.2f}%)")

print()
print("If 'MATCH' = PRICE consistently -> SPX is price-only, need dividend adjustment")
print("If 'MATCH' = TOTAL consistently -> SPX already includes dividends (lucky!)")
