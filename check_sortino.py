import sys; sys.path.insert(0,'.')
import numpy as np, pandas as pd, duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)
df = con.execute("""
    SELECT date::DATE AS dt, adjusted_close AS close
    FROM PROD_EOD_ETFs
    WHERE symbol='SPY.US'
      AND date::DATE BETWEEN '1993-01-01' AND '2024-12-31'
      AND adjusted_close > 0
    ORDER BY dt
""").fetchdf()
con.close()

df['dt'] = pd.to_datetime(df['dt'])
monthly = df.set_index('dt')['close'].resample('ME').last().dropna()
r = monthly.pct_change().dropna()
rf = 0.04 / 12
neg = r[r < rf]

print(f"Negative returns: {len(neg)} of {len(r)}")
print(f"std ddof=1 (pandas default): {neg.std(ddof=1)*np.sqrt(12)*100:.4f}%")
print(f"std ddof=0 (population):     {neg.std(ddof=0)*np.sqrt(12)*100:.4f}%")

cagr = (1 + r).cumprod().iloc[-1] ** (1/(len(r)/12)) - 1
print(f"\nSortino ddof=1: {(cagr-0.04)/(neg.std(ddof=1)*np.sqrt(12)):.6f}")
print(f"Sortino ddof=0: {(cagr-0.04)/(neg.std(ddof=0)*np.sqrt(12)):.6f}")
print(f"Manual target:  0.605395")
print()
# The CFA textbook uses population std (ddof=0) for downside deviation
# Let's check which matches
print("CFA Institute definition uses population std (divides by N, not N-1)")
print(f"-> ddof=0 gives {(cagr-0.04)/(neg.std(ddof=0)*np.sqrt(12)):.6f}")
