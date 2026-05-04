"""
Identify exact Sharpe/Sortino/Calmar bugs vs GIPS standard.
"""
import sys; sys.path.insert(0,'.')
import numpy as np
import pandas as pd
import duckdb, os
from dotenv import load_dotenv; load_dotenv()

MIRROR = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(MIRROR, read_only=True)
df = con.execute("""
    SELECT date::DATE AS dt, adjusted_close AS close
    FROM PROD_EOD_ETFs
    WHERE symbol = 'SPY.US'
      AND date::DATE BETWEEN '1993-01-01'::DATE AND '2024-12-31'::DATE
      AND adjusted_close > 0
    ORDER BY dt
""").fetchdf()
con.close()

df['dt'] = pd.to_datetime(df['dt'])
monthly = df.set_index('dt')['close'].resample('ME').last().dropna()
r = monthly.pct_change().dropna()
ppy = 12
rf_annual = 0.04
rf_per_period = rf_annual / ppy

eq = (1 + r).cumprod()
eq = pd.concat([pd.Series([1.0], index=[monthly.index[0]]), eq])
n_years = len(r) / ppy
cagr = float(eq.iloc[-1] ** (1/n_years) - 1)
ann_vol = float(r.std() * np.sqrt(ppy))  # sample std (ddof=1) * sqrt(12)

print("=== SHARPE INVESTIGATION ===")
print(f"CAGR: {cagr*100:.4f}%")
print(f"rf_annual: {rf_annual*100:.2f}%")
print(f"ann_vol: {ann_vol*100:.4f}%")

# Method 1: GIPS standard — (CAGR - rf) / ann_vol
sharpe_gips = (cagr - rf_annual) / ann_vol
print(f"\nMethod 1 GIPS (CAGR-rf)/vol:         {sharpe_gips:.6f}")

# Method 2: Using expected_annual return instead of CAGR
exp_annual = float(r.mean() * ppy)
sharpe_exp = (exp_annual - rf_annual) / ann_vol
print(f"Method 2 exp_annual/vol:             {sharpe_exp:.6f}  <- THIS is what engine uses")

# Method 3: Annualised from monthly Sharpe
monthly_sharpe = (r.mean() - rf_per_period) / r.std()
sharpe_annualized = monthly_sharpe * np.sqrt(ppy)
print(f"Method 3 monthly*sqrt(12):           {sharpe_annualized:.6f}")

print(f"\nEngine shows: 0.479146 -> matches Method 2 (arithmetic mean, not CAGR)")
print(f"GIPS correct: 0.435535 -> Method 1 (geometric CAGR)")
print(f"Bug: engine uses r.mean()*12 for numerator instead of CAGR")

print("\n=== SORTINO INVESTIGATION ===")
# GIPS/CFA standard: downside deviation = std of returns BELOW MAR, annualized
neg_r_std = r[r < rf_per_period]
dsd_std = float(neg_r_std.std() * np.sqrt(ppy))
sortino_std = (cagr - rf_annual) / dsd_std
print(f"Method 1 GIPS std(neg_r)*sqrt(12):   {sortino_std:.6f}  <- CORRECT")

# Engine method: RMS of returns below MAR
neg_r_rms = r[r < rf_per_period]
dsd_rms = float(np.sqrt(np.mean(neg_r_rms**2)) * np.sqrt(ppy))
sortino_rms = (cagr - rf_annual) / dsd_rms
print(f"Method 2 RMS*sqrt(12):               {sortino_rms:.6f}")

# Engine also uses exp_annual not CAGR
sortino_engine = (exp_annual - rf_annual) / dsd_rms
print(f"Method 3 exp_annual/RMS (engine):    {sortino_engine:.6f}  <- WHAT ENGINE DOES")
print(f"Engine shows: 0.423558 -> matches Method 3")
print(f"Two bugs: (1) exp_annual not CAGR, (2) RMS not std for downside dev")

print("\n=== CALMAR INVESTIGATION ===")
max_dd = float((eq/eq.cummax()-1).min())
# Standard GIPS Calmar
calmar_gips = cagr / abs(max_dd)
# OBQ modified Calmar
wr_mo = float((r > 0).mean())
calmar_obq = (cagr * wr_mo) / abs(max_dd)
print(f"GIPS Calmar (CAGR/MaxDD):            {calmar_gips:.6f}")
print(f"OBQ Calmar (CAGR*WinMo%/MaxDD):     {calmar_obq:.6f}")
print(f"Engine shows: 0.134919 -> OBQ modified (intentional, needs renaming)")

print("\n=== QUARTERLY periods_per_year RISK ===")
# If ppy=4 (quarterly) and we use r.mean()*4 for expected return:
# Monthly r.mean() would be wrong — need to use period returns
# Check: for quarterly data the vol calc is std(qtr_ret)*sqrt(4)
# This is correct IF monthly_ret input is actually quarterly periods
print("ppy=4: Sharpe uses r.mean()*4 vs CAGR — same bug amplified")
print("ppy=4: vol = std(qtr_ret)*sqrt(4) — correct if inputs are quarterly")
print("Risk: if quarterly returns fed as monthly_ret, vol understated by sqrt(3)=1.73x")
