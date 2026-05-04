import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)
tickers = ['QQQ.US', 'MDY.US', 'IWM.US', 'SPY.US', 'IVV.US', 'DIA.US']
for t in tickers:
    r = con.execute(f"SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR, COUNT(*) FROM PROD_EOD_ETFs WHERE symbol='{t}'").fetchone()
    print(f"  {t:<12} {r[0]} -> {r[1]}  ({r[2]:,} rows)")
con.close()
