import sys; sys.path.insert(0,'.')
from engine.portfolio_bank import get_portfolio_model
m = get_portfolio_model('PM-JCNFUL-20260430-BE81')
bm = m.get('bm_metrics_json', {})
pm = m.get('portfolio_metrics_json', {})
bm_eq = m.get('bm_equity_json', [])
ann = m.get('annual_ret_json', [])

print(f"Portfolio CAGR: {pm.get('cagr',0)*100:.2f}%")
print(f"BM CAGR:        {bm.get('cagr',0)*100:.2f}%")
print(f"BM final eq:    {bm_eq[-1] if bm_eq else None:.2f}")
print()
print("Annual returns (portfolio vs universe):")
for a in ann[:5]:
    print(f"  {a['year']}: port={a['portfolio_ret']*100:.1f}%  univ={a['universe_ret']*100:.1f}%  spy={a['spy_ret']*100:.1f}% " if a['spy_ret'] else f"  {a['year']}: port={a['portfolio_ret']*100:.1f}%  univ={a['universe_ret']*100:.1f}%")

# What symbols are in the universe each period?
# Check how many stocks go into the EW calc
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)
r = con.execute("""
    SELECT COUNT(*) as n, COUNT(DISTINCT Symbol) as nsym
    FROM v_backtest_scores
    WHERE month_date = '1990-09-30'
      AND jcn_full_composite IS NOT NULL
""").fetchone()
print(f"\nUniverse at 1990-09-30: {r[0]} records, {r[1]} symbols")
r2 = con.execute("""
    SELECT COUNT(*) as n, COUNT(DISTINCT Symbol) as nsym
    FROM v_backtest_scores
    WHERE month_date = '2010-03-31'
      AND jcn_full_composite IS NOT NULL
""").fetchone()
print(f"Universe at 2010-03-31: {r2[0]} records, {r2[1]} symbols")
con.close()
