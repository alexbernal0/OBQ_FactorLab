import sys; sys.path.insert(0,'.')
from engine.portfolio_bank import get_portfolio_model
m = get_portfolio_model('PM-JCNFUL-20260501-BE81')
ann = m.get('annual_ret_json', [])
eq  = m.get('portfolio_equity_json', [])
dates = m.get('equity_dates_json', [])

print("Annual returns with spike:")
for a in ann:
    flag = " <-- SPIKE" if abs(a.get('portfolio_ret',0) or 0) > 2 else ""
    print(f"  {a['year']}: port={a.get('portfolio_ret',0)*100:.1f}%  spy={a.get('spy_ret',0)*100:.1f}%{flag}")

# Find the spike year — check surrounding equity values
print("\nEquity curve around year 2000:")
for i, (d, v) in enumerate(zip(dates, eq)):
    if '1999' in d or '2000' in d or '2001' in d:
        print(f"  [{i}] {d}: {v:.4f}")
