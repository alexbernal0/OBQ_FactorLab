import sys; sys.path.insert(0,'.')
from engine.portfolio_bank import get_portfolio_model
m = get_portfolio_model('PM-JCNFUL-20260501-BE81')
pd_rows = m.get('period_data_json', [])
print("Periods with >50% return:")
for p in pd_rows:
    r = p.get('portfolio_return', 0) or 0
    if abs(r) > 0.5:
        print(f"  {p['date']} -> {p['next_date']}  ret={r*100:.1f}%  top5={p.get('top5', [])}")
