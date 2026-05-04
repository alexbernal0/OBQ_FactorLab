import sys; sys.path.insert(0,'.')
from engine.portfolio_bank import get_portfolio_model
m = get_portfolio_model('PM-JCNFUL-20260501-BE81')
hl = m.get('holdings_log_json', [])
print(f"Holdings log entries: {len(hl)}")
if hl:
    # Sample first entry
    h0 = hl[0]
    print(f"First period date: {h0['date']}")
    print(f"First period holdings ({len(h0['holdings'])} stocks):")
    for h in h0['holdings'][:5]:
        print(f"  {h}")
    # Check if market_cap is in holdings
    print(f"\nKeys in each holding: {list(h0['holdings'][0].keys()) if h0['holdings'] else 'none'}")
