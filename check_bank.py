import sys, json
sys.path.insert(0, '.')
from engine.strategy_bank import get_model, get_all_models

models = get_all_models()
print("All models in bank:")
for m in models:
    print(f"  {m['strategy_id']} | {m['start_date']} -> {m['end_date']} | ICIR={m.get('icir',0):.3f}")

# Check the newest one
sid = models[0]['strategy_id']
full = get_model(sid)

tort = full.get('tortoriello_json', {})
um = full.get('universe_metrics_json', {})
pd_rows = full.get('period_data_json', [])
dates = full.get('dates_json', [])
sa = full.get('sector_attribution_json', [])
bm = full.get('bucket_metrics_json', {})
be = full.get('bucket_equity_json', {})

print(f"\n=== {sid} ===")
print(f"Dates: {len(dates)} | first={dates[0] if dates else None} last={dates[-1] if dates else None}")
print(f"Tortoriello buckets: {list(tort.keys())}")
print(f"Universe metrics keys: {list(um.keys())[:8]}")
print(f"Period data rows: {len(pd_rows)}")
print(f"Sector attribution rows: {len(sa)}")
print(f"Bucket equity keys: {list(be.keys())}")
print(f"Bucket metrics keys: {list(bm.keys())}")

if '1' in tort:
    t1 = tort['1']
    print(f"\nTortoriello Q1 sample:")
    print(f"  terminal_wealth: {t1.get('terminal_wealth')}")
    print(f"  avg_excess_vs_univ: {t1.get('avg_excess_vs_univ')}")
    print(f"  pct_1y_beats_univ: {t1.get('pct_1y_beats_univ')}")
    print(f"  pct_3y_beats_univ: {t1.get('pct_3y_beats_univ')}")
    print(f"  max_gain: {t1.get('max_gain')}")
    print(f"  max_loss: {t1.get('max_loss')}")
    print(f"  std_dev_ann: {t1.get('std_dev_ann')}")
    print(f"  beta_vs_univ: {t1.get('beta_vs_univ')}")
    print(f"  alpha_vs_univ: {t1.get('alpha_vs_univ')}")
    print(f"  avg_portfolio_size: {t1.get('avg_portfolio_size')}")
    print(f"  avg_market_cap: {t1.get('avg_market_cap')}")
    print(f"  roll_3y_excess len: {len(t1.get('roll_3y_excess',[]))}")
