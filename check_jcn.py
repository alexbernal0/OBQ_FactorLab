import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models
all_f = get_all_models(200)
jcn = [m for m in all_f if m.get('score_column')=='jcn_full_composite']
for m in jcn:
    sid = m['strategy_id']
    icir = m.get('icir',0)
    start = m.get('start_date','')
    end = m.get('end_date','')
    rebal = m.get('rebalance_freq','')
    label = (m.get('run_label') or '')[:70]
    print(f"  {sid} | ICIR={icir:.3f} | {start} to {end} | {rebal} | {label}")
