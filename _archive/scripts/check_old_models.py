import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models
models = get_all_models(100)
old = [m for m in models if '20260430' in m.get('strategy_id','')]
print(f"April 30 models: {len(old)}")
for m in old:
    print(f"  {m['strategy_id']} | fund={m.get('obq_fund_score')} | stair={m.get('staircase_score')} | icir={m.get('icir')}")
