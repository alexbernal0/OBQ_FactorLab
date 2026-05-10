import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models
models = get_all_models()
for m in models:
    print(f"\n{m['strategy_id']} | {m['start_date']}")
    print(f"  obq_fund_score  = {m.get('obq_fund_score')}")
    print(f"  alpha_win_rate  = {m.get('alpha_win_rate')}")
    print(f"  staircase_score = {m.get('staircase_score')}")
    print(f"  bear_score      = {m.get('bear_score')}")
    print(f"  bull_score      = {m.get('bull_score')}")
    print(f"  q1_calmar       = {m.get('q1_calmar')}")
    print(f"  q1_surefire     = {m.get('q1_surefire')}")
    print(f"  downside_capture= {m.get('downside_capture')}")
    print(f"  alpha_sharpe    = {m.get('alpha_sharpe')}")
