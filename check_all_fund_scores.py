import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models
models = get_all_models(100)
blank = [m for m in models if m.get('obq_fund_score') is None]
ok    = [m for m in models if m.get('obq_fund_score') is not None]
print(f"Total: {len(models)} | OK: {len(ok)} | BLANK fund score: {len(blank)}")
print()
if blank:
    print("BLANK models:")
    for m in blank:
        print(f"  {m['strategy_id']:<30} | {(m.get('run_label') or '')[:60]}")
