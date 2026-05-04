import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models
models = get_all_models(100)
cyc = [m for m in models if 'CYC-001' in (m.get('run_label') or '')]
print(f"CYC-001 factor models: {len(cyc)}")
print()
print(f"{'SID':<32} {'FUND':>8} {'STAIR%':>8} {'ALPHA_WIN%':>11} {'POPULATED'}")
print("-"*70)
blank_fund = 0
for m in sorted(cyc, key=lambda x: x.get('icir') or 0, reverse=True):
    fund  = m.get('obq_fund_score')
    stair = m.get('staircase_score')
    aw    = m.get('alpha_win_rate')
    fund_s  = f"{fund:.4f}" if fund is not None else "BLANK"
    stair_s = f"{stair*100:.2f}%" if stair is not None else "BLANK"
    aw_s    = f"{aw*100:.1f}%" if aw is not None else "BLANK"
    ok = "OK" if fund is not None else "MISSING"
    if fund is None: blank_fund += 1
    print(f"  {m['strategy_id']:<30} {fund_s:>8} {stair_s:>8} {aw_s:>11}  {ok}")
print()
print(f"Missing obq_fund_score: {blank_fund} / {len(cyc)}")
