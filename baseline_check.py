import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models
all_f = get_all_models(200)
all_scores = set(m.get('score_column') for m in all_f)

planned = [
    # JCN composites
    'jcn_full_composite','jcn_qarp','jcn_garp','jcn_quality_momentum',
    'jcn_value_momentum','jcn_growth_quality_momentum','jcn_fortress','jcn_alpha_trifecta',
    # Individual
    'value_score','quality_score','growth_score','finstr_score',
    'momentum_score','momentum_af_score','momentum_fip_score','momentum_sys_score',
    # Universe-normalized
    'value_score_universe','quality_score_universe','growth_score_universe',
    'finstr_score_universe','af_universe_score',
    # Separate tables (Batch 3)
    'longeq_rank','rulebreaker_rank','fundsmith_rank','moat_score','moat_rank',
]

print("BASELINE COVERAGE CHECK")
print("-"*70)
all_ok = True
for score in planned:
    models = [m for m in all_f if m.get('score_column')==score]
    n = len(models)
    if n == 0:
        print(f"  MISSING  {score}")
        all_ok = False
    else:
        icirs = [m.get('icir') or 0 for m in models]
        best = max(icirs)
        # Flag negative ICIR as suspicious
        flag = " <-- NEGATIVE ICIR (check direction)" if best < 0 else ""
        best_s = "{:.3f}".format(best)
        dupes = " (DUPES: {})".format(n) if n > 1 else ""
        print(f"  OK{dupes:<15} {score:<42} best_ICIR={best_s}{flag}")

print()
missing = [s for s in planned if s not in all_scores]
if missing:
    print("MISSING:", missing)
else:
    print("All 26 planned baselines present in factor bank.")

if all_ok:
    print("No missing baselines.")
else:
    print("WARNING: some baselines missing.")
