import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models, get_model
from collections import Counter

all_f = get_all_models(200)

# Duplicate configs
configs = Counter([(m.get('score_column'), m.get('start_date'), m.get('rebalance_freq'), m.get('n_buckets')) for m in all_f])
dupes = {k:v for k,v in configs.items() if v > 1}
print("=== DUPLICATE CONFIGS ===")
for cfg, count in sorted(dupes.items(), key=lambda x: -x[1]):
    models = [m for m in all_f if m.get('score_column')==cfg[0] and m.get('start_date')==cfg[1] and m.get('rebalance_freq')==cfg[2]]
    ids = [m['strategy_id'] for m in models]
    print(f"  {count}x | {cfg[0]} | {cfg[1]} | {cfg[2]} | {ids}")

# Negative ICIR
print("\n=== NEGATIVE ICIR MODELS (7) ===")
neg = [m for m in all_f if (m.get('icir') or 0) < 0]
for m in neg:
    sid = m['strategy_id']
    score = m.get('score_column','')
    icir  = m.get('icir',0)
    start = m.get('start_date','')
    rebal = m.get('rebalance_freq','')
    print(f"  {sid} | score={score} | ICIR={icir:.3f} | start={start} | rebal={rebal}")

print("\n=== PLANNED BASELINES vs WHAT WE HAVE ===")
planned_jcn = ['jcn_full_composite','jcn_qarp','jcn_garp','jcn_quality_momentum',
               'jcn_value_momentum','jcn_growth_quality_momentum','jcn_fortress','jcn_alpha_trifecta']
planned_ind = ['value_score','quality_score','growth_score','finstr_score',
               'momentum_score','momentum_af_score','momentum_fip_score','momentum_sys_score']
planned_univ = ['value_score_universe','quality_score_universe','growth_score_universe',
                'finstr_score_universe','af_universe_score']
planned_sep  = ['longeq_rank','rulebreaker_rank','fundsmith_rank','moat_score','moat_rank']

all_scores = set(m.get('score_column') for m in all_f)

all_planned = planned_jcn + planned_ind + planned_univ + planned_sep
print(f"Total planned: {len(all_planned)}")
for score in all_planned:
    models = [m for m in all_f if m.get('score_column')==score]
    n = len(models)
    best_icir = max((m.get('icir') or -99) for m in models) if models else None
    status = 'OK' if n > 0 else 'MISSING'
    print(f"  {'OK' if n>0 else 'MISSING':<8} {score:<40} n={n} best_ICIR={best_icir:.3f if best_icir else 'N/A'}")

missing = [s for s in all_planned if s not in all_scores]
print(f"\nMissing: {missing}")
