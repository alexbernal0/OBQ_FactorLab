import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models, get_model

all_f = get_all_models(200)
batch3_scores = ['longeq_rank','rulebreaker_rank','fundsmith_rank','moat_score','moat_rank']

print("BATCH 3 DUPLICATE INSPECTION")
print("="*80)
for score in batch3_scores:
    models = [m for m in all_f if m.get('score_column')==score]
    print(f"\n{score} ({len(models)} models):")
    for m in sorted(models, key=lambda x: x.get('created_at',''), reverse=True):
        # Get full config
        full = get_model(m['strategy_id'])
        cfg = (full.get('config_json') or {}) if full else {}
        direction = cfg.get('score_direction','unknown')
        rebal = m.get('rebalance_freq') or cfg.get('rebalance_freq','?')
        hold  = m.get('hold_months') or cfg.get('hold_months','?')
        print(f"  {m['strategy_id']} | ICIR={m.get('icir',0):.3f} | dir={direction} | rebal={rebal} | hold={hold}mo | created={str(m.get('created_at',''))[:19]}")
