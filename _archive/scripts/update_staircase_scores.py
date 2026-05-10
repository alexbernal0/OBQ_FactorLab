# -*- coding: utf-8 -*-
"""
Recalculate staircase_score for all bank models using new formula.
Much faster than full reseed — just reads bucket_metrics and updates scalar.
"""
import sys, math, numpy as np
sys.path.insert(0, '.')
import duckdb
from engine.strategy_bank import get_all_models, get_model, BANK_FILE

def new_staircase(bucket_metrics, n):
    cagrs = [bucket_metrics.get(str(b), {}).get("cagr", 0.0) or 0.0 for b in range(1, n+1)]
    if not cagrs or len(cagrs) < 2:
        return 0.0
    steps = [cagrs[i] - cagrs[i+1] for i in range(len(cagrs)-1)]
    total_spread = cagrs[0] - cagrs[-1]
    n_steps = len(steps)
    mono = sum(1 for s in steps if s > 0) / n_steps if n_steps > 0 else 0.0
    if n_steps > 1 and any(abs(s) > 0 for s in steps):
        cv = np.std(steps) / max(np.mean([abs(s) for s in steps]), 1e-6)
        unif = 1.0 / (1.0 + cv * 0.5)
    else:
        unif = 1.0
    q1_is_best = cagrs[0] >= max(cagrs)
    inv_pen = 1.0 if q1_is_best else max(0.1, cagrs[0] / max(cagrs) if max(cagrs) > 0 else 0.1)
    score = float(total_spread * mono * unif * inv_pen)
    return None if (math.isnan(score) or math.isinf(score)) else round(score, 6)

models = get_all_models(200)
print(f"Updating staircase_score for {len(models)} models...")

con = duckdb.connect(str(BANK_FILE))
updated = 0
for m in models:
    sid = m['strategy_id']
    n   = m.get('n_buckets') or 5
    # Load bucket_metrics from JSON
    full = get_model(sid)
    if not full:
        continue
    bm = full.get('bucket_metrics_json', {}) or {}
    if not isinstance(bm, dict) or not bm:
        continue
    score = new_staircase(bm, n)
    if score is None:
        continue
    con.execute("UPDATE factor_models SET staircase_score = ? WHERE strategy_id = ?", [score, sid])
    updated += 1
    old = m.get('staircase_score') or 0
    print(f"  {sid:<30} old={old*100:.2f}%  new={score*100:.2f}%")

con.commit()
con.close()
print(f"\nUpdated {updated} models.")
