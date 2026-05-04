import numpy as np

def staircase(cagrs):
    steps = [cagrs[i] - cagrs[i+1] for i in range(len(cagrs)-1)]
    total_spread = cagrs[0] - cagrs[-1]
    mono = sum(1 for s in steps if s > 0) / len(steps) if steps else 0
    if len(steps) > 1 and any(abs(s) > 0 for s in steps):
        cv = np.std(steps) / max(np.mean([abs(s) for s in steps]), 1e-6)
        unif = 1.0 / (1.0 + cv * 0.5)   # soft penalty, never zeros out
    else:
        unif = 1.0
    q1_is_best = cagrs[0] >= max(cagrs)
    inv_pen = 1.0 if q1_is_best else max(0.1, cagrs[0] / max(cagrs) if max(cagrs) > 0 else 0.1)
    score = total_spread * mono * unif * inv_pen
    return score

cases = [
    ("Perfect even staircase Q1>Q2>Q3>Q4>Q5",  [0.14, 0.12, 0.10, 0.08, 0.06]),
    ("Top chart: Q4 is best (worst pattern)",   [0.01, -0.005, 0.002, 0.075, -0.01]),
    ("Bottom chart: Q1>Q2>Q3 then drops",       [0.011, 0.0076, -0.0002, -0.0069, -0.0102]),
    ("Wide spread, lumpy (all in Q1->Q2)",       [0.15, 0.03, 0.02, 0.01, 0.00]),
    ("Narrow but perfectly even",               [0.08, 0.06, 0.04, 0.02, 0.00]),
    ("One inversion Q3 > Q2",                   [0.14, 0.10, 0.11, 0.06, 0.02]),
    ("Fundsmith: wide spread mostly mono",      [0.18, 0.12, 0.08, 0.04, 0.01]),
    ("Flat (no factor signal)",                 [0.08, 0.08, 0.08, 0.08, 0.08]),
    ("Reversed (Q5 best = anti-factor)",        [0.04, 0.06, 0.08, 0.10, 0.14]),
]

print(f"{'CASE':<44} {'STAIRCASE':>10}  {'VERDICT'}")
print("-"*65)
for name, cagrs in cases:
    s = staircase(cagrs)
    verdict = "EXCELLENT" if s > 0.06 else "GOOD" if s > 0.02 else "WEAK" if s > 0 else "BAD/ANTI"
    print(f"  {name:<42} {s*100:>9.3f}%  {verdict}")
