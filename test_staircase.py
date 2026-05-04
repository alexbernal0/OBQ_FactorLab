"""Test the new staircase formula on example patterns."""
import numpy as np

def staircase(cagrs):
    n = len(cagrs)
    steps = [cagrs[i] - cagrs[i+1] for i in range(n-1)]
    total_spread = cagrs[0] - cagrs[-1]
    n_steps = len(steps)
    mono = sum(1 for s in steps if s > 0) / n_steps if n_steps > 0 else 0
    if n_steps > 1 and any(abs(s) > 0 for s in steps):
        step_std  = np.std(steps)
        step_mean = np.mean([abs(s) for s in steps])
        unif = float(np.clip(1.0 - (step_std / max(step_mean, 1e-6)), 0.0, 1.0))
    else:
        unif = 1.0
    score = total_spread * mono * unif
    print(f"  CAGRs: {[f'{c*100:.1f}%' for c in cagrs]}")
    print(f"  Steps: {[f'{s*100:.2f}%' for s in steps]}")
    print(f"  Spread={total_spread*100:.2f}%  Mono={mono*100:.0f}%  Unif={unif:.2f}")
    print(f"  STAIRCASE = {score*100:.3f}%")
    return score

print("=== TEST CASES ===\n")

print("1. PERFECT staircase (Q1>Q2>Q3>Q4>Q5, even steps, +2% each):")
staircase([0.14, 0.12, 0.10, 0.08, 0.06])

print("\n2. TOP CHART pattern (Q4 is best — worst case):")
# Q1=1%, Q2=-0.5%, Q3=0.2%, Q4=7.5% (best!), Q5=-1%
staircase([0.01, -0.005, 0.002, 0.075, -0.01])

print("\n3. BOTTOM CHART pattern (Q1>Q2>Q3, then Q4/Q5 go negative):")
# Q1=1.1%, Q2=0.76%, Q3=-0.02%, Q4=-0.69%, Q5=-1.02%
staircase([0.011, 0.0076, -0.0002, -0.0069, -0.0102])

print("\n4. Wide spread but all alpha in Q1→Q2 jump (lumpy):")
staircase([0.15, 0.03, 0.02, 0.01, 0.00])

print("\n5. Narrow spread but perfectly even steps:")
staircase([0.08, 0.06, 0.04, 0.02, 0.00])

print("\n6. One inversion only (Q3 slightly above Q2):")
staircase([0.14, 0.10, 0.11, 0.06, 0.02])

print("\n7. Fundsmith-like: wide spread, mostly monotonic:")
staircase([0.18, 0.12, 0.08, 0.04, 0.01])
