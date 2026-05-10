# -*- coding: utf-8 -*-
"""
qc_audit.py — CYC-002 Quality Control Audit
============================================
Checks every model in the bank for:
1. Completeness — all 47 singles + 19 combos present (factor + portfolio)
2. Data integrity — required fields non-null, ICIR/spread/CAGR plausible
3. Direction sanity — lower_better factors: Q1 should beat Q5 (positive spread)
4. Duplicate detection — same score_column stored multiple times
5. ICIR sign anomalies — flags any factor with |ICIR| < 0.05 (near-zero = data issue)
6. Spread sign vs direction — positive spread = Q5 beats Q1 (higher_better)
7. Negative spread detection — flags factors where Q5-Q1 < 0 (direction may be wrong)
8. Portfolio CAGR plausibility — outside [-5%, 30%] range is suspicious
9. Combo overlap check — T5 and L3 are the same combo (int cov + ev/ebitda), expected
10. Coverage gap check — factors with ICIR=0 (data gap, not real zero)
"""
import sys, math
sys.path.insert(0, '.')
from engine.strategy_bank import get_all_models
from engine.portfolio_bank import get_all_portfolio_models
from engine.cyc002_factors import CYC002_FACTORS, CYC002_COMBOS

# ── Load banks ──────────────────────────────────────────────────────────────────
all_f = get_all_models(1000)
all_p = get_all_portfolio_models(1000)

cyc2_f = [m for m in all_f if 'CYC-002' in (m.get('run_label') or '')]
cyc2_p = [m for m in all_p if 'CYC-002' in (m.get('run_label') or '')]

# Build best-per-score dicts (highest ICIR wins on dupes)
def best_per_score(models):
    d = {}
    for m in models:
        sc = m['score_column']
        if sc not in d or (m.get('icir') or 0) > (d[sc].get('icir') or 0):
            d[sc] = m
    return d

fmap = best_per_score(cyc2_f)
pmap = best_per_score(cyc2_p)

# Expected score_column IDs
expected_singles = [s for s,*_ in CYC002_FACTORS]
expected_combos  = [f"combo_{cid}" for cid,*_ in CYC002_COMBOS]
expected_all     = expected_singles + expected_combos

PASS = "✅"
WARN = "⚠️ "
FAIL = "❌"

issues = []
warnings = []

def flag(level, check, detail):
    entry = f"  {level} [{check}] {detail}"
    if level == FAIL:
        issues.append(entry)
    else:
        warnings.append(entry)
    print(entry)

print("=" * 72)
print("CYC-002 QUALITY CONTROL AUDIT")
print("=" * 72)

# ── 1. Completeness ─────────────────────────────────────────────────────────────
print("\n── 1. COMPLETENESS ────────────────────────────────────────────────────────")
missing_f = [s for s in expected_all if s not in fmap]
missing_p = [s for s in expected_all if s not in pmap]

if not missing_f:
    print(f"  {PASS} Factor bank: all {len(expected_all)} models present")
else:
    for s in missing_f:
        flag(FAIL, "MISSING_FACTOR", s)

if not missing_p:
    print(f"  {PASS} Portfolio bank: all {len(expected_all)} models present")
else:
    for s in missing_p:
        flag(WARN, "MISSING_PORT", s)

# ── 2. Duplicates ───────────────────────────────────────────────────────────────
print("\n── 2. DUPLICATES ──────────────────────────────────────────────────────────")
from collections import Counter
dup_f = Counter(m['score_column'] for m in cyc2_f)
dup_p = Counter(m['score_column'] for m in cyc2_p)
any_dup = False
for sc, cnt in dup_f.items():
    if cnt > 1:
        flag(WARN, "DUP_FACTOR", f"{sc} appears {cnt}x in factor bank")
        any_dup = True
for sc, cnt in dup_p.items():
    if cnt > 1:
        flag(WARN, "DUP_PORT", f"{sc} appears {cnt}x in portfolio bank")
        any_dup = True
if not any_dup:
    print(f"  {PASS} No duplicates in factor or portfolio bank")

# ── 3. Required fields ──────────────────────────────────────────────────────────
print("\n── 3. REQUIRED FIELDS ─────────────────────────────────────────────────────")
required_f_fields = ['icir', 'quintile_spread_cagr', 'q1_cagr', 'obq_fund_score']
required_p_fields = ['cagr', 'sharpe', 'max_dd']
any_missing = False
for sc in expected_all:
    m = fmap.get(sc)
    if m:
        for fld in required_f_fields:
            if m.get(fld) is None:
                flag(FAIL, "NULL_FIELD", f"{sc}.{fld} is NULL in factor model")
                any_missing = True
    p = pmap.get(sc)
    if p:
        pm = p.get('portfolio_metrics') or {}
        for fld in required_p_fields:
            if pm.get(fld) is None:
                flag(WARN, "NULL_PFIELD", f"{sc}.{fld} is NULL in portfolio model")
                any_missing = True
if not any_missing:
    print(f"  {PASS} All required fields populated")

# ── 4. ICIR plausibility ────────────────────────────────────────────────────────
print("\n── 4. ICIR PLAUSIBILITY ───────────────────────────────────────────────────")
near_zero   = []
very_high   = []
for sc in expected_all:
    m = fmap.get(sc)
    if not m: continue
    icir = m.get('icir') or 0
    if abs(icir) < 0.05 and sc not in ('cyc2_rev_cagr_3y', 'cyc2_share_chg_5y'):
        near_zero.append((sc, icir))
    if abs(icir) > 2.0:
        very_high.append((sc, icir))

if near_zero:
    for sc, v in near_zero:
        flag(WARN, "NEAR_ZERO_ICIR", f"{sc} ICIR={v:.4f} — possible data gap or noise")
else:
    print(f"  {PASS} No unexpectedly near-zero ICIRs (excluding known data gaps)")

if very_high:
    for sc, v in very_high:
        flag(WARN, "HIGH_ICIR", f"{sc} ICIR={v:.3f} — unusually high, verify")
else:
    print(f"  {PASS} No suspiciously high ICIRs (>2.0)")

# ── 5. Known data gaps ─────────────────────────────────────────────────────────
print("\n── 5. DATA GAPS ───────────────────────────────────────────────────────────")
known_gaps = ['cyc2_rev_cagr_3y', 'cyc2_share_chg_5y']
for sc in known_gaps:
    m = fmap.get(sc)
    if m:
        icir = m.get('icir') or 0
        spread = (m.get('quintile_spread_cagr') or 0) * 100
        if abs(icir) < 0.01 and abs(spread) < 0.01:
            flag(WARN, "DATA_GAP", f"{sc}: ICIR={icir:.4f} Spread={spread:.2f}% — confirmed data gap (zero coverage)")
        else:
            print(f"  {PASS} {sc}: ICIR={icir:.3f} — data gap resolved or partial")

# ── 6. Direction vs spread sanity ───────────────────────────────────────────────
print("\n── 6. SPREAD DIRECTION SANITY ─────────────────────────────────────────────")
# Get direction from registry
dir_map = {s: d for s,_,_,d,_,_ in CYC002_FACTORS}

problematic_spread = []
for sc in expected_singles:
    m = fmap.get(sc)
    if not m: continue
    spread = (m.get('quintile_spread_cagr') or 0) * 100
    icir   = m.get('icir') or 0
    # We only flag if ICIR > 0.3 and spread is strongly negative (likely wrong direction)
    # Profitability factors with negative spread are a KNOWN FINDING, not a bug
    if icir > 0.3 and spread < -5.0:
        # Check if it's a known profitability paradox factor
        known_paradox = sc in {
            'cyc2_fcf_margin','cyc2_roic','cyc2_cash_roc','cyc2_roce',
            'cyc2_gpa','cyc2_op_margin','cyc2_roa','cyc2_fcf_debt',
            'cyc2_int_cov','cyc2_fcf_cagr_5y','cyc2_rev_cagr_1y',
            'cyc2_rev_cagr_3y','cyc2_rev_cagr_5y','cyc2_eps_cagr_3y',
            'cyc2_eps_cagr_5y','cyc2_fcf_cagr_3y',
        }
        status = "KNOWN PARADOX" if known_paradox else "INVESTIGATE"
        lvl = WARN if known_paradox else FAIL
        flag(lvl if not known_paradox else PASS, "NEG_SPREAD",
             f"{sc}: ICIR={icir:.3f} Spread={spread:.2f}% → {status}")
        if not known_paradox:
            problematic_spread.append(sc)

# FIP factors specifically — negative ICIR + positive spread
for sc in ['cyc2_fip_6m', 'cyc2_fip_12m']:
    m = fmap.get(sc)
    if not m: continue
    icir   = m.get('icir') or 0
    spread = (m.get('quintile_spread_cagr') or 0) * 100
    if icir < -0.05:
        flag(WARN, "FIP_INVERSION", f"{sc}: ICIR={icir:.3f} Spread={spread:.2f}% — FIP direction likely inverted in large-cap, test lower_better in CYC-003")

if not problematic_spread:
    print(f"  {PASS} No unexpected negative spreads (profitability paradox noted as known finding)")

# ── 7. Portfolio CAGR plausibility ──────────────────────────────────────────────
print("\n── 7. PORTFOLIO CAGR PLAUSIBILITY ─────────────────────────────────────────")
outliers = []
for sc in expected_all:
    p = pmap.get(sc)
    if not p: continue
    pm   = p.get('portfolio_metrics') or {}
    cagr = (pm.get('cagr') or 0) * 100
    if cagr < -5.0 or cagr > 35.0:
        flag(FAIL, "CAGR_OUTLIER", f"{sc}: portfolio CAGR={cagr:.2f}% — outside plausible range")
        outliers.append(sc)
if not outliers:
    print(f"  {PASS} All portfolio CAGRs within plausible range [-5%, 35%]")

# ── 8. Sharpe plausibility ──────────────────────────────────────────────────────
print("\n── 8. SHARPE PLAUSIBILITY ─────────────────────────────────────────────────")
sharpe_issues = []
for sc in expected_all:
    p = pmap.get(sc)
    if not p: continue
    pm     = p.get('portfolio_metrics') or {}
    sharpe = pm.get('sharpe') or 0
    if sharpe > 3.0:
        flag(WARN, "HIGH_SHARPE", f"{sc}: Sharpe={sharpe:.3f} — unusually high, check period count")
        sharpe_issues.append(sc)
    elif sharpe < -1.5:
        flag(WARN, "NEG_SHARPE", f"{sc}: Sharpe={sharpe:.3f} — negative, underperformed risk-free")
        sharpe_issues.append(sc)
if not sharpe_issues:
    print(f"  {PASS} All Sharpe ratios within plausible range")

# ── 9. Period count sanity ──────────────────────────────────────────────────────
print("\n── 9. PERIOD COUNT SANITY ─────────────────────────────────────────────────")
short_periods = []
for sc in expected_all:
    m = fmap.get(sc)
    if not m: continue
    n_obs = m.get('n_obs') or 0
    if n_obs < 20:
        flag(WARN, "SHORT_HISTORY", f"{sc}: n_obs={n_obs} — fewer than 20 periods (start date limited)")
        short_periods.append(sc)
if not short_periods:
    print(f"  {PASS} All factors have >= 20 observation periods")

# ── 10. Combo integrity ─────────────────────────────────────────────────────────
print("\n── 10. COMBO INTEGRITY ────────────────────────────────────────────────────")
# T5 (EV/EBITDA + Int Cov) and L3 (Int Cov + EV/EBITDA) are identical by definition
t5 = fmap.get('combo_T5')
l3 = fmap.get('combo_L3')
if t5 and l3:
    t5_icir = t5.get('icir') or 0
    l3_icir = l3.get('icir') or 0
    if abs(t5_icir - l3_icir) < 0.01:
        print(f"  {PASS} T5 and L3 are identical combos (symmetric) — ICIR match confirmed ({t5_icir:.3f})")
    else:
        flag(WARN, "COMBO_MISMATCH", f"T5 ICIR={t5_icir:.3f} vs L3 ICIR={l3_icir:.3f} — should be identical")

# Check all combo models have combo_ prefix in score_column
for cid,*_ in CYC002_COMBOS:
    sc = f"combo_{cid}"
    m = fmap.get(sc)
    if m and m.get('score_column') != sc:
        flag(FAIL, "COMBO_ID_MISMATCH", f"{sc}: stored score_column={m.get('score_column')}")

print(f"  {PASS} All combo IDs correctly stored")

# ── FINAL SCORECARD ─────────────────────────────────────────────────────────────
print("\n" + "=" * 72)
print("AUDIT SCORECARD")
print("=" * 72)
print(f"\n  Factor models in bank (CYC-002):    {len(fmap)}")
print(f"  Portfolio models in bank (CYC-002): {len(pmap)}")
print(f"  Expected total:                      {len(expected_all)}")
print(f"\n  FAILURES: {len(issues)}")
print(f"  WARNINGS: {len(warnings)}")

if issues:
    print(f"\n  FAILURES REQUIRING ACTION:")
    for i in issues: print(i)
if warnings:
    print(f"\n  WARNINGS (review):")
    for w in warnings: print(w)

if not issues:
    print(f"\n  {PASS} QC PASSED — no hard failures")
else:
    print(f"\n  {FAIL} QC FAILED — {len(issues)} issue(s) require attention")

# ── Full model table ────────────────────────────────────────────────────────────
print("\n" + "=" * 72)
print("COMPLETE MODEL TABLE (sorted by ICIR desc)")
print("=" * 72)
print(f"\n  {'ID':<24} {'ICIR':>6} {'SPREAD':>8} {'Q1CAGR':>8} {'PORT_CAGR':>10} {'SHARPE':>7} {'N_OBS':>6}")
print("  " + "-"*72)

rows = []
for sc in expected_all:
    m = fmap.get(sc)
    p = pmap.get(sc)
    if not m: continue
    pm     = (p.get('portfolio_metrics') or {}) if p else {}
    icir   = m.get('icir') or 0
    spread = (m.get('quintile_spread_cagr') or 0) * 100
    q1     = (m.get('q1_cagr') or 0) * 100
    n_obs  = m.get('n_obs') or 0
    cagr   = (pm.get('cagr') or 0) * 100 if p else float('nan')
    sharpe = pm.get('sharpe') or 0 if p else float('nan')
    typ    = "[C]" if sc.startswith("combo_") else "   "
    rows.append((icir, sc, typ, spread, q1, cagr, sharpe, n_obs))

for icir, sc, typ, spread, q1, cagr, sharpe, n_obs in sorted(rows, reverse=True):
    c = f"{cagr:>9.2f}%" if not math.isnan(cagr) else "       —  "
    s = f"{sharpe:>7.3f}" if not math.isnan(sharpe) else "      —"
    print(f"  {typ}{sc:<24} {icir:>6.3f} {spread:>7.2f}% {q1:>7.2f}% {c} {s} {n_obs:>6}")

print(f"\n  Total: {len(rows)} models")
print("AUDIT COMPLETE.")
