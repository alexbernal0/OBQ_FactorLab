# -*- coding: utf-8 -*-
"""
Encyclopedia v2 Mass Generator.

Generates all 91 factor + 19 combo chapters from the CYC-003-GPU strategy bank,
organized into Part folders matching v1 structure for continuity.

Output: D:/OBQ_AI/OBQ_Encyclopedia_v2/
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from factor_data import load_factor_bundle, list_available_factors, TIER_ORDER  # noqa
from chapter_renderer import render_chapter, DISPLAY_NAMES  # noqa

OUT_ROOT = Path(r'D:/OBQ_AI/OBQ_Encyclopedia_v2')

# ── Part assignment — maps factor score_column → (Part_folder, sort_order) ───
# Mirrors v1 organization where possible; extends with cap-tier doctrine.
PART_MAP = {
    # Part II — Valuation
    'cyc2_ps':                ('Part_II_Valuation', 1, 'Price-to-Sales'),
    'cyc2_pe':                ('Part_II_Valuation', 2, 'Price-to-Earnings'),
    'cyc2_pb':                ('Part_II_Valuation', 3, 'Price-to-Book'),
    'cyc2_ev_ebitda':         ('Part_II_Valuation', 4, 'EV/EBITDA'),
    'cyc2_pfcf':              ('Part_II_Valuation', 5, 'Price-to-FCF'),
    'cyc2_fcf_yield':         ('Part_II_Valuation', 6, 'FCF Yield'),
    'value_score':            ('Part_II_Valuation', 7, 'OBQ Value Composite'),
    'value_score_universe':   ('Part_II_Valuation', 8, 'OBQ Value (Universe rank)'),

    # Part III — Profitability / Quality
    'cyc2_roic':              ('Part_III_Profitability', 1, 'ROIC'),
    'cyc2_roa':               ('Part_III_Profitability', 2, 'ROA'),
    'cyc2_roce':              ('Part_III_Profitability', 3, 'ROCE (Fundsmith)'),
    'cyc2_gpa':               ('Part_III_Profitability', 4, 'GPA (Novy-Marx)'),
    'cyc2_op_margin':         ('Part_III_Profitability', 5, 'Operating Margin'),
    'cyc2_fcf_margin':        ('Part_III_Profitability', 6, 'FCF Margin'),
    'cyc2_gross_margin':      ('Part_III_Profitability', 7, 'Gross Margin'),
    'cyc2_earn_quality':      ('Part_III_Profitability', 8, 'Earnings Quality'),
    'cyc2_cash_roc':          ('Part_III_Profitability', 9, 'Cash ROIC'),
    'cyc2_cash_conv':         ('Part_III_Profitability', 10, 'Cash Conversion'),
    'quality_score':          ('Part_III_Profitability', 11, 'OBQ Quality Composite'),
    'quality_score_universe': ('Part_III_Profitability', 12, 'OBQ Quality (Universe rank)'),

    # Part IV — Financial Strength
    'cyc2_int_cov':           ('Part_IV_Financial_Strength', 1, 'Interest Coverage'),
    'cyc2_fcf_debt':          ('Part_IV_Financial_Strength', 2, 'FCF / Debt'),
    'cyc2_nd_ebitda':         ('Part_IV_Financial_Strength', 3, 'Net Debt / EBITDA'),
    'cyc2_nd_ebit':           ('Part_IV_Financial_Strength', 4, 'Net Debt / EBIT'),
    'cyc2_debt_assets':       ('Part_IV_Financial_Strength', 5, 'Debt / Assets'),
    'cyc2_cash_assets':       ('Part_IV_Financial_Strength', 6, 'Cash / Assets'),
    'cyc2_int_pct_op':        ('Part_IV_Financial_Strength', 7, 'Interest % of Op Income'),
    'cyc2_capex_ocf':         ('Part_IV_Financial_Strength', 8, 'CapEx % of OCF'),
    'cyc2_share_chg':         ('Part_IV_Financial_Strength', 9, 'Share Count Change 5yr'),
    'finstr_score':           ('Part_IV_Financial_Strength', 10, 'OBQ FinStr Composite'),
    'finstr_score_universe':  ('Part_IV_Financial_Strength', 11, 'OBQ FinStr (Universe rank)'),

    # Part V — Momentum
    'cyc2_mom_3m':            ('Part_V_Momentum', 1, '3-Month Momentum'),
    'cyc2_mom_6m':            ('Part_V_Momentum', 2, '6-Month Momentum'),
    'cyc2_mom_12m':           ('Part_V_Momentum', 3, '12-Month Momentum'),
    'cyc2_fip_6m':            ('Part_V_Momentum', 4, 'FIP 6-Month'),
    'cyc2_fip_12m':           ('Part_V_Momentum', 5, 'FIP 12-Month'),
    'momentum_score':         ('Part_V_Momentum', 6, 'OBQ Momentum Composite'),
    'momentum_sys_score':     ('Part_V_Momentum', 7, 'OBQ Momentum Systematic'),
    'momentum_af_score':      ('Part_V_Momentum', 8, 'OBQ Momentum AF'),
    'momentum_fip_score':     ('Part_V_Momentum', 9, 'OBQ Momentum FIP'),
    'cyc2_sys_score':         ('Part_V_Momentum', 10, 'Systematic Score'),
    'af_universe_score':      ('Part_V_Momentum', 11, 'AF Universe Score'),

    # Part VI — Growth
    'cyc2_rev_growth_3y':     ('Part_VI_Growth', 1, 'Revenue Growth 3yr'),
    'cyc2_rev_cagr_1y':       ('Part_VI_Growth', 2, 'Revenue/Shr CAGR 1yr'),
    'cyc2_rev_cagr_3y':       ('Part_VI_Growth', 3, 'Revenue/Shr CAGR 3yr'),
    'cyc2_rev_cagr_5y':       ('Part_VI_Growth', 4, 'Revenue/Shr CAGR 5yr'),
    'cyc2_eps_cagr_1y':       ('Part_VI_Growth', 5, 'EPS CAGR 1yr'),
    'cyc2_eps_cagr_3y':       ('Part_VI_Growth', 6, 'EPS CAGR 3yr'),
    'cyc2_eps_cagr_5y':       ('Part_VI_Growth', 7, 'EPS CAGR 5yr'),
    'cyc2_fcf_cagr_3y':       ('Part_VI_Growth', 8, 'FCF/Shr CAGR 3yr'),
    'cyc2_fcf_cagr_5y':       ('Part_VI_Growth', 9, 'FCF CAGR 5yr'),
    'growth_score':           ('Part_VI_Growth', 10, 'OBQ Growth Composite'),
    'growth_score_universe':  ('Part_VI_Growth', 11, 'OBQ Growth (Universe rank)'),

    # Part VII — Moat / R&D / Capital
    'cyc2_rd_ratio':          ('Part_VII_Moat_Capital', 1, 'R&D Ratio'),
    'cyc2_moat_intangible':   ('Part_VII_Moat_Capital', 2, 'Intangible Assets Moat'),
    'cyc2_moat_switching':    ('Part_VII_Moat_Capital', 3, 'Switching Cost Moat'),
    'cyc2_moat_network':      ('Part_VII_Moat_Capital', 4, 'Network Effect Moat'),
    'cyc2_moat_cost':         ('Part_VII_Moat_Capital', 5, 'Cost Advantage Moat'),
    'cyc2_moat_scale':        ('Part_VII_Moat_Capital', 6, 'Efficient Scale Moat'),
    'moat_score':             ('Part_VII_Moat_Capital', 7, 'Moat Composite'),
    'moat_rank':              ('Part_VII_Moat_Capital', 8, 'Moat Rank'),
    'fundsmith_rank':         ('Part_VII_Moat_Capital', 9, 'Fundsmith Rank'),
    'longeq_rank':            ('Part_VII_Moat_Capital', 10, 'LongEQ Rank'),
    'rulebreaker_rank':       ('Part_VII_Moat_Capital', 11, 'RuleBreaker Rank'),

    # Part IX — JCN / OBQ Composites (top-tier multi-factor)
    'jcn_full_composite':     ('Part_IX_OBQ_Composites', 1, 'JCN Full Composite'),
    'jcn_qarp':               ('Part_IX_OBQ_Composites', 2, 'JCN QARP'),
    'jcn_garp':               ('Part_IX_OBQ_Composites', 3, 'JCN GARP'),
    'jcn_quality_momentum':   ('Part_IX_OBQ_Composites', 4, 'JCN Quality-Momentum'),
    'jcn_value_momentum':     ('Part_IX_OBQ_Composites', 5, 'JCN Value-Momentum'),
    'jcn_growth_quality_momentum': ('Part_IX_OBQ_Composites', 6, 'JCN GQM'),
    'jcn_fortress':           ('Part_IX_OBQ_Composites', 7, 'JCN Fortress'),
    'jcn_alpha_trifecta':     ('Part_IX_OBQ_Composites', 8, 'JCN Alpha Trifecta'),
}

# Part VIII — Two-Factor Combos (combo_T1..T8, L1..L6, O1..O5)
COMBO_PART = 'Part_VIII_Two_Factor_Combos'

# Part XI — CYC-004 Pure Factor Baselines (Tier 1 — Efficiency & Quality)
# Part XII — CYC-004 Pure Factor Baselines (Tier 2 — Valuation & Risk)
# Part XIII — CYC-004 Pure Factor Baselines (Tier 3 — Change & Stability)
CYC004_PART_MAP = {
    # TIER 1 — 12 factors
    'cyc4_ocf_assets':            ('Part_XI_CYC004_Tier1', 1,  'OCF / Assets'),
    'cyc4_ebit_assets':           ('Part_XI_CYC004_Tier1', 2,  'EBIT / Assets'),
    'cyc4_asset_turnover':        ('Part_XI_CYC004_Tier1', 3,  'Asset Turnover'),
    'cyc4_roe':                   ('Part_XI_CYC004_Tier1', 4,  'Return on Equity'),
    'cyc4_accruals_ratio':        ('Part_XI_CYC004_Tier1', 5,  'Accruals Ratio'),
    'cyc4_fscore':                ('Part_XI_CYC004_Tier1', 6,  'Piotroski F-Score'),
    'cyc4_current_ratio':         ('Part_XI_CYC004_Tier1', 7,  'Current Ratio'),
    'cyc4_eps_stability':         ('Part_XI_CYC004_Tier1', 8,  'EPS Stability'),
    'cyc4_sales_stability':       ('Part_XI_CYC004_Tier1', 9,  'Sales Stability'),
    'cyc4_dividend_yield':        ('Part_XI_CYC004_Tier1', 10, 'Dividend Yield'),
    'cyc4_net_payout_yield':      ('Part_XI_CYC004_Tier1', 11, 'Net Payout Yield'),
    'cyc4_realized_vol':          ('Part_XI_CYC004_Tier1', 12, 'Realized Volatility'),
    # TIER 2 — 13 factors
    'cyc4_retained_earnings_ta':  ('Part_XII_CYC004_Tier2', 1,  'Retained Earnings / Assets'),
    'cyc4_ebit_ev':               ('Part_XII_CYC004_Tier2', 2,  'EBIT / Enterprise Value'),
    'cyc4_fcf_ev':                ('Part_XII_CYC004_Tier2', 3,  'FCF / Enterprise Value'),
    'cyc4_sales_ev':              ('Part_XII_CYC004_Tier2', 4,  'Sales / Enterprise Value'),
    'cyc4_pretax_margin':         ('Part_XII_CYC004_Tier2', 5,  'Pretax Margin'),
    'cyc4_pretax_margin_dev':     ('Part_XII_CYC004_Tier2', 6,  'Pretax Margin vs 5yr Avg'),
    'cyc4_shareholder_yield':     ('Part_XII_CYC004_Tier2', 7,  'Shareholder Yield'),
    'cyc4_repurchase_yield':      ('Part_XII_CYC004_Tier2', 8,  'Repurchase Yield'),
    'cyc4_op_leverage':           ('Part_XII_CYC004_Tier2', 9,  'Operating Leverage'),
    'cyc4_wc_assets':             ('Part_XII_CYC004_Tier2', 10, 'Working Capital / Assets'),
    'cyc4_tax_paid_sales':        ('Part_XII_CYC004_Tier2', 11, 'Tax Expense / Sales'),
    'cyc4_market_beta':           ('Part_XII_CYC004_Tier2', 12, 'Market Beta (60m Rolling)'),
    'cyc4_intangibles_pb':        ('Part_XII_CYC004_Tier2', 13, 'Intangibles-Adjusted P/B'),
    # TIER 3 — 12 factors
    'cyc4_skip_month_mom':        ('Part_XIII_CYC004_Tier3', 1,  'Skip-Month Momentum'),
    'cyc4_gross_margin':          ('Part_XIII_CYC004_Tier3', 2,  'Gross Profit Margin'),
    'cyc4_debt_equity':           ('Part_XIII_CYC004_Tier3', 3,  'Debt / Equity'),
    'cyc4_quick_ratio':           ('Part_XIII_CYC004_Tier3', 4,  'Quick Ratio'),
    'cyc4_cash_conv_cycle':       ('Part_XIII_CYC004_Tier3', 5,  'Cash Conversion Cycle'),
    'cyc4_change_ar_assets':      ('Part_XIII_CYC004_Tier3', 6,  'Change in AR / Assets'),
    'cyc4_change_inv_assets':     ('Part_XIII_CYC004_Tier3', 7,  'Change in Inventory / Assets'),
    'cyc4_roa_dev':               ('Part_XIII_CYC004_Tier3', 8,  'ROA vs 5yr Avg (Trend)'),
    'cyc4_roe_dev':               ('Part_XIII_CYC004_Tier3', 9,  'ROE vs 5yr Avg (Trend)'),
    'cyc4_idio_vol':              ('Part_XIII_CYC004_Tier3', 10, 'Idiosyncratic Volatility'),
    'cyc4_altman_z':              ('Part_XIII_CYC004_Tier3', 11, 'Altman Z-Score'),
    'cyc4_log_market_cap':        ('Part_XIII_CYC004_Tier3', 12, 'Log Market Cap (Size)'),
}
PART_MAP.update(CYC004_PART_MAP)

# Part XIV — CYC-005 Sector-Specific Novel Factors
# Each factor is the purpose-built keystone metric for its GICS sector
CYC005_PART_MAP = {
    'cyc5_energy_midcycle_fcf':    ('Part_XIV_CYC005_Sector', 1,  'Energy: Mid-Cycle FCF Yield'),
    'cyc5_materials_ccc':          ('Part_XIV_CYC005_Sector', 2,  'Materials: Cash Conversion Cycle'),
    'cyc5_industrials_efficiency': ('Part_XIV_CYC005_Sector', 3,  'Industrials: Efficiency Composite'),
    'cyc5_consumer_disc_brand':    ('Part_XIV_CYC005_Sector', 4,  'Consumer Disc: Brand-Growth Score'),
    'cyc5_staples_div_growth':     ('Part_XIV_CYC005_Sector', 5,  'Consumer Staples: Dividend Growth'),
    'cyc5_healthcare_rnd_yield':   ('Part_XIV_CYC005_Sector', 6,  'Health Care: R&D Yield'),
    'cyc5_financials_capital':     ('Part_XIV_CYC005_Sector', 7,  'Financials: Capital Adequacy'),
    'cyc5_it_rule_of_40':          ('Part_XIV_CYC005_Sector', 8,  'IT: Rule of 40'),
    'cyc5_comms_content_roi':      ('Part_XIV_CYC005_Sector', 9,  'Comm Services: Content ROI'),
    'cyc5_utilities_safe_yield':   ('Part_XIV_CYC005_Sector', 10, 'Utilities: Safe Dividend Yield'),
    'cyc5_realestate_ffo_yield':   ('Part_XIV_CYC005_Sector', 11, 'Real Estate: FFO Proxy Yield'),
}
PART_MAP.update(CYC005_PART_MAP)


def _slugify(name: str) -> str:
    """Filename-safe version of a chapter title."""
    s = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_').replace('/', '_')
    return s[:60]


def _resolve_chapter_meta(score_column: str) -> tuple:
    """Returns (part_folder, sort_idx, display_name)."""
    if score_column in PART_MAP:
        return PART_MAP[score_column]
    if score_column.startswith('combo_'):
        # combo_T4 -> sort 14, combo_L2 -> 22, combo_O3 -> 33
        cid = score_column.replace('combo_', '')
        prefix_order = {'T': 10, 'L': 20, 'O': 30}
        sort_idx = prefix_order.get(cid[0], 99) + (int(cid[1:]) if cid[1:].isdigit() else 0)
        return (COMBO_PART, sort_idx, score_column.replace('_', ' ').upper())
    # Fallback bucket
    return ('Part_X_Unsorted', 99, score_column)


def main():
    # Clean output dir
    if OUT_ROOT.exists():
        # Don't blow away — just clear the chapter contents, preserve any manual files
        print(f"Output dir exists: {OUT_ROOT}. Will overwrite chapter files.")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    factors = list_available_factors()
    print(f"\nLoading {len(factors)} factor bundles...")

    # Group by part for sequential numbering within each Part
    by_part: dict = {}
    bundles_meta = []
    for f in factors:
        sc = f['score_column']
        bundle = load_factor_bundle(sc)
        if bundle is None:
            print(f"  SKIP {sc} — no bundle")
            continue
        part, sort_idx, display = _resolve_chapter_meta(sc)
        by_part.setdefault(part, []).append((sort_idx, sc, display, bundle))
        bundles_meta.append((part, sort_idx, sc, display))

    print(f"\nGrouped into {len(by_part)} parts:")
    for part, items in sorted(by_part.items()):
        print(f"  {part}: {len(items)} chapters")

    # ── Render and write ─────────────────────────────────────────────────────
    t0 = time.time()
    written = 0
    errors = []
    chapter_global = 0

    for part in sorted(by_part.keys()):
        part_dir = OUT_ROOT / part
        part_dir.mkdir(exist_ok=True)
        items = sorted(by_part[part], key=lambda x: x[0])

        for local_idx, (sort_idx, sc, display, bundle) in enumerate(items, start=1):
            chapter_global += 1
            try:
                md = render_chapter(bundle, chapter_num=chapter_global)
                slug = _slugify(display)
                fname = f"Chapter_{chapter_global:02d}_{slug}.md"
                fpath = part_dir / fname
                fpath.write_text(md, encoding='utf-8')
                written += 1
            except Exception as e:
                errors.append((sc, str(e)[:120]))
                print(f"  ERROR {sc}: {e}")

    elapsed = time.time() - t0
    print(f"\nWrote {written} chapters in {elapsed:.1f}s ({written/max(elapsed,0.01):.0f} ch/s)")
    if errors:
        print(f"\n{len(errors)} errors:")
        for sc, e in errors:
            print(f"  {sc}: {e}")

    # ── Write index / TOC ────────────────────────────────────────────────────
    print("\nGenerating INDEX.md...")
    idx_lines = [
        "# OBQ Factor Encyclopedia — Volume II (R3000 Edition)",
        "",
        f"*Generated: {time.strftime('%Y-%m-%d %H:%M')}  |  "
        f"Source: CYC-003-GPU run on Russell 3000 (PIT, survivorship-bias free)  |  "
        f"Period: 1995-03-31 to 2024-12-31  |  Rebalance: semi-annual  |  Hold: 6mo*",
        "",
        "## Doctrine",
        "Per `ARCHITECTURE.md`: this encyclopedia treats Russell 3000 as the default universe. "
        "Cap-tier slicing (`all` / `large` ≥$10B / `mid` $2B-$10B) is presented on equal footing in every chapter — "
        "no factor is discussed at large-cap-only by default. Sector decomposition is mandatory; "
        "bear/bull regime testing is mandatory.",
        "",
        "Every chapter features the **OBQ Master Score** (25% ICIR + 25% Staircase + 20% AlphaWin + "
        "20% AlphaMag + 10% IC-Hit) and **Staircase Score** (Q1-Q5 spread × monotonicity × step-uniformity) "
        "as the two primary fitness signals.",
        "",
        "## Table of Contents",
        "",
    ]
    chapter_global = 0
    for part in sorted(by_part.keys()):
        items = sorted(by_part[part], key=lambda x: x[0])
        idx_lines.append(f"### {part.replace('_', ' ')}\n")
        for sort_idx, sc, display, bundle in items:
            chapter_global += 1
            slug = _slugify(display)
            base = bundle['tiers'].get('all') or list(bundle['tiers'].values())[0]
            obq = base.get('obq_fund_score')
            stair = base.get('staircase_score')
            obq_str = f"{obq:.3f}" if obq is not None else "—"
            stair_str = f"{stair:.4f}" if stair is not None else "—"
            link = f"{part}/Chapter_{chapter_global:02d}_{slug}.md"
            idx_lines.append(f"- **Ch. {chapter_global:2d}** [{display}]({link}) — "
                             f"OBQ `{obq_str}` · Staircase `{stair_str}`")
        idx_lines.append("")

    (OUT_ROOT / 'INDEX.md').write_text('\n'.join(idx_lines), encoding='utf-8')
    print(f"  Wrote {OUT_ROOT / 'INDEX.md'}")

    # ── Top performers leaderboard ────────────────────────────────────────────
    print("\nGenerating Leaderboard.md...")
    leaderboard = sorted(
        bundles_meta,
        key=lambda m: -(load_factor_bundle(m[2])['tiers'].get('all', {}).get('obq_fund_score') or -99)
    )[:25]
    lb_lines = [
        "# Top 25 Factors by OBQ Master Score (R3000 All-Cap)",
        "",
        f"*Source: CYC-003-GPU, ranked by OBQ Master Score on the `all` cap tier. "
        f"This is the encyclopedia v2 default lens.*",
        "",
        "| # | Factor | OBQ Master | Staircase | Q1-Q5 Spread | Best Tier | Cap-Sensitivity |",
        "|---|---|---|---|---|---|---|",
    ]
    for i, (part, sort_idx, sc, display) in enumerate(leaderboard, start=1):
        b = load_factor_bundle(sc)
        if not b:
            continue
        all_t = b['tiers'].get('all', {})
        obq_vals = {t: b['tiers'].get(t, {}).get('obq_fund_score')
                    for t in TIER_ORDER if t in b['tiers']}
        obq_vals_filt = {k: v for k, v in obq_vals.items() if v is not None}
        if not obq_vals_filt:
            continue
        best_t = max(obq_vals_filt, key=obq_vals_filt.get)
        obq_range = max(obq_vals_filt.values()) - min(obq_vals_filt.values())
        sens = "Stable" if obq_range < 0.05 else ("Moderate" if obq_range < 0.15 else "High")
        lb_lines.append(f"| {i} | {display} | "
                       f"**{all_t.get('obq_fund_score', 0):.3f}** | "
                       f"{all_t.get('staircase_score', 0):.4f} | "
                       f"{(all_t.get('quintile_spread_cagr') or 0)*100:+.1f}% | "
                       f"{best_t.upper()} ({obq_vals_filt[best_t]:.3f}) | "
                       f"{sens} ({obq_range:.2f}) |")

    (OUT_ROOT / 'Leaderboard.md').write_text('\n'.join(lb_lines), encoding='utf-8')
    print(f"  Wrote {OUT_ROOT / 'Leaderboard.md'}")

    print(f"\n{'='*60}")
    print(f"Encyclopedia v2 generation complete")
    print(f"  Chapters: {written}")
    print(f"  Errors:   {len(errors)}")
    print(f"  Output:   {OUT_ROOT}")
    print(f"  Time:     {elapsed:.1f}s")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
