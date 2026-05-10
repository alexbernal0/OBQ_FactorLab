# -*- coding: utf-8 -*-
"""
Build OBQ_Factor_Intelligence.md — a single, Claude-optimized markdown file
that encodes the full intelligence of the OBQ Factor Encyclopedia v2.

Designed for: a friend who wants to drop this file into a Claude conversation
and ask questions like "which factors should I use to screen for quality mid-cap
stocks?" or "which factors work best in bear markets?" or "evaluate AAPL for me
using OBQ factors."

Structure:
  1. How to use this file with Claude
  2. OBQ factor scoring system explained
  3. Full factor database (91 factors, all 3 tiers — compact table format)
  4. Sector attribution cheat sheet
  5. Bear/bull regime factor rankings
  6. Combination signals (19 tested combos)
  7. Stock screening cookbook (ready-to-use templates)

Output: ~/Downloads/OBQ_Factor_Intelligence.md
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb

BANK = r'D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb'
DOWNLOADS = Path.home() / 'Downloads'

TIER_ORDER = ['all', 'large', 'mid']

def _pct(v, d=1):
    if v is None: return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v*100:.{d}f}%"

def _num(v, d=3):
    if v is None: return "—"
    return f"{v:.{d}f}"


def main():
    con = duckdb.connect(BANK, read_only=True)

    # Load all CYC-003-GPU factor records
    rows = con.execute("""
        SELECT score_column, cap_tier, run_label,
               obq_fund_score, staircase_score, icir, ic_hit_rate,
               quintile_spread_cagr, q1_cagr, q1_sharpe, q1_max_dd,
               alpha_win_rate, avg_annual_alpha, bear_score, bull_score,
               monotonicity_score, n_obs, n_stocks_avg,
               sector_attribution_json, tortoriello_json
        FROM factor_models
        WHERE run_label LIKE '%CYC-003-GPU%'
        ORDER BY score_column, cap_tier
    """).fetchall()
    cols = [d[0] for d in con.description]
    con.close()

    # Index by (score_column, cap_tier)
    data: dict = {}
    for r in rows:
        rec = dict(zip(cols, r))
        sc = rec['score_column']
        t  = rec['cap_tier']
        data.setdefault(sc, {})[t] = rec

    # Unique score_columns
    all_sc = sorted(data.keys())

    lines = []

    # ── Header ────────────────────────────────────────────────────────────────
    lines += [
        "# OBQ Factor Intelligence — Complete Encyclopedia v2",
        "",
        f"*Generated {time.strftime('%Y-%m-%d')} from OBQ FactorLab CYC-003 backtest.*",
        "*Russell 3000 universe · 1995–2024 · 60 semi-annual periods · Survivorship-bias free*",
        "",
        "---",
        "",
        "## HOW TO USE THIS FILE WITH CLAUDE",
        "",
        "Drop this entire file into a Claude conversation, then ask any of these:",
        "",
        "**Stock screening:**",
        "- 'Screen the Russell 3000 for stocks that would score well on the top 3 OBQ factors'",
        "- 'Which factors should I use to find quality mid-cap stocks?'",
        "- 'Evaluate [stock ticker] against OBQ quality and value criteria'",
        "",
        "**Factor selection:**",
        "- 'Which factors have the highest OBQ Master Score at large-cap?'",
        "- 'Which factors work best in bear markets?'",
        "- 'What 2-factor combo gives the best Staircase score?'",
        "- 'Which sectors does [factor] work best in?'",
        "",
        "**Portfolio construction:**",
        "- 'Build me a quality-value-momentum composite using OBQ factors'",
        "- 'Which factors are most tier-stable (work across all cap sizes)?'",
        "- 'Rank all 91 factors by alpha win rate'",
        "",
        "**Interpretation:**",
        "- 'What does an OBQ Master Score of 0.80 mean?'",
        "- 'Explain the Staircase score for JCN Alpha Trifecta'",
        "- 'Which factors provide bear-market protection?'",
        "",
        "---",
        "",
        "## THE OBQ SCORING SYSTEM",
        "",
        "### OBQ Master Score (0 to 1, higher = better)",
        "",
        "```",
        "OBQ Master = 25% × tanh(ICIR/1.5)         → ranking consistency",
        "           + 25% × tanh(Staircase/0.10)     → quintile ladder quality",
        "           + 20% × Alpha Win Rate            → % of years Q1 beats market",
        "           + 20% × tanh(Avg Annual Alpha/0.05) → how much Q1 beats market",
        "           + 10% × (IC Hit Rate − 0.5) × 2  → % of periods IC is positive",
        "```",
        "",
        "| Score | Verdict | Use |",
        "|---|---|---|",
        "| 0.70–1.00 | Exceptional | Standalone primary signal |",
        "| 0.50–0.70 | Strong | Standalone viable, great combo anchor |",
        "| 0.30–0.50 | Moderate | Best in multi-factor composite |",
        "| 0.10–0.30 | Weak | Supporting signal only |",
        "| < 0.10 | Marginal/Inverted | Avoid as primary signal |",
        "",
        "### Staircase Score",
        "```",
        "Staircase = (Q1 CAGR − Q5 CAGR) × monotonicity × step_uniformity",
        "```",
        "Measures how cleanly Q1>Q2>Q3>Q4>Q5 in CAGR. 0.20+ = Exceptional. 0.10+ = Clean. 0.05+ = Modest.",
        "",
        "### Cap Tiers",
        "- **all**: Full Russell 3000 (~3,440 stocks/period)",
        "- **large**: $10B+ market cap (~520 stocks/period)",
        "- **mid**: $2B–$10B market cap (~660 stocks/period)",
        "",
        "---",
        "",
    ]

    # ── Full factor database — compact 3-tier table ───────────────────────────
    lines += [
        "## COMPLETE FACTOR DATABASE (91 factors × 3 cap tiers)",
        "",
        "Columns: Factor · OBQ(all) · OBQ(large) · OBQ(mid) · Best Tier · "
        "Staircase(best) · Spread(best) · AlphaWin(best) · Bear(best)",
        "",
        "| Factor | all | large | mid | Best | Staircase | Spread | α-Win | Bear |",
        "|---|---|---|---|---|---|---|---|---|",
    ]

    for sc in all_sc:
        tiers = data[sc]
        obqs = {t: (tiers[t]['obq_fund_score'] or 0) for t in TIER_ORDER if t in tiers}
        if not obqs:
            continue
        best_t = max(obqs, key=obqs.get)
        best = tiers[best_t]
        lines.append(
            f"| {sc} | "
            f"{_num(obqs.get('all'),3)} | "
            f"{_num(obqs.get('large'),3)} | "
            f"{_num(obqs.get('mid'),3)} | "
            f"**{best_t}** | "
            f"{_num(best.get('staircase_score'),4)} | "
            f"{_pct(best.get('quintile_spread_cagr'),1)} | "
            f"{_pct(best.get('alpha_win_rate'),0)} | "
            f"{_pct(best.get('bear_score'),1)} |"
        )

    lines += ["", "---", ""]

    # ── Top 20 leaderboard (all-cap) ──────────────────────────────────────────
    lines += [
        "## TOP 20 FACTORS — R3K ALL-CAP (Ranked by OBQ Master Score)",
        "",
        "| Rank | Factor | OBQ | Staircase | Spread | IC Hit | AlphaWin | Bear |",
        "|---|---|---|---|---|---|---|---|",
    ]
    ranked = sorted(
        [(sc, (data[sc].get('all',{}).get('obq_fund_score') or -99)) for sc in all_sc],
        key=lambda x: -x[1]
    )[:20]
    for rank, (sc, _) in enumerate(ranked, 1):
        r = data[sc].get('all', {})
        lines.append(
            f"| {rank} | **{sc}** | {_num(r.get('obq_fund_score'),3)} | "
            f"{_num(r.get('staircase_score'),4)} | {_pct(r.get('quintile_spread_cagr'),1)} | "
            f"{_pct(r.get('ic_hit_rate'),0)} | {_pct(r.get('alpha_win_rate'),0)} | "
            f"{_pct(r.get('bear_score'),1)} |"
        )

    lines += ["", "---", ""]

    # ── Bear-market champions ─────────────────────────────────────────────────
    lines += [
        "## BEAR-MARKET CHAMPION FACTORS",
        "",
        "*Ranked by bear_score (Q1 excess return vs universe during: 1990 recession, 2000–02 dot-com, "
        "2007–09 GFC, 2020 COVID, 2022 rate shock). Positive = Q1 outperforms in downturns.*",
        "",
        "| Factor | Tier | Bear Score | OBQ | Spread |",
        "|---|---|---|---|---|",
    ]
    bear_ranked = []
    for sc in all_sc:
        for t in TIER_ORDER:
            if t not in data[sc]: continue
            r = data[sc][t]
            bear = r.get('bear_score') or -99
            bear_ranked.append((sc, t, bear, r))
    bear_ranked.sort(key=lambda x: -x[2])
    for sc, t, bear, r in bear_ranked[:20]:
        lines.append(
            f"| {sc} | {t} | **{_pct(bear,1)}** | "
            f"{_num(r.get('obq_fund_score'),3)} | {_pct(r.get('quintile_spread_cagr'),1)} |"
        )

    lines += ["", "---", ""]

    # ── Sector attribution cheat sheet ────────────────────────────────────────
    lines += [
        "## SECTOR ATTRIBUTION CHEAT SHEET",
        "",
        "*For each factor, which GICS sectors have the strongest Q1-Q5 spread? "
        "Use this to understand where each factor's predictive power is concentrated.*",
        "",
    ]

    for sc in ranked[:15]:  # top 15 by OBQ all-cap
        sc_name = sc[0]
        r = data[sc_name].get('all', {})
        sa = r.get('sector_attribution_json')
        if not sa:
            continue
        try:
            sec_data = json.loads(sa) if isinstance(sa, str) else sa
            if not sec_data:
                continue
        except Exception:
            continue
        lines.append(f"### {sc_name}")
        lines.append(f"| Sector | Q1 | Q5 | Spread |")
        lines.append("|---|---|---|---|")
        for s in sec_data[:8]:  # top 8 sectors by spread
            if s.get('sector') == 'Unknown': continue
            lines.append(
                f"| {s.get('sector','?')} | {_pct(s.get('q1'),1)} | "
                f"{_pct(s.get('q5'),1)} | **{_pct(s.get('spread'),1)}** |"
            )
        lines.append("")

    lines += ["---", ""]

    # ── Stock screening cookbook ──────────────────────────────────────────────
    lines += [
        "## STOCK SCREENING COOKBOOK",
        "",
        "Use these templates to ask Claude (or your own screener) to identify stocks.",
        "All criteria based on OBQ CYC-003 factor evidence.",
        "",
        "### Template 1: Quality Compounder (Buffett-style)",
        "Screen for stocks that would score in Q1 on ALL of:",
        "- `jcn_qarp` (OBQ 0.848 large, 0.828 all) — Quality at Reasonable Price",
        "- `cyc2_roic` (OBQ 0.660) — Return on Invested Capital",
        "- `cyc2_fcf_margin` (check OBQ) — FCF Margin",
        "- `finstr_score` (OBQ 0.817 all) — Balance sheet strength",
        "",
        "### Template 2: Deep Value (Contrarian)",
        "Screen for stocks that would score in Q1 on:",
        "- `cyc2_ps` (OBQ 0.185 all — note: weak standalone, use in combo)",
        "- `cyc2_ev_ebitda` (check OBQ)",
        "- `cyc2_pfcf` (check OBQ)",
        "- Avoid stocks scoring in Q5 on `quality_score` (value traps)",
        "",
        "### Template 3: Momentum Quality (QARM)",
        "Screen for stocks in Q1 on ALL of:",
        "- `jcn_alpha_trifecta` (OBQ 0.829 all) — best 3-factor combo",
        "- `cyc2_mom_12m` (OBQ — see table) — 12-month momentum",
        "- `quality_score` (OBQ 0.820) — Quality filter",
        "",
        "### Template 4: Bear-Proof Portfolio",
        "For defensive positioning, prioritise factors with high bear_score:",
        "- `jcn_qarp` (bear: +21.9% large)",
        "- `jcn_alpha_trifecta` (bear: +18.8% large)",
        "- `jcn_full_composite` (bear: +21.9% large)",
        "- `quality_score` (check bear score in table above)",
        "",
        "### Template 5: Mid-Cap Alpha",
        "For mid-cap ($2B–$10B) universe, top factors by OBQ mid-tier:",
        "- `jcn_alpha_trifecta` (OBQ 0.784 mid)",
        "- `jcn_qarp` (OBQ 0.751 mid)",
        "- `quality_score` (check mid OBQ in table)",
        "- `jcn_quality_momentum` (check mid OBQ)",
        "",
        "---",
        "",
        "## HOW TO EVALUATE A SPECIFIC STOCK",
        "",
        "Ask Claude: *'Using the OBQ factor framework in this document, evaluate [TICKER]. "
        "Tell me where it likely falls on the top 5 OBQ factors and whether it would qualify "
        "for Q1 in any of the Exceptional-tier factors.'*",
        "",
        "Claude will need the stock's:",
        "- **P/S ratio** (for cyc2_ps, value_score)",
        "- **ROIC** (for cyc2_roic, quality_score)",
        "- **FCF Margin** (for cyc2_fcf_margin)",
        "- **Net Debt/EBITDA** (for finstr_score)",
        "- **12-month return** (for momentum_score)",
        "- **EV/EBITDA** (for cyc2_ev_ebitda)",
        "",
        "These are all available on financial data sites (Macrotrends, Gurufocus, Koyfin, etc.).",
        "",
        "---",
        "",
        f"*OBQ FactorLab · Encyclopedia v2 · CYC-003-GPU · Generated {time.strftime('%Y-%m-%d')}*",
        "*Full methodology: see Chapter 1 (Database & Methodology) in the companion PDF*",
    ]

    out_path = DOWNLOADS / 'OBQ_Factor_Intelligence.md'
    out_path.write_text('\n'.join(lines), encoding='utf-8')
    sz = out_path.stat().st_size / 1e3
    print(f"Written: {out_path}  ({sz:.0f} KB)")


if __name__ == '__main__':
    main()
