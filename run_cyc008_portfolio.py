# -*- coding: utf-8 -*-
"""
run_cyc008_portfolio.py — CYC-008 Portfolio Model Batch Run
============================================================
Builds a Top-28, 4-Tranche, quarterly-staggered, equal-weight portfolio
model for every unique factor in the bank (139 factors total).

Architecture:
  GPU: batch top-N stock selection across all factors simultaneously
  CPU: sequential tranche simulation per factor (holdings carry over)
  Post-batch: OBQ Port Score with percentile drawdown ranking

Portfolio config:
  - 28 stocks total (4 tranches × 7 stocks)
  - Tranche 1→Mar31  Tranche 2→Jun30  Tranche 3→Sep30  Tranche 4→Dec31
  - Each tranche holds 12 months (annual rebalance per tranche)
  - No sector caps — pure factor signal
  - Equal weight within tranche at rebalance
  - 15bps one-way transaction cost
  - Benchmark: SPX (spliced SPX price + SPY total return from 1990)

Usage:
    python run_cyc008_portfolio.py                  # full run, all 139 factors
    python run_cyc008_portfolio.py --dry-run        # 3 factors, no save
    python run_cyc008_portfolio.py --top-n 20       # override stock count
    python run_cyc008_portfolio.py --factor jcn_qarp # single factor
    python run_cyc008_portfolio.py --min-obq 0.5    # only factors OBQ >= 0.5
"""

from __future__ import annotations

import os
import sys
import time
import argparse
import logging
import io as _io

if isinstance(sys.stdout, _io.TextIOWrapper):
    sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

os.environ.setdefault('CUDA_PATH', r'C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

import duckdb
import cupy as cp

from engine.gpu_data_loader  import load_all_data, MIRROR_DB
from engine.gpu_portfolio_compute import run_portfolio_batch_gpu

FUND_DB    = os.environ.get("OBQ_FUND_DB",   r"D:/OBQ_AI/obq_fundamentals.duckdb")
BANK_DB    = os.environ.get("OBQ_BANK_DB",   r"D:/OBQ_AI/OBQ_FactorLab_Bank/factor_strategy_bank.duckdb")
START_DATE = "1995-03-31"
END_DATE   = "2024-12-31"

# ── Score direction registry ──────────────────────────────────────────────────
# Factors where LOWER score = better quintile 1 rank
LOWER_BETTER_FACTORS = {
    "longeq_rank", "rulebreaker_rank", "fundsmith_rank", "moat_rank",
    "cyc2_ev_ebitda", "cyc2_pb", "cyc2_pe", "cyc2_ps", "cyc2_pfcf",
    "cyc2_nd_ebit", "cyc2_nd_ebitda", "cyc4_debt_equity",
    "cyc4_idio_vol", "cyc4_realized_vol", "cyc4_market_beta",
    "cyc4_accruals_ratio", "cyc4_cash_conv_cycle",
    "cyc2_share_chg",
}

# ── Display name registry ─────────────────────────────────────────────────────
DISPLAY_NAMES = {
    "jcn_full_composite":          "JCN Composite",
    "jcn_qarp":                    "JCN QARP",
    "jcn_garp":                    "JCN GARP",
    "jcn_quality_momentum":        "JCN Quality-Momentum",
    "jcn_value_momentum":          "JCN Value-Momentum",
    "jcn_growth_quality_momentum": "JCN Growth-Quality-Momentum",
    "jcn_fortress":                "JCN Fortress",
    "jcn_alpha_trifecta":          "JCN Alpha Trifecta",
    "value_score":                 "OBQ Value",
    "quality_score":               "OBQ Quality",
    "growth_score":                "OBQ Growth",
    "momentum_score":              "OBQ Momentum",
    "finstr_score":                "OBQ FinStr",
    "momentum_af_score":           "Momentum (Alpha Factor)",
    "momentum_fip_score":          "Momentum (FIP)",
    "momentum_sys_score":          "Momentum (Systematic)",
    "value_score_universe":        "OBQ Value (Universe)",
    "quality_score_universe":      "OBQ Quality (Universe)",
    "growth_score_universe":       "OBQ Growth (Universe)",
    "finstr_score_universe":       "OBQ FinStr (Universe)",
    "af_universe_score":           "Alpha Factor (Universe)",
    "longeq_rank":                 "LongEQ Rank",
    "rulebreaker_rank":            "Rulebreaker Rank",
    "fundsmith_rank":              "Fundsmith Rank",
    "moat_score":                  "Moat Score",
    "moat_rank":                   "Moat Rank",
    # CYC-002 pure factors
    "cyc2_ocf_assets":             "OCF / Assets",
    "cyc2_fcf_margin":             "FCF Margin",
    "cyc2_gpa":                    "GPA (Gross Profit/Assets)",
    "cyc2_roa":                    "Return on Assets",
    "cyc2_roce":                   "Return on Capital Employed",
    "cyc2_roic":                   "ROIC",
    "cyc2_cash_roc":               "Cash Return on Capital",
    "cyc2_fcf_yield":              "FCF Yield",
    "cyc2_ev_ebitda":              "EV/EBITDA",
    "cyc2_pb":                     "Price/Book",
    "cyc2_pe":                     "Price/Earnings",
    "cyc2_ps":                     "Price/Sales",
    "cyc2_pfcf":                   "Price/FCF",
    "cyc2_gross_margin":           "Gross Margin",
    "cyc2_op_margin":              "Operating Margin",
    "cyc2_moat_scale":             "Moat (Scale)",
    "cyc2_moat_switching":         "Moat (Switching Costs)",
    "cyc2_moat_cost":              "Moat (Cost Advantage)",
    "cyc2_moat_intangible":        "Moat (Intangibles)",
    "cyc2_moat_network":           "Moat (Network Effect)",
    "cyc2_mom_3m":                 "Momentum 3M",
    "cyc2_mom_6m":                 "Momentum 6M",
    "cyc2_mom_12m":                "Momentum 12M",
    "cyc2_nd_ebit":                "Net Debt / EBIT",
    "cyc2_nd_ebitda":              "Net Debt / EBITDA",
    "cyc2_fcf_debt":               "FCF / Debt",
    "cyc2_int_cov":                "Interest Coverage",
    "cyc2_debt_assets":            "Debt / Assets",
    "cyc2_cash_assets":            "Cash / Assets",
    "cyc2_earn_quality":           "Earnings Quality",
    "cyc2_share_chg":              "Share Count Change",
    "cyc2_rd_ratio":               "R&D Ratio",
    "cyc2_rev_growth_3y":          "Revenue Growth 3Y",
    "cyc2_eps_cagr_1y":            "EPS CAGR 1Y",
    "cyc2_eps_cagr_3y":            "EPS CAGR 3Y",
    "cyc2_eps_cagr_5y":            "EPS CAGR 5Y",
    "cyc2_fcf_cagr_3y":            "FCF CAGR 3Y",
    "cyc2_fcf_cagr_5y":            "FCF CAGR 5Y",
    "cyc2_rev_cagr_1y":            "Revenue CAGR 1Y",
    "cyc2_rev_cagr_3y":            "Revenue CAGR 3Y",
    "cyc2_rev_cagr_5y":            "Revenue CAGR 5Y",
    "cyc2_capex_ocf":              "CapEx / OCF",
    "cyc2_cash_conv":              "Cash Conversion",
    "cyc2_int_pct_op":             "Interest % of Operating Income",
    "cyc2_sys_score":              "Systematic Score",
    "cyc2_fip_6m":                 "FIP 6M",
    "cyc2_fip_12m":                "FIP 12M",
    # CYC-004 pure factors
    "cyc4_ocf_assets":             "OCF / Assets (CYC4)",
    "cyc4_ebit_assets":            "EBIT / Assets",
    "cyc4_ebit_ev":                "EBIT / EV",
    "cyc4_fscore":                 "Piotroski F-Score",
    "cyc4_retained_earnings_ta":   "Retained Earnings / Total Assets",
    "cyc4_roe":                    "Return on Equity",
    "cyc4_roa_dev":                "ROA Deviation",
    "cyc4_roe_dev":                "ROE Deviation",
    "cyc4_pretax_margin":          "Pre-Tax Margin",
    "cyc4_pretax_margin_dev":      "Pre-Tax Margin Dev",
    "cyc4_gross_margin":           "Gross Margin (CYC4)",
    "cyc4_asset_turnover":         "Asset Turnover",
    "cyc4_sales_ev":               "Sales / EV",
    "cyc4_fcf_ev":                 "FCF / EV",
    "cyc4_net_payout_yield":       "Net Payout Yield",
    "cyc4_repurchase_yield":       "Repurchase Yield",
    "cyc4_shareholder_yield":      "Shareholder Yield",
    "cyc4_dividend_yield":         "Dividend Yield",
    "cyc4_altman_z":               "Altman Z-Score",
    "cyc4_current_ratio":          "Current Ratio",
    "cyc4_quick_ratio":            "Quick Ratio",
    "cyc4_debt_equity":            "Debt / Equity",
    "cyc4_cash_conv_cycle":        "Cash Conversion Cycle",
    "cyc4_change_ar_assets":       "Change AR / Assets",
    "cyc4_change_inv_assets":      "Change Inventory / Assets",
    "cyc4_accruals_ratio":         "Accruals Ratio",
    "cyc4_idio_vol":               "Idiosyncratic Volatility",
    "cyc4_realized_vol":           "Realized Volatility",
    "cyc4_market_beta":            "Market Beta",
    "cyc4_log_market_cap":         "Log Market Cap",
    "cyc4_intangibles_pb":         "Intangibles / Book",
    "cyc4_skip_month_mom":         "Skip-Month Momentum",
    "cyc4_eps_stability":          "EPS Stability",
    "cyc4_sales_stability":        "Sales Stability",
    "cyc4_op_leverage":            "Operating Leverage",
    "cyc4_tax_paid_sales":         "Tax Paid / Sales",
    "cyc4_wc_assets":              "Working Capital / Assets",
    # CYC-005 sector factors
    "cyc5_industrials_efficiency": "Industrials Efficiency",
    "cyc5_energy_midcycle_fcf":    "Energy Mid-Cycle FCF",
    "cyc5_realestate_ffo_yield":   "Real Estate FFO Yield",
    "cyc5_healthcare_rnd_yield":   "Healthcare R&D Yield",
    "cyc5_financials_capital":     "Financials Capital Adequacy",
    "cyc5_it_rule_of_40":          "IT Rule of 40",
    "cyc5_materials_ccc":          "Materials Cash Conv Cycle",
    "cyc5_staples_div_growth":     "Staples Dividend Growth",
    "cyc5_comms_content_roi":      "Comms Content ROI",
    "cyc5_consumer_disc_brand":    "Consumer Disc Brand Value",
    "cyc5_utilities_safe_yield":   "Utilities Safe Yield",
    # CYC-007 composites
    "cyc7_hc_quality":             "HC Quality (OCF+EBIT+F)",
    "cyc7_hc_alpha":               "HC Alpha (Trifecta+OCF+FCF)",
    "cyc7_it_quality":             "IT Quality (Trifecta+OBQQual+OCF)",
    "cyc7_it_fcf":                 "IT FCF (FCF+OCF+Rule40)",
    "cyc7_fin_quality":            "FIN Quality (F+Trifecta+QARP)",
    "cyc7_condisc_quality":        "CD Quality (OCF+F+Trifecta)",
    "cyc7_staples_quality":        "CS Quality (OCF+QARP+Trifecta)",
    "cyc7_ind_quality":            "IND Quality (F+Trifecta+IndEff)",
    "cyc7_mat_quality":            "MAT Quality (Trifecta+F+OCF)",
}


def _get_factor_list(bank_db: str, min_obq: float = 0.0,
                     factor_filter: str = None) -> list[dict]:
    """
    Load all unique factors from the factor bank that are available in the
    GPU data pack (scores loaded into VRAM during load_all_data).
    Returns list of {score_column, lower_better, display_name, obq_fund_score}.
    """
    con = duckdb.connect(bank_db, read_only=True)
    rows = con.execute(f"""
        SELECT score_column, MAX(obq_fund_score) as best_obq
        FROM factor_models
        WHERE obq_fund_score IS NOT NULL
          AND (cap_tier = 'all' OR cap_tier LIKE 'all-%')
        GROUP BY score_column
        HAVING MAX(obq_fund_score) >= {min_obq}
        ORDER BY best_obq DESC NULLS LAST
    """).fetchall()
    con.close()

    factors = []
    for score_col, obq in rows:
        if factor_filter and factor_filter not in score_col:
            continue
        factors.append({
            "score_column":  score_col,
            "lower_better":  score_col in LOWER_BETTER_FACTORS,
            "display_name":  DISPLAY_NAMES.get(score_col, score_col),
            "obq_fund_score": obq,
        })
    return factors


def _print_banner(n_jobs: int, cfg: dict, dry_run: bool = False):
    try:
        dev = cp.cuda.Device()
        free, total = dev.mem_info
        gpu_str = f"Device {dev.id} | {free/1e9:.1f}/{total/1e9:.1f}GB free"
    except Exception:
        gpu_str = "GPU info unavailable"

    print(f"\n{'='*70}")
    print(f"  CYC-008 Portfolio Model Batch Run")
    print(f"  GPU:      {gpu_str}")
    print(f"  Factors:  {n_jobs}")
    print(f"  Config:   Top-{cfg['total_stocks']} | {cfg['n_tranches']}T-Qtrly | "
          f"{cfg['hold_months']}mo hold | {cfg['cost_bps']}bps")
    print(f"  Period:   {cfg['start_date']} to {cfg['end_date']}")
    print(f"  Benchmark: SPX (spliced)")
    if dry_run:
        print(f"  MODE:     DRY RUN — no bank writes")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="CYC-008 Portfolio Model Batch Run")
    ap.add_argument("--dry-run",   action="store_true", help="3 factors, no save")
    ap.add_argument("--top-n",     type=int,   default=28,      help="Total stocks in portfolio")
    ap.add_argument("--tranches",  type=int,   default=4,       help="Number of tranches")
    ap.add_argument("--hold",      type=int,   default=12,      help="Hold months per tranche")
    ap.add_argument("--cost-bps",  type=float, default=15.0,    help="One-way cost bps")
    ap.add_argument("--start",     default=START_DATE)
    ap.add_argument("--end",       default=END_DATE)
    ap.add_argument("--min-obq",   type=float, default=0.0,     help="Min OBQ to include factor")
    ap.add_argument("--factor",    default=None,                help="Run single factor only")
    args = ap.parse_args()

    cfg = {
        "total_stocks": args.top_n,
        "n_tranches":   args.tranches,
        "hold_months":  args.hold,
        "cost_bps":     args.cost_bps,
        "start_date":   args.start,
        "end_date":     args.end,
    }

    # ── Load factor list ──────────────────────────────────────────────────────
    log.info("Loading factor list from bank...")
    factors = _get_factor_list(BANK_DB, min_obq=args.min_obq,
                               factor_filter=args.factor)

    if args.dry_run:
        factors = factors[:3]
        log.info(f"DRY RUN: limiting to 3 factors")

    log.info(f"Found {len(factors)} factors to process")

    # ── Load GPU data pack ────────────────────────────────────────────────────
    log.info("Loading all data into GPU VRAM...")
    t0 = time.time()
    pack = load_all_data(
        mirror_db   = MIRROR_DB,
        fund_db     = FUND_DB,
        start_date  = args.start,
        end_date    = args.end,
        rebal_freq  = "quarterly",   # quarterly dates (all quarter-ends)
        hold_months = args.hold,
        min_price   = 5.0,
        min_adv_usd = 1_000_000.0,
    )
    load_s = time.time() - t0
    log.info(f"Data loaded: {load_s:.1f}s | {pack.gpu_status()}")

    # Filter factors to those actually in VRAM
    factors_in_vram = [f for f in factors if f["score_column"] in pack.score_columns]
    factors_missing = [f["score_column"] for f in factors
                       if f["score_column"] not in pack.score_columns]
    if factors_missing:
        log.warning(f"  {len(factors_missing)} factors not in VRAM: {factors_missing[:5]}...")
    log.info(f"  {len(factors_in_vram)}/{len(factors)} factors available in VRAM")

    _print_banner(len(factors_in_vram), cfg, dry_run=args.dry_run)

    if not factors_in_vram:
        log.error("No factors available — check score columns in VRAM")
        sys.exit(1)

    # ── GPU warmup ────────────────────────────────────────────────────────────
    log.info("GPU warmup...")
    _d = cp.zeros((5, 50), dtype=cp.float64)
    _v = cp.ones((5, 50), dtype=bool)
    from engine.gpu_portfolio_compute import gpu_topn_selection
    gpu_topn_selection(_d, _v, top_n=5)
    del _d, _v
    log.info("  Warmup done")

    # ── Run batch ─────────────────────────────────────────────────────────────
    def _cb(level, msg):
        log.info(f"  {msg}")

    results = run_portfolio_batch_gpu(
        pack          = pack,
        factor_configs= factors_in_vram,
        total_stocks  = cfg["total_stocks"],
        n_tranches    = cfg["n_tranches"],
        hold_months   = cfg["hold_months"],
        cost_bps      = cfg["cost_bps"],
        start_date    = cfg["start_date"],
        end_date      = cfg["end_date"],
        save          = not args.dry_run,
        overwrite     = True,
        cb            = _cb,
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    total_s = time.time() - t0
    ok = sum(1 for r in results if r.get("status") == "complete")

    print(f"\n{'='*70}")
    print(f"  CYC-008 COMPLETE | {ok}/{len(factors_in_vram)} OK | {total_s:.0f}s total")
    print(f"{'='*70}")

    if results:
        # Sort by OBQ Port Score
        ranked = sorted(results, key=lambda r: r.get("obq_port_score") or -99, reverse=True)
        print(f"\n  {'Factor':<35} {'OBQ Port':>8} {'CAGR':>7} {'Calmar':>7} "
              f"{'MaxDD':>7} {'R²':>5} {'Alpha':>7}")
        print(f"  {'-'*35} {'-'*8} {'-'*7} {'-'*7} {'-'*7} {'-'*5} {'-'*7}")
        for r in ranked[:20]:
            pm   = r.get("portfolio_metrics", {})
            obq  = r.get("obq_port_score")
            cagr = pm.get("cagr", 0) * 100
            cal  = pm.get("calmar_gips") or pm.get("calmar") or 0
            mdd  = pm.get("max_dd", 0) * 100
            r2   = pm.get("equity_r2") or 0
            alph = pm.get("alpha", 0) * 100
            name = r.get("display_name", r.get("score_column", "?"))[:34]
            print(f"  {name:<35} {obq:>8.3f} {cagr:>6.1f}% {cal:>7.2f} "
                  f"{mdd:>6.1f}% {r2:>5.3f} {alph:>6.1f}%")

    # Save run log
    if not args.dry_run:
        log_path = "cyc008_portfolio_results.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"CYC-008 Portfolio Run — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Factors: {ok}/{len(factors_in_vram)} | Total: {total_s:.0f}s\n\n")
            for r in results:
                pm = r.get("portfolio_metrics", {})
                f.write(f"{r.get('score_column')} | OBQ={r.get('obq_port_score',0):.3f} | "
                        f"CAGR={pm.get('cagr',0)*100:.1f}% | MaxDD={pm.get('max_dd',0)*100:.1f}%\n")
        log.info(f"Results saved to {log_path}")
