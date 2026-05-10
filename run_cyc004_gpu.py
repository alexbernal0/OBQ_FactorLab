# -*- coding: utf-8 -*-
"""
run_cyc004_gpu.py — CYC-004 Full Factor GPU Batch Run
======================================================
Entry point for the CYC-004 factor backtest batch.

CYC-004 tests 37 new PURE FACTOR baselines across 3 cap tiers (all/large/mid),
computed with full GPU acceleration on the RTX 3090.

Two-step execution:
  Step A: python run_cyc004_gpu.py --compute-scores   # Build score table (~15-20 min, CPU)
  Step B: python run_cyc004_gpu.py                    # GPU backtest batch (~5 min)

Or combined:
  python run_cyc004_gpu.py --full                     # Steps A + B sequentially

Usage:
    python run_cyc004_gpu.py                 # GPU batch only (scores must already exist)
    python run_cyc004_gpu.py --compute-scores # Compute scores then GPU batch
    python run_cyc004_gpu.py --dry-run       # No bank writes
    python run_cyc004_gpu.py --parity        # Parity check vs CYC-003 overlapping factors
    python run_cyc004_gpu.py --tier all      # Single tier only
    python run_cyc004_gpu.py --tier large    # Large-cap only
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

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Imports ────────────────────────────────────────────────────────────────────
from engine.gpu_data_loader import load_all_data, MIRROR_DB
from engine.gpu_batch_runner import (
    run_cyc003_gpu,          # reusing same batch runner engine
    DEFAULT_CAP_TIERS,
    COMBO_TIERS,
)

# ── Config ─────────────────────────────────────────────────────────────────────
START_DATE = '1995-03-31'
END_DATE   = '2024-12-31'

FUND_DB = os.environ.get("OBQ_FUND_DB", r"D:/OBQ_AI/obq_fundamentals.duckdb")

# ── CYC-004 factor list (37 pure factors, 3 tiers = 111 jobs) ─────────────────
# (score_col, direction, display_name, cycle)
CYC004_FACTORS = [
    # TIER 1 — 12 factors
    ("cyc4_accruals_ratio",       "lower_better",  "Accruals Ratio",                "cyc004"),
    ("cyc4_asset_turnover",       "higher_better", "Asset Turnover",                "cyc004"),
    ("cyc4_ebit_assets",          "higher_better", "EBIT / Assets",                 "cyc004"),
    ("cyc4_ocf_assets",           "higher_better", "OCF / Assets",                  "cyc004"),
    ("cyc4_roe",                  "higher_better", "Return on Equity",              "cyc004"),
    ("cyc4_dividend_yield",       "higher_better", "Dividend Yield",                "cyc004"),
    ("cyc4_net_payout_yield",     "higher_better", "Net Payout Yield",              "cyc004"),
    ("cyc4_eps_stability",        "lower_better",  "EPS Stability",                 "cyc004"),
    ("cyc4_sales_stability",      "lower_better",  "Sales Stability",               "cyc004"),
    ("cyc4_current_ratio",        "higher_better", "Current Ratio",                 "cyc004"),
    ("cyc4_fscore",               "higher_better", "Piotroski F-Score",             "cyc004"),
    ("cyc4_realized_vol",         "lower_better",  "Realized Volatility",           "cyc004"),
    # TIER 2 — 13 factors
    ("cyc4_ebit_ev",              "higher_better", "EBIT / EV",                     "cyc004"),
    ("cyc4_fcf_ev",               "higher_better", "FCF / EV",                      "cyc004"),
    ("cyc4_sales_ev",             "higher_better", "Sales / EV",                    "cyc004"),
    ("cyc4_pretax_margin",        "higher_better", "Pretax Margin",                 "cyc004"),
    ("cyc4_pretax_margin_dev",    "higher_better", "Pretax Margin vs 5yr",          "cyc004"),
    ("cyc4_wc_assets",            "lower_better",  "Working Capital / Assets",      "cyc004"),
    ("cyc4_tax_paid_sales",       "higher_better", "Tax Expense / Sales",           "cyc004"),
    ("cyc4_op_leverage",          "higher_better", "Operating Leverage",            "cyc004"),
    ("cyc4_retained_earnings_ta", "higher_better", "Retained Earnings / TA",        "cyc004"),
    ("cyc4_shareholder_yield",    "higher_better", "Shareholder Yield",             "cyc004"),
    ("cyc4_market_beta",          "lower_better",  "Market Beta (60m)",             "cyc004"),
    ("cyc4_log_market_cap",       "higher_better", "Log Market Cap (Size)",         "cyc004"),
    ("cyc4_intangibles_pb",       "lower_better",  "Intangibles-Adjusted P/B",      "cyc004"),
    # TIER 3 — 12 factors
    ("cyc4_cash_conv_cycle",      "lower_better",  "Cash Conversion Cycle",         "cyc004"),
    ("cyc4_change_ar_assets",     "lower_better",  "Change AR/Assets",              "cyc004"),
    ("cyc4_change_inv_assets",    "lower_better",  "Change Inventory/Assets",       "cyc004"),
    ("cyc4_skip_month_mom",       "higher_better", "Skip-Month Momentum",           "cyc004"),
    ("cyc4_gross_margin",         "higher_better", "Gross Profit Margin",           "cyc004"),
    ("cyc4_debt_equity",          "lower_better",  "Debt / Equity",                 "cyc004"),
    ("cyc4_quick_ratio",          "higher_better", "Quick Ratio",                   "cyc004"),
    ("cyc4_idio_vol",             "lower_better",  "Idiosyncratic Volatility",      "cyc004"),
    ("cyc4_altman_z",             "higher_better", "Altman Z-Score",                "cyc004"),
    ("cyc4_repurchase_yield",     "higher_better", "Repurchase Yield",              "cyc004"),
    ("cyc4_roa_dev",              "higher_better", "ROA vs 5yr (Trend)",            "cyc004"),
    ("cyc4_roe_dev",              "higher_better", "ROE vs 5yr (Trend)",            "cyc004"),
]


def _print_banner(n_factors: int, tiers: dict, dry_run: bool = False):
    import cupy as cp
    dev = cp.cuda.Device()
    free, total = dev.mem_info
    n_tiers = len(tiers)
    print(f"\n{'='*65}")
    print(f"  CYC-004 GPU Factor Batch Run — Pure Factor Baselines")
    print(f"  GPU:      Device {dev.id} | cc={dev.compute_capability} | "
          f"VRAM {free/1e9:.1f}/{total/1e9:.1f}GB")
    print(f"  Factors:  {n_factors} singles x {n_tiers} tiers = {n_factors * n_tiers}")
    print(f"  Combos:   0 (pure baselines only — no combos in CYC-004)")
    print(f"  DB:       {MIRROR_DB}")
    print(f"  Date:     {START_DATE} to {END_DATE}")
    print(f"  Tiers:    {list(tiers.keys())}")
    if dry_run:
        print(f"  MODE:     DRY RUN — no bank writes")
    print(f"{'='*65}\n")


def _check_scores_exist() -> tuple[bool, int]:
    """Check if CYC-004 scores have been computed."""
    try:
        import duckdb
        con = duckdb.connect(FUND_DB, read_only=True)
        tbl_exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='scores' AND table_name='obq_cyc004_scores'
        """).fetchone()[0] > 0
        if not tbl_exists:
            con.close()
            return False, 0
        row_count = con.execute("SELECT COUNT(*) FROM scores.obq_cyc004_scores").fetchone()[0]
        con.close()
        return True, row_count
    except Exception:
        return False, 0


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='CYC-004 GPU Factor Batch — Pure Factor Baselines')
    ap.add_argument('--compute-scores', action='store_true',
                    help='Compute CYC-004 score table first (CPU, ~15-20 min)')
    ap.add_argument('--full',    action='store_true',
                    help='Run --compute-scores then GPU batch in one go')
    ap.add_argument('--tier',    type=str, default=None,
                    help='Single tier: all | large | mid')
    ap.add_argument('--dry-run', action='store_true',
                    help='No bank writes')
    ap.add_argument('--start',   type=str, default=START_DATE)
    ap.add_argument('--end',     type=str, default=END_DATE)
    ap.add_argument('--factors-only', type=str, default=None,
                    help='Comma-separated score_columns to run (for testing)')
    args = ap.parse_args()

    # ── Step A: Compute scores if requested ───────────────────────────────────
    if args.compute_scores or args.full:
        log.info("=" * 65)
        log.info("CYC-004 Score Computation (CPU pipeline)")
        log.info("=" * 65)
        from engine.cyc004_score_compute import run_score_pipeline
        score_result = run_score_pipeline(
            fund_db=FUND_DB,
            mirror_db=MIRROR_DB,
            start_date=args.start,
            end_date=args.end,
            overwrite=True,
            dry_run=args.dry_run,
        )
        log.info(f"Scores computed: {score_result['rows']:,} rows | "
                 f"{score_result['symbols']:,} symbols | {score_result['elapsed_s']:.1f}s")

        if not args.full:
            sys.exit(0)

    # ── Check scores exist before GPU run ─────────────────────────────────────
    scores_exist, score_rows = _check_scores_exist()
    if not scores_exist:
        print("\nERROR: CYC-004 score table not found.")
        print("Run first: python run_cyc004_gpu.py --compute-scores")
        print("Or run combined: python run_cyc004_gpu.py --full")
        sys.exit(1)

    # Build active tiers
    tiers = DEFAULT_CAP_TIERS  # all / large / mid
    if args.tier:
        if args.tier not in DEFAULT_CAP_TIERS:
            log.error(f"Unknown tier '{args.tier}'. Valid: {list(DEFAULT_CAP_TIERS.keys())}")
            sys.exit(1)
        tiers = {args.tier: DEFAULT_CAP_TIERS[args.tier]}

    # Filter factors if requested
    all_factors = CYC004_FACTORS
    if args.factors_only:
        keep = set(args.factors_only.split(','))
        all_factors = [f for f in all_factors if f[0] in keep]
        log.info(f"Filtered to {len(all_factors)} specified factors")

    _print_banner(len(all_factors), tiers, dry_run=args.dry_run)

    # ── Step B: Load all data into GPU VRAM ───────────────────────────────────
    t0 = time.time()
    log.info("Step 1/2: Loading all data into GPU VRAM (includes CYC-004 scores)...")
    pack = load_all_data(
        mirror_db=MIRROR_DB,
        fund_db=FUND_DB,
        start_date=args.start,
        end_date=args.end,
        rebal_freq='semi-annual',
        hold_months=6,
        min_price=5.0,
        min_adv_usd=1_000_000.0,
    )
    load_s = time.time() - t0

    # Check how many CYC-004 factors actually loaded
    cyc4_loaded = [f[0] for f in all_factors if f[0] in pack.score_columns]
    cyc4_missing = [f[0] for f in all_factors if f[0] not in pack.score_columns]
    log.info(f"CYC-004 factors in VRAM: {len(cyc4_loaded)}/{len(all_factors)}")
    if cyc4_missing:
        log.warning(f"  Missing from VRAM: {cyc4_missing}")
        # Filter to only factors that actually loaded
        all_factors = [f for f in all_factors if f[0] in pack.score_columns]

    log.info(f"Data load complete: {load_s:.1f}s | {pack.gpu_status()}")

    # ── Step C: Run all CYC-004 jobs on pre-loaded VRAM data ─────────────────
    total_jobs = len(all_factors) * len(tiers)
    log.info(f"\nStep 2/2: Running {total_jobs} GPU jobs ({len(all_factors)} factors x "
             f"{len(tiers)} tiers)...")
    t1 = time.time()

    batch_result = run_cyc003_gpu(  # same engine, different factor list
        data_pack=pack,
        factor_list=all_factors,
        cap_tiers=tiers,
        combo_list=[],              # CYC-004 = pure baselines, no combos
        run_label_tag='[CYC-004-GPU]',
        save_to_bank=not args.dry_run,
        cb=lambda m: log.info(m),
    )

    run_s  = time.time() - t1
    total_s = time.time() - t0

    # ── Summary ───────────────────────────────────────────────────────────────
    n_ok  = batch_result['ok_count']
    n_err = batch_result['err_count']
    n_tot = batch_result['total']

    print(f"\n{'='*65}")
    print(f"  CYC-004 GPU Run Complete — Pure Factor Baselines")
    print(f"  Results:  {n_ok}/{n_tot} ok | {n_err} errors")
    print(f"  Load:     {load_s:.1f}s")
    print(f"  Run:      {run_s:.1f}s ({run_s/max(n_tot,1):.2f}s/job)")
    print(f"  TOTAL:    {total_s:.1f}s ({total_s/60:.1f}min)")
    print(f"{'='*65}\n")

    # Save results log
    if not args.dry_run:
        log_path = 'cyc004_gpu_results.log'
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"CYC-004 GPU Run — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total: {n_ok}/{n_tot} ok | {n_err} errors | {total_s:.1f}s\n\n")
            f.write('\n'.join(batch_result['results_log']))
        log.info(f"Results saved to {log_path}")

    sys.exit(1 if n_err > 0 and n_ok == 0 else 0)
