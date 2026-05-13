# -*- coding: utf-8 -*-
"""
run_pm_tranche_trial.py — Trial run: JCN Full Composite, 28-stock 4-tranche model
==================================================================================

Portfolio spec:
  - 28 stocks total
  - 4 tranches × 7 stocks
  - Each tranche rebalances annually, staggered by quarter
    Tranche 1 → Mar 31   Tranche 2 → Jun 30
    Tranche 3 → Sep 30   Tranche 4 → Dec 31
  - Equal weight within each tranche at rebalance
  - 1-year hold per tranche
  - No sector limits
  - All-cap universe, $5 min price

Usage:
    python run_pm_tranche_trial.py
    python run_pm_tranche_trial.py --dry-run     # no save
    python run_pm_tranche_trial.py --score jcn_qarp
"""

from __future__ import annotations
import sys
import argparse
import logging
import io as _io

if isinstance(sys.stdout, _io.TextIOWrapper):
    sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, ".")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _cb(level, msg):
    log.info(f"  {msg}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="PM Tranche Trial Run")
    ap.add_argument("--score",   default="jcn_full_composite", help="Score column")
    ap.add_argument("--start",   default="1995-03-31")
    ap.add_argument("--end",     default="2024-12-31")
    ap.add_argument("--dry-run", action="store_true", help="Skip saving to bank")
    args = ap.parse_args()

    from engine.tranche_portfolio_backtest import TrancheCfg, run_tranche_portfolio_backtest

    cfg = TrancheCfg(
        score_column          = args.score,
        score_direction       = "higher_better",
        total_stocks          = 28,
        n_tranches            = 4,
        sector_max            = 0,          # no sector limit
        weight_scheme         = "equal",
        start_date            = args.start,
        end_date              = args.end,
        min_price             = 5.0,
        min_adv_usd           = 1_000_000.0,
        min_market_cap        = 0.0,
        cap_tier              = "all",
        transaction_cost_bps  = 15.0,
        stop_loss_pct         = 0.0,
    )

    log.info("=" * 65)
    log.info(f"PM TRANCHE TRIAL — {args.score}")
    log.info(f"  Config: {cfg.n_tranches} tranches × {cfg.total_stocks // cfg.n_tranches} stocks")
    log.info(f"  Universe: all-cap | min_price=${cfg.min_price}")
    log.info(f"  Period:   {args.start} → {args.end}")
    log.info(f"  Sector cap: {'NONE' if cfg.sector_max == 0 else cfg.sector_max}")
    log.info("=" * 65)

    result = run_tranche_portfolio_backtest(cfg, cb=_cb)

    if result["status"] != "complete":
        log.error(f"FAILED: {result.get('error')}")
        sys.exit(1)

    pm = result["portfolio_metrics"]
    spy = result.get("spy_metrics", {})

    log.info("")
    log.info("═" * 65)
    log.info(f"  Run label:  {result['run_label']}")
    log.info(f"  Periods:    {result['n_periods']}  ({result['elapsed_s']}s)")
    log.info(f"  ─────────────────────────────────────────────")
    log.info(f"  CAGR:       {(pm.get('cagr',0)*100):.2f}%   SPY: {(spy.get('cagr',0)*100):.2f}%")
    log.info(f"  Sharpe:     {pm.get('sharpe',0):.2f}        SPY: {spy.get('sharpe',0):.2f}")
    log.info(f"  Max DD:     {(pm.get('max_dd',0)*100):.1f}%  SPY: {(spy.get('max_dd',0)*100):.1f}%")
    log.info(f"  Sortino:    {pm.get('sortino',0):.2f}")
    log.info(f"  Calmar:     {pm.get('calmar',0):.2f}")
    log.info(f"  Ann Vol:    {(pm.get('ann_vol',0)*100):.1f}%")
    log.info(f"  Win Rate:   {(pm.get('win_rate_monthly',0)*100):.0f}%")
    if pm.get('alpha') is not None:
        log.info(f"  Alpha:      {(pm.get('alpha',0)*100):.2f}%  Beta: {pm.get('beta',0):.2f}")
    log.info("═" * 65)

    # Print annual returns
    ann = result.get("annual_ret_by_year", [])
    if ann:
        log.info("")
        log.info("  Annual Returns:")
        log.info(f"  {'Year':<6} {'Portfolio':>10} {'SPY':>8}")
        log.info(f"  {'-'*6} {'-'*10} {'-'*8}")
        for row in ann:
            yr  = row.get("year", "?")
            pr  = row.get("portfolio_ret")
            sr  = row.get("spy_ret")
            ps  = f"{pr*100:+.1f}%" if pr is not None else "—"
            ss  = f"{sr*100:+.1f}%" if sr is not None else "—"
            log.info(f"  {yr:<6} {ps:>10} {ss:>8}")

    # Save to bank
    if not args.dry_run:
        log.info("")
        log.info("Saving to portfolio bank...")
        from engine.portfolio_bank import save_portfolio_model
        sid = save_portfolio_model(result, overwrite=True)
        log.info(f"  Saved: {sid}")
        result["strategy_id"] = sid
    else:
        log.info("")
        log.info("[DRY RUN] — not saved to bank")

    log.info("Done.")
