"""
CYC-001 Baseline Backtests — All JCN Scores
============================================
For each score, run:
  1. Factor (quintile) backtest — 5Q, semi-annual, large-cap top-1000
  2. Portfolio (Top-20) backtest — semi-annual, equal-weight, large-cap top-1000

Universe: Top ~1000 by market cap (min_market_cap=$10B = large+mega cap)
Period: 1990-07-31 → 2024-12-31
Rebalance: Semi-Annual (June + December)
Min price: $5
Cost: 15 bps

Each strategy gets a short notation in the run_label.
"""
import sys, time, traceback
sys.path.insert(0, '.')

from engine.factor_backtest import run_factor_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_portfolio_backtest, PortfolioBacktestConfig
from engine.strategy_bank import save_factor_model
from engine.portfolio_bank import save_portfolio_model

# All scores to test (skip moat_score — different table)
SCORES = {
    "jcn_full_composite":         "JCN Composite — all-factor blend",
    "jcn_qarp":                   "JCN QARP — quality at reasonable price",
    "jcn_garp":                   "JCN GARP — growth at reasonable price",
    "jcn_quality_momentum":       "JCN Quality-Momentum blend",
    "jcn_value_momentum":         "JCN Value-Momentum blend",
    "jcn_growth_quality_momentum":"JCN Growth-Quality-Momentum",
    "jcn_fortress":               "JCN Fortress — defensive quality",
    "value_score":                "Pure Value factor",
    "quality_score":              "Pure Quality factor",
    "growth_score":               "Pure Growth factor",
    "finstr_score":               "Financial Strength factor",
    "momentum_score":             "Momentum (standard)",
    "momentum_af_score":          "Momentum Alpha Factor",
    "momentum_fip_score":         "Momentum FIP",
}

COMMON_PARAMS = dict(
    start_date           = "1990-07-31",
    end_date             = "2024-12-31",
    min_price            = 5.0,
    min_adv_usd          = 1_000_000.0,
    min_market_cap       = 10_000_000_000.0,  # $10B = large+mega cap (~top 1000)
    transaction_cost_bps = 15.0,
)

results = []
errors  = []

def _cb(msg):
    print(f"    {msg}")

print("=" * 70)
print("CYC-001 BASELINE BACKTEST SUITE")
print(f"Scores: {len(SCORES)} | Factor + Portfolio = {len(SCORES)*2} total runs")
print("=" * 70)

for score_col, notation in SCORES.items():
    print(f"\n{'='*70}")
    print(f"SCORE: {score_col}")
    print(f"       {notation}")

    # ── 1. FACTOR (Quintile) Backtest ────────────────────────────────────────
    print(f"\n  [FACTOR] 5-quintile, semi-annual...")
    try:
        factor_label = f"{notation} | 5Q | 6mo | Large-Cap | 1990-2024 [CYC-001-BASELINE]"
        cfg_f = FactorBacktestConfig(
            score_column         = score_col,
            n_buckets            = 5,
            hold_months          = 6,
            rebalance_freq       = "semi-annual",
            cap_tier             = "all",   # use min_market_cap instead
            run_label            = factor_label,
            **COMMON_PARAMS,
        )
        t0 = time.time()
        r_f = run_factor_backtest(cfg_f, cb=_cb)
        elapsed = round(time.time()-t0, 1)

        if r_f.get("status") == "complete":
            sid = save_factor_model(r_f, overwrite=True)
            fm  = r_f.get("factor_metrics", {})
            print(f"  ✓ FACTOR saved: {sid}")
            print(f"    ICIR={fm.get('icir',0):.3f}  Spread={fm.get('quintile_spread_cagr',0)*100:.2f}%  Q1={fm.get('q1_cagr',0)*100:.2f}%  {elapsed}s")
            results.append({"type":"factor","score":score_col,"sid":sid,"icir":fm.get("icir"),
                            "spread":fm.get("quintile_spread_cagr"),"q1_cagr":fm.get("q1_cagr")})
        else:
            print(f"  ✗ FACTOR error: {r_f.get('error')}")
            errors.append({"type":"factor","score":score_col,"error":r_f.get("error")})
    except Exception as e:
        traceback.print_exc()
        errors.append({"type":"factor","score":score_col,"error":str(e)})

    # ── 2. PORTFOLIO (Top-20) Backtest ───────────────────────────────────────
    print(f"\n  [PORTFOLIO] Top-20, semi-annual, 5/sector...")
    try:
        port_label = f"{notation} | Top-20 | Semi-Ann | 5/Sector | Large-Cap | 1990-2024 [CYC-001-BASELINE]"
        cfg_p = PortfolioBacktestConfig(
            score_column         = score_col,
            top_n                = 20,
            sector_max           = 5,
            rebalance_freq       = "semi-annual",
            cap_tier             = "all",
            run_label            = port_label,
            **COMMON_PARAMS,
        )
        t0 = time.time()
        r_p = run_portfolio_backtest(cfg_p, cb=_cb)
        elapsed = round(time.time()-t0, 1)

        if r_p.get("status") == "complete":
            sid = save_portfolio_model(r_p, overwrite=True)
            pm  = r_p.get("portfolio_metrics", {})
            print(f"  ✓ PORTFOLIO saved: {sid}")
            print(f"    CAGR={pm.get('cagr',0)*100:.2f}%  Sharpe={pm.get('sharpe',0):.3f}  MaxDD={pm.get('max_dd',0)*100:.1f}%  {elapsed}s")
            results.append({"type":"portfolio","score":score_col,"sid":sid,
                            "cagr":pm.get("cagr"),"sharpe":pm.get("sharpe"),"max_dd":pm.get("max_dd")})
        else:
            print(f"  ✗ PORTFOLIO error: {r_p.get('error')}")
            errors.append({"type":"portfolio","score":score_col,"error":r_p.get("error")})
    except Exception as e:
        traceback.print_exc()
        errors.append({"type":"portfolio","score":score_col,"error":str(e)})

# ── Summary ──────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("BATCH COMPLETE")
print("=" * 70)
print(f"\nSuccessful: {len(results)} / {len(SCORES)*2}")
print(f"Errors:     {len(errors)}")

if results:
    print("\nFACTOR RESULTS (sorted by ICIR):")
    factor_r = sorted([r for r in results if r["type"]=="factor"], key=lambda x: x.get("icir") or 0, reverse=True)
    print(f"  {'SCORE':<40} {'SID':<30} {'ICIR':>6} {'SPREAD':>8} {'Q1 CAGR':>8}")
    for r in factor_r:
        print(f"  {r['score']:<40} {r['sid']:<30} {(r.get('icir') or 0):>6.3f} {(r.get('spread') or 0)*100:>7.2f}% {(r.get('q1_cagr') or 0)*100:>7.2f}%")

    print("\nPORTFOLIO RESULTS (sorted by Sharpe):")
    port_r = sorted([r for r in results if r["type"]=="portfolio"], key=lambda x: x.get("sharpe") or 0, reverse=True)
    print(f"  {'SCORE':<40} {'SID':<30} {'CAGR':>7} {'SHARPE':>7} {'MAX DD':>8}")
    for r in port_r:
        print(f"  {r['score']:<40} {r['sid']:<30} {(r.get('cagr') or 0)*100:>6.2f}% {(r.get('sharpe') or 0):>7.3f} {(r.get('max_dd') or 0)*100:>7.1f}%")

if errors:
    print("\nERRORS:")
    for e in errors:
        print(f"  [{e['type']}] {e['score']}: {e['error']}")
