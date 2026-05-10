"""Dry run both model types via the Flask API, then print result summary."""
import requests, time, json

BASE = "http://127.0.0.1:5744"

def run(cfg, label):
    print(f"\n{'='*70}\n{label}\n{'='*70}")
    resp = requests.post(f"{BASE}/api/backtest/run", json=cfg, timeout=10)
    run_id = resp.json()["run_id"]
    print(f"  run_id: {run_id[:8]}  launched")

    # Stream log
    with requests.get(f"{BASE}/api/backtest/stream/{run_id}", stream=True, timeout=300) as sse:
        for line in sse.iter_lines():
            if not line: continue
            line = line.decode()
            if not line.startswith("data:"): continue
            d = json.loads(line[5:])
            if d.get("msg"):
                print(f"  [{d.get('level','info'):5}] {d['msg']}")
            status_resp = requests.get(f"{BASE}/api/backtest/status/{run_id}").json()
            if status_resp["status"] in ("complete","error"):
                break

    result = requests.get(f"{BASE}/api/backtest/result/{run_id}", timeout=10).json()
    if result.get("status") == "error":
        print(f"  ERROR: {result.get('error')}")
        return

    mode = result.get("mode")
    if mode == "quintile":
        mets = result.get("quintile_metrics", {})
        q1 = mets.get("Q1", {})
        q5 = mets.get(f"Q{cfg['n_quintiles']}", {})
        obs = result.get("obsidian_score", {})
        print(f"\n  QUINTILE RESULTS:")
        print(f"    Q1 CAGR:          {(q1.get('cagr',0)*100):.2f}%")
        print(f"    Q1 Sharpe:        {q1.get('sharpe',0):.3f}")
        print(f"    Q1 Max DD:        {(q1.get('max_dd',0)*100):.2f}%")
        print(f"    Q5 CAGR:          {(q5.get('cagr',0)*100):.2f}%")
        print(f"    Q1-Q5 Spread:     {(q1.get('q1q5_spread_cagr',0)*100):.2f}%")
        print(f"    IC Mean:          {q1.get('ic_mean',0):.4f}")
        print(f"    ICIR:             {q1.get('icir',0):.3f}")
        print(f"    Obsidian Score:   {obs.get('total',0):.2f}/5.0  {obs.get('rating','')}")
        print(f"    Periods:          {result.get('n_periods')}")
        print(f"    GPU used:         {result.get('gpu_used')}")
        print(f"    Elapsed:          {result.get('elapsed_s')}s")
    elif mode == "topn":
        pm = result.get("portfolio_metrics", {})
        bm = result.get("bm_metrics", {})
        print(f"\n  TOP-N RESULTS:")
        print(f"    Portfolio CAGR:   {(pm.get('cagr',0)*100):.2f}%")
        print(f"    Benchmark CAGR:   {(bm.get('cagr',0)*100):.2f}%")
        print(f"    Alpha:            {(pm.get('alpha',0)*100):.2f}%")
        print(f"    Sharpe:           {pm.get('sharpe',0):.3f}")
        print(f"    Max DD:           {(pm.get('max_dd',0)*100):.2f}%")
        print(f"    Info Ratio:       {pm.get('info_ratio',0):.3f}")
        print(f"    Up Capture:       {(pm.get('up_capture',0)*100):.1f}%")
        print(f"    Elapsed:          {result.get('elapsed_s')}s")

# ── Run 1: Quintile (small date range for speed) ──────────────────────────
run({
    "model_type": "quintile",
    "factor": "value",
    "index": "OBQ Investable Universe (Top 3000)",
    "start_date": "2010-01-31",
    "end_date": "2015-12-31",
    "rebalance": "Monthly",
    "n_quintiles": 5,
    "min_price": 2.0,
    "na_handling": "Exclude",
    "winsorize": True,
    "sector_neutral": False,
    "commission_bps": 5,
    "slippage_bps": 10,
    "rf_annual": 0.04,
    "initial_capital": 1000000,
}, "DRY RUN 1: QUINTILE / VALUE / 2010-2015")

# ── Run 2: Top-N ──────────────────────────────────────────────────────────
run({
    "model_type": "topn",
    "factor": "jcn",
    "index": "OBQ Investable Universe (Top 3000)",
    "start_date": "2015-01-31",
    "end_date": "2020-12-31",
    "rebalance": "Monthly",
    "top_n": 30,
    "position_sizing": "Equal",
    "direction": "Long Only",
    "min_price": 2.0,
    "na_handling": "Exclude",
    "winsorize": True,
    "commission_bps": 5,
    "slippage_bps": 10,
    "rf_annual": 0.04,
    "initial_capital": 1000000,
}, "DRY RUN 2: TOP-30 / JCN COMPOSITE / 2015-2020")

print("\nDRY RUNS COMPLETE")
