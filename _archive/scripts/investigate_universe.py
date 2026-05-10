# -*- coding: utf-8 -*-
"""
Investigate Tortoriello universe benchmark vs our EW universe vs SPX.
"""
import sys; sys.path.insert(0,'.')
import numpy as np, pandas as pd, duckdb, os
from dotenv import load_dotenv; load_dotenv()
from engine.metrics import compute_all

db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

START = '1990-07-31'
END   = '2024-12-31'
PPY   = 2  # semi-annual

# ── 1. Our EW universe (ALL scored stocks, semi-annual) ───────────────────────
print("=== OUR EW UNIVERSE (all scored stocks, no size filter) ===")
r1 = con.execute(f"""
    SELECT s.month_date::VARCHAR as d, AVG(fwd.fwd_return) as univ_ret
    FROM v_backtest_scores s
    JOIN (
        SELECT t0.symbol, t0.month_start::VARCHAR as entry_month,
               (t1.close_price / NULLIF(t0.close_price,0) - 1) as fwd_return
        FROM (SELECT symbol, DATE_TRUNC('month', price_date) AS month_start,
                     LAST(adjusted_close ORDER BY price_date) as close_price
              FROM v_backtest_prices
              WHERE price_date >= '{START}'::DATE AND price_date <= '{END}'::DATE
              AND adjusted_close IS NOT NULL
              GROUP BY symbol, DATE_TRUNC('month', price_date)) t0
        JOIN (SELECT symbol, DATE_TRUNC('month', price_date) AS month_start,
                     LAST(adjusted_close ORDER BY price_date) as close_price
              FROM v_backtest_prices
              WHERE price_date >= '{START}'::DATE AND price_date <= '{END}'::DATE
              AND adjusted_close IS NOT NULL
              GROUP BY symbol, DATE_TRUNC('month', price_date)) t1
        ON t0.symbol = t1.symbol
        AND t1.month_start = t0.month_start + INTERVAL '6 months'
    ) fwd ON s.Symbol = fwd.symbol
    AND DATE_TRUNC('month', s.month_date::DATE)::VARCHAR = fwd.entry_month
    WHERE s.month_date >= '{START}'::DATE AND s.month_date <= '{END}'::DATE
    AND MONTH(s.month_date) IN (6,12)
    AND s.jcn_full_composite IS NOT NULL
    AND fwd.fwd_return BETWEEN -0.95 AND 3.0
    GROUP BY s.month_date ORDER BY d
""").fetchdf()
print(f"Periods: {len(r1)}")
if len(r1) > 2:
    rets = r1['univ_ret'].values
    eq   = np.concatenate([[1.0], np.cumprod(1 + rets)])
    m = compute_all(equity=eq, monthly_ret=rets, periods_per_year=PPY, label="OBQ EW All Stocks")
    print(f"  CAGR: {m['cagr']*100:.2f}%  Sharpe: {m['sharpe']:.3f}  MaxDD: {m['max_dd']*100:.1f}%")

# ── 2. Large-cap only EW universe ($10B+) ────────────────────────────────────
print("\n=== LARGE-CAP EW UNIVERSE ($10B+ only — what we actually test against) ===")
r2 = con.execute(f"""
    SELECT s.month_date::VARCHAR as d, AVG(fwd.fwd_return) as univ_ret
    FROM v_backtest_scores s
    JOIN (
        SELECT t0.symbol, t0.month_start::VARCHAR as entry_month,
               (t1.close_price / NULLIF(t0.close_price,0) - 1) as fwd_return
        FROM (SELECT symbol, DATE_TRUNC('month', price_date) AS month_start,
                     LAST(adjusted_close ORDER BY price_date) as close_price,
                     LAST(market_cap ORDER BY price_date) as mktcap
              FROM v_backtest_prices
              WHERE price_date >= '{START}'::DATE AND price_date <= '{END}'::DATE
              AND adjusted_close IS NOT NULL AND adjusted_close >= 5
              GROUP BY symbol, DATE_TRUNC('month', price_date)) t0
        JOIN (SELECT symbol, DATE_TRUNC('month', price_date) AS month_start,
                     LAST(adjusted_close ORDER BY price_date) as close_price
              FROM v_backtest_prices
              WHERE price_date >= '{START}'::DATE AND price_date <= '{END}'::DATE
              AND adjusted_close IS NOT NULL
              GROUP BY symbol, DATE_TRUNC('month', price_date)) t1
        ON t0.symbol = t1.symbol
        AND t1.month_start = t0.month_start + INTERVAL '6 months'
        WHERE t0.mktcap >= 10000000000
    ) fwd ON s.Symbol = fwd.symbol
    AND DATE_TRUNC('month', s.month_date::DATE)::VARCHAR = fwd.entry_month
    WHERE s.month_date >= '{START}'::DATE AND s.month_date <= '{END}'::DATE
    AND MONTH(s.month_date) IN (6,12)
    AND s.jcn_full_composite IS NOT NULL
    AND fwd.fwd_return BETWEEN -0.95 AND 3.0
    GROUP BY s.month_date ORDER BY d
""").fetchdf()
print(f"Periods: {len(r2)}")
if len(r2) > 2:
    rets2 = r2['univ_ret'].values
    eq2   = np.concatenate([[1.0], np.cumprod(1 + rets2)])
    m2 = compute_all(equity=eq2, monthly_ret=rets2, periods_per_year=PPY, label="OBQ EW Large-Cap")
    print(f"  CAGR: {m2['cagr']*100:.2f}%  Sharpe: {m2['sharpe']:.3f}  MaxDD: {m2['max_dd']*100:.1f}%")

con.close()

# ── 3. SPX for comparison ────────────────────────────────────────────────────
print("\n=== SPX TOTAL RETURN (our benchmark) ===")
from engine.spy_backtest import run_spy_backtest
spy = run_spy_backtest(start_date=START, end_date=END)
sm = spy['portfolio_metrics']
print(f"  CAGR: {sm['cagr']*100:.2f}%  Sharpe: {sm['sharpe']:.3f}  MaxDD: {sm['max_dd']*100:.1f}%")

print("""
=== SUMMARY ===
- OBQ EW All Stocks: inflated CAGR (micro-cap included) — NOT a valid benchmark
- OBQ EW Large-Cap ($10B+): should be close to cap-weight S&P 500
- SPX Total Return: the proper benchmark for reporting alpha
- Tortoriello Universe: S&P 1500 EW (would return ~12-13%/yr historically)

RECOMMENDATION:
  For 'Q1 vs Universe' comparison in Tortoriello tables:
  → Use OBQ EW Large-Cap (our actual investable universe for these backtests)
  
  For fund-level performance reporting:
  → Use SPX Total Return (honest benchmark investors care about)
  
  The Tortoriello 'excess return' column in tearsheet should be labeled
  'Excess vs Investable Universe' not 'vs EW All Stocks'
""")
