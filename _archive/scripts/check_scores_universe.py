"""
Check all available JCN scores + top-1000 mktcap universe coverage.
"""
import sys; sys.path.insert(0,'.')
import duckdb, os, numpy as np
from dotenv import load_dotenv; load_dotenv()

db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

# All score columns we care about
SCORES = {
    # Individual factors
    "value_score":           "Value Score",
    "quality_score":         "Quality Score",
    "growth_score":          "Growth Score",
    "finstr_score":          "Financial Strength",
    "momentum_score":        "Momentum Score",
    "momentum_af_score":     "Momentum (Alpha Factor)",
    "momentum_fip_score":    "Momentum (FIP)",
    "moat_score":            "Moat Score",
    # JCN blend composites
    "jcn_full_composite":         "JCN Composite",
    "jcn_qarp":                   "JCN QARP",
    "jcn_garp":                   "JCN GARP",
    "jcn_quality_momentum":       "JCN Quality-Momentum",
    "jcn_value_momentum":         "JCN Value-Momentum",
    "jcn_growth_quality_momentum":"JCN Growth-Quality-Momentum",
    "jcn_fortress":               "JCN Fortress",
}

print("=" * 70)
print("SCORE AVAILABILITY CHECK (semi-annual dates, top-1000 mktcap universe)")
print("=" * 70)

# Check top-1000 mktcap availability by score + date range
for col, name in SCORES.items():
    try:
        r = con.execute(f"""
            SELECT 
                COUNT(DISTINCT s.month_date) as n_dates,
                MIN(s.month_date)::VARCHAR as min_date,
                MAX(s.month_date)::VARCHAR as max_date,
                AVG(cnt) as avg_symbols_per_period
            FROM (
                SELECT s.month_date, COUNT(*) as cnt
                FROM v_backtest_scores s
                JOIN (
                    SELECT symbol, month_date,
                           ROW_NUMBER() OVER (PARTITION BY month_date ORDER BY market_cap DESC NULLS LAST) as rnk
                    FROM v_backtest_prices
                    WHERE price_date = month_date
                ) p ON s.Symbol = p.symbol AND s.month_date = p.month_date
                WHERE s.{col} IS NOT NULL
                  AND MONTH(s.month_date) IN (6, 12)
                  AND p.rnk <= 1000
                  AND p.market_cap > 0
                GROUP BY s.month_date
            ) t
        """).fetchone()
        print(f"  {col:<35} {r[1]} -> {r[2]}  ({r[0]} semi-ann periods, ~{int(r[3] or 0)} stocks/period)")
    except Exception as e:
        print(f"  {col:<35} ERROR: {e}")

print()
print("=" * 70)
print("TOP-1000 MKTCAP FILTER CHECK (2024-06-30)")
print("=" * 70)
r2 = con.execute("""
    SELECT 
        COUNT(*) as n_top1000,
        MIN(market_cap)/1e9 as min_mktcap_B,
        MAX(market_cap)/1e9 as max_mktcap_B,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY market_cap) / 1e9 as median_mktcap_B
    FROM (
        SELECT symbol, market_cap,
               ROW_NUMBER() OVER (ORDER BY market_cap DESC NULLS LAST) as rnk
        FROM v_backtest_prices
        WHERE price_date = '2024-06-28'
          AND market_cap > 0
          AND adjusted_close >= 5
    )
    WHERE rnk <= 1000
""").fetchone()
print(f"  Top-1000 at 2024-06-28:")
print(f"    Count:          {r2[0]}")
print(f"    Min mktcap:     ${r2[1]:.1f}B")
print(f"    Max mktcap:     ${r2[3]:.0f}B")
print(f"    Median mktcap:  ${r2[2]:.1f}B")

# GPU check
print()
print("=" * 70)
print("GPU AVAILABILITY")
print("=" * 70)
try:
    import cupy as cp
    print(f"  CuPy available: {cp.__version__}")
except:
    print("  CuPy: NOT available")
try:
    import torch
    print(f"  PyTorch CUDA: {torch.cuda.is_available()}, device: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'N/A'}")
except:
    pass
try:
    from engine import gpu
    print(f"  OBQ GPU module: available={gpu.available()}")
except Exception as e:
    print(f"  OBQ GPU module: {e}")

con.close()
print("\nDone.")
