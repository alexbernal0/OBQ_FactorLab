"""Simplified score availability check."""
import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()

db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

SCORES = {
    "value_score":                "Value Score",
    "quality_score":              "Quality Score",
    "growth_score":               "Growth Score",
    "finstr_score":               "Financial Strength",
    "momentum_score":             "Momentum Score",
    "momentum_af_score":          "Momentum AF",
    "momentum_fip_score":         "Momentum FIP",
    "moat_score":                 "Moat Score",
    "jcn_full_composite":         "JCN Composite",
    "jcn_qarp":                   "JCN QARP",
    "jcn_garp":                   "JCN GARP",
    "jcn_quality_momentum":       "JCN Quality-Mom",
    "jcn_value_momentum":         "JCN Value-Mom",
    "jcn_growth_quality_momentum":"JCN GQM",
    "jcn_fortress":               "JCN Fortress",
}

# Top-1000 mktcap per date — precompute as temp table
con.execute("""
    CREATE OR REPLACE TEMP TABLE top1000_dates AS
    SELECT symbol, price_date as month_date, market_cap, adjusted_close
    FROM (
        SELECT symbol, price_date, market_cap, adjusted_close,
               ROW_NUMBER() OVER (PARTITION BY price_date ORDER BY market_cap DESC NULLS LAST) as rnk
        FROM v_backtest_prices
        WHERE MONTH(price_date) IN (6, 12)
          AND market_cap > 0
          AND adjusted_close >= 5
    )
    WHERE rnk <= 1000
""")
print("Temp table built.")

print("\n{:<35} {:>6} {:>12} {:>12} {:>8}".format("SCORE", "DATES", "FIRST", "LAST", "AVG/PER"))
print("-" * 80)
for col, name in SCORES.items():
    try:
        r = con.execute(f"""
            SELECT 
                COUNT(DISTINCT s.month_date) as n_dates,
                MIN(s.month_date)::VARCHAR as min_d,
                MAX(s.month_date)::VARCHAR as max_d,
                ROUND(AVG(cnt),0) as avg_n
            FROM (
                SELECT s.month_date, COUNT(*) as cnt
                FROM v_backtest_scores s
                INNER JOIN top1000_dates t ON s.Symbol = t.symbol AND s.month_date = t.month_date
                WHERE s.{col} IS NOT NULL
                GROUP BY s.month_date
            ) x
        """).fetchone()
        n_dates, min_d, max_d, avg_n = r
        print(f"  {name:<33} {n_dates:>6} {(min_d or '')[:10]:>12} {(max_d or '')[:10]:>12} {int(avg_n or 0):>8}")
    except Exception as e:
        print(f"  {name:<33} ERROR: {str(e)[:40]}")

print("\nTop-1000 mktcap threshold at 2024-06-28:")
r2 = con.execute("""
    SELECT 
        MIN(market_cap)/1e9 as min_b,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY market_cap)/1e9 as med_b,
        MAX(market_cap)/1e9 as max_b
    FROM top1000_dates
    WHERE month_date = '2024-06-28'
""").fetchone()
print(f"  Min: ${r2[0]:.1f}B  |  Median: ${r2[1]:.1f}B  |  Max: ${r2[2]:.0f}B")
con.close()
