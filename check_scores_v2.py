"""Check score availability — simplified direct query."""
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
    "jcn_quality_momentum":       "JCN Quality-Momentum",
    "jcn_value_momentum":         "JCN Value-Momentum",
    "jcn_growth_quality_momentum":"JCN Growth-Quality-Momentum",
    "jcn_fortress":               "JCN Fortress",
}

print(f"{'SCORE':<34} {'SEMI-ANN':>9} {'FIRST':>12} {'LAST':>12} {'AVG_N':>8}")
print("-" * 80)

for col, name in SCORES.items():
    try:
        r = con.execute(f"""
            SELECT 
                COUNT(DISTINCT month_date) as n_dates,
                MIN(month_date)::VARCHAR as min_d,
                MAX(month_date)::VARCHAR as max_d,
                ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT month_date),0), 0) as avg_n
            FROM v_backtest_scores
            WHERE {col} IS NOT NULL
              AND MONTH(month_date) IN (6, 12)
        """).fetchone()
        print(f"  {name:<32} {r[0]:>9} {(r[1] or '')[:10]:>12} {(r[2] or '')[:10]:>12} {int(r[3] or 0):>8}")
    except Exception as e:
        print(f"  {name:<32} ERROR: {str(e)[:50]}")

print(f"\nTop-1000 mktcap threshold (2024-06-28, price>=5):")
r2 = con.execute("""
    SELECT MIN(market_cap)/1e9, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY market_cap)/1e9
    FROM (
        SELECT market_cap FROM v_backtest_prices
        WHERE price_date = '2024-06-28' AND market_cap > 0 AND adjusted_close >= 5
        ORDER BY market_cap DESC LIMIT 1000
    )
""").fetchone()
print(f"  Cutoff (rank #1000): ${r2[0]:.1f}B  |  Median top-1000: ${r2[1]:.1f}B")
print(f"\n  Note: 'large' cap tier = $10B-$200B. Top-1000 ≈ $17B+ cutoff.")
print(f"  Use cap_tier='large' + 'mega' OR implement explicit rank-based filter.")

con.close()
