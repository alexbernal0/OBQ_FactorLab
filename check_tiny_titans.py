"""Check if we have data to run Tiny Titans strategy."""
import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

print("=== TINY TITANS DATA CHECK ===")
print("Universe: $25M-$200M mktcap, P/S < 1.0, top momentum")
print()

# Check how many stocks fall in $25M-$200M range per period
r = con.execute("""
    SELECT
        YEAR(price_date) as yr,
        COUNT(DISTINCT symbol) as n_stocks,
        AVG(market_cap)/1e6 as avg_mktcap_M
    FROM v_backtest_prices
    WHERE market_cap BETWEEN 25e6 AND 200e6
      AND adjusted_close >= 1.0
      AND MONTH(price_date) = 12
      AND price_date >= '1990-01-01'
    GROUP BY yr ORDER BY yr
""").fetchall()

print("Stocks in $25M-$200M range (December each year):")
for row in r:
    print(f"  {row[0]}: {row[1]:,} stocks | avg mktcap ${row[2]:.0f}M")

print()
# Check if we have P/S data for micro-caps
r2 = con.execute("""
    SELECT COUNT(DISTINCT s.symbol) as n_with_ps
    FROM v_backtest_scores s
    JOIN v_backtest_prices p ON s.symbol = p.symbol AND p.price_date = s.month_date
    WHERE p.market_cap BETWEEN 25e6 AND 200e6
      AND s.ps_ttm IS NOT NULL
      AND s.month_date >= '1990-07-31'
""").fetchone()
print(f"Micro-cap stocks with P/S data: {r2[0]:,}")

# Check momentum coverage in micro-cap
r3 = con.execute("""
    SELECT COUNT(DISTINCT s.symbol) as n_with_mom
    FROM v_backtest_scores s
    JOIN v_backtest_prices p ON s.symbol = p.symbol AND p.price_date = s.month_date
    WHERE p.market_cap BETWEEN 25e6 AND 200e6
      AND s.momentum_score IS NOT NULL
      AND s.month_date >= '1990-07-31'
""").fetchone()
print(f"Micro-cap stocks with momentum score: {r3[0]:,}")

# O'Shaughnessy also tested "Cornerstone Value" and "Cornerstone Growth"
print()
print("=== RELATED O'SHAUGHNESSY STRATEGIES ===")
print("""
Tiny Titans:       $25M-$200M mktcap | P/S < 1.0 | Top 25 12mo momentum
Cornerstone Value: Large-cap | P/S bottom 20% | High Shareholder Yield + Momentum
Cornerstone Growth: All-cap | P/S < 1.5 | EPS growth > 25% | top momentum
Dogs of Dow:       Dow 30 | Highest dividend yield (top 10)
Value Composite:   P/B + P/E + P/S + P/FCF + EV/EBITDA composite ranking
""")

# Check if $25M stocks exist (O'Shaughnessy used very small stocks)
r4 = con.execute("""
    SELECT COUNT(DISTINCT symbol) as n, MIN(market_cap)/1e6, MAX(market_cap)/1e6
    FROM v_backtest_prices
    WHERE market_cap BETWEEN 10e6 AND 25e6
      AND price_date >= '1990-01-01' AND adjusted_close >= 1.0
""").fetchone()
print(f"Stocks $10M-$25M (below Tiny Titan range): {r4[0]:,}")

con.close()

print("""
=== VERDICT ===
Tiny Titans is a MICRO-CAP strategy ($25M-$200M) — very different universe
from our CYC-001/002 large-cap ($10B+) tests.

Should be a SEPARATE universe test:
  - Tiny Titans ($25M-$200M) = Micro-Cap Universe
  - O'Shaughnessy All-Stocks = Full universe tests
  - Our CYC-001/002 = Large-Cap ($10B+)

This is actually perfect for CYC-003 or CYC-004 where we test
the same factors across DIFFERENT market cap tiers.
""")
