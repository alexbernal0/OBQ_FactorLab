"""Check fundamentals coverage."""
import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()
db = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')
con = duckdb.connect(db, read_only=True)

r = con.execute("""
    SELECT MIN(pit_date)::VARCHAR, MAX(pit_date)::VARCHAR,
           COUNT(DISTINCT symbol) as n_symbols, COUNT(*) as n_rows
    FROM v_backtest_filings WHERE pit_date IS NOT NULL
""").fetchone()
print(f"v_backtest_filings: {r[0][:10]} to {r[1][:10]} | {r[2]:,} symbols | {r[3]:,} rows")

# Check which columns have good coverage
cols_to_check = ['revenue','gross_profit','operating_income','ebitda','net_income','fcf',
                 'total_assets','total_liabilities','equity','net_debt','enterprise_value',
                 'pe_ratio','price_book_mrq','price_sales_ttm','enterprise_value_ebitda',
                 'roe','roa','operating_margin','profit_margin',
                 'earnings_growth_yoy','revenue_growth_yoy']
print("\nColumn coverage (non-null %):")
for col in cols_to_check:
    try:
        r2 = con.execute(f"SELECT COUNT(*) as n, COUNT({col}) as nn FROM v_backtest_filings").fetchone()
        pct = r2[1]/r2[0]*100 if r2[0] > 0 else 0
        print(f"  {col:<40} {pct:.1f}%  ({r2[1]:,} non-null)")
    except: print(f"  {col:<40} ERROR")

# What's missing that we need to compute
print("\n=== WHAT WE NEED TO DERIVE (not directly available) ===")
missing = [
    ("Interest Coverage",    "operating_income / interest_expense",      "interest_expense NOT in filings"),
    ("ROIC",                 "EBIT*(1-t) / InvestedCapital",             "tax_rate and invested_capital not direct"),
    ("Cash ROIC",            "fcf / invested_capital",                   "invested_capital not direct"),
    ("Debt/Equity",          "total_liabilities / equity",               "CAN COMPUTE from filings"),
    ("Current Ratio",        "current_assets / current_liabilities",     "current_assets/liabilities not in filings"),
    ("Gross Margin",         "gross_profit / revenue",                   "CAN COMPUTE from filings"),
    ("FCF Margin",           "fcf / revenue",                            "CAN COMPUTE from filings"),
    ("FCF Yield",            "fcf / market_cap",                         "CAN COMPUTE from filings"),
    ("EV/Sales",             "enterprise_value / revenue",               "CAN COMPUTE from filings"),
    ("Net Debt/EBITDA",      "net_debt / ebitda",                        "CAN COMPUTE from filings"),
    ("Accruals",             "(net_income - fcf) / total_assets",        "CAN COMPUTE from filings"),
    ("Revenue/Assets",       "revenue / total_assets",                   "CAN COMPUTE (asset turnover)"),
    ("Equity/Assets",        "equity / total_assets",                    "CAN COMPUTE (leverage)"),
    ("EPS Growth",           "earnings_growth_yoy",                      "ALREADY IN filings"),
    ("Buyback Yield",        "net_repurchases / market_cap",             "repurchases NOT in filings"),
    ("Shareholder Yield",    "div + buyback / market_cap",               "dividends NOT in filings"),
    ("Dividend Yield",       "dividends / market_cap",                   "dividends NOT in filings"),
    ("Earnings Surprise",    "actual - consensus EPS",                   "analyst cols in filings"),
    ("Estimate Revisions",   "consensus EPS change trend",               "analyst cols in filings"),
]
print(f"{'FACTOR':<25} {'FORMULA':<40} {'STATUS'}")
print("-"*100)
for name, formula, status in missing:
    can = "CAN COMPUTE" if "CAN COMPUTE" in status else ("IN FILINGS" if "IN filings" in status or "ALREADY" in status else "NEED MORE DATA")
    print(f"  {name:<23} {formula:<40} {can}")

con.close()
