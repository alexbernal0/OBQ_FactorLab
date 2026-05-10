import sys; sys.path.insert(0,'.')
from engine.factor_backtest import FactorBacktestConfig, _get_rebalance_dates, _load_scores, _get_con, SEPARATE_SCORE_TABLES

cfg = FactorBacktestConfig(
    score_column='cyc2_roic', score_direction='higher',
    start_date='2020-01-31', end_date='2022-12-31',
    rebalance_freq='semi-annual', min_market_cap=10_000_000_000.0,
    min_price=5.0, min_adv_usd=1_000_000.0, transaction_cost_bps=15.0
)

print(f"cyc2_roic registered: {'cyc2_roic' in SEPARATE_SCORE_TABLES}")
entry = SEPARATE_SCORE_TABLES.get('cyc2_roic')
print(f"Entry: {entry}")

con = _get_con()
dates = _get_rebalance_dates(con, cfg)
print(f"Dates: {len(dates)} ({dates[0]} to {dates[-1]})")
df = _load_scores(con, cfg, dates, 'cyc2_roic')
print(f"Data: {len(df)} rows, {df.symbol.nunique()} symbols")
print(f"Score sample: {df.score_raw.dropna().head(5).tolist()}")
con.close()
print("ENGINE READY")
