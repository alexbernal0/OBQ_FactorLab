import sys, time; sys.path.insert(0,'.')
from engine.factor_backtest import run_combo_backtest, FactorBacktestConfig
from engine.portfolio_backtest import run_combo_portfolio_backtest, PortfolioBacktestConfig

COMMON_F = dict(
    start_date='2015-01-31', end_date='2022-12-31',
    n_buckets=5, hold_months=6, rebalance_freq='semi-annual',
    cap_tier='all', min_price=5.0, min_adv_usd=1_000_000.0,
    min_market_cap=10_000_000_000.0, transaction_cost_bps=15.0,
)
COMMON_P = {k: v for k, v in COMMON_F.items() if k not in ('n_buckets','hold_months')}
COMMON_P.update(top_n=20, sector_max=5)

def cb(m): print(f"  {m}")

print("=== COMBO FACTOR TEST: T1 (EV/EBITDA + Cash ROIC) ===")
t0 = time.time()
r = run_combo_backtest(
    combo_id="T1", factor_a_id="cyc2_ev_ebitda", factor_b_id="cyc2_cash_roc",
    display_name="EV/EBITDA + Cash ROIC", source="Tortoriello #1",
    cfg_override=FactorBacktestConfig(**COMMON_F), cb=cb
)
if r.get('status') == 'complete':
    fm = r['factor_metrics']
    print(f"FACTOR OK: ICIR={fm.get('icir',0):.3f} Spread={fm.get('quintile_spread_cagr',0)*100:.2f}% ({round(time.time()-t0)}s)")
else:
    print(f"FACTOR ERR: {r.get('error')}")

print("\n=== COMBO PORTFOLIO TEST: O1 (P/S + 12M Momentum) ===")
t0 = time.time()
base_p = PortfolioBacktestConfig(
    start_date=COMMON_F['start_date'], end_date=COMMON_F['end_date'],
    rebalance_freq=COMMON_F['rebalance_freq'],
    min_price=COMMON_F['min_price'], min_adv_usd=COMMON_F['min_adv_usd'],
    min_market_cap=COMMON_F['min_market_cap'], transaction_cost_bps=COMMON_F['transaction_cost_bps'],
    cap_tier=COMMON_F['cap_tier'], top_n=20, sector_max=5,
    score_column='cyc2_ps'
)
r2 = run_combo_portfolio_backtest(
    combo_id="O1", factor_a_id="cyc2_ps", factor_b_id="cyc2_mom_12m",
    display_name="P/S + 12M Momentum", source="O'Shaughnessy #1",
    cfg=base_p, cb=cb
)
if r2.get('status') == 'complete':
    pm = r2['portfolio_metrics']
    print(f"PORTFOLIO OK: CAGR={pm.get('cagr',0)*100:.2f}% Sharpe={pm.get('sharpe',0):.3f} ({round(time.time()-t0)}s)")
else:
    print(f"PORTFOLIO ERR: {r2.get('error')}")
