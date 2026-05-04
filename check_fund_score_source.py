import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_model

m = get_model('FM-JCNQAR-20260503-595A')  # QARP — best factor
fitness = m.get('fitness_json', {}) or {}
bm = m.get('bucket_metrics_json', {}) or {}
spy = m.get('spy_metrics_json', {}) or {}
univ = m.get('universe_metrics_json', {}) or {}

print("=== WHAT OBQ FUND SCORE IS BASED ON ===")
print(f"OBQ Fund Score: {m.get('obq_fund_score'):.4f}")
print()
print("Components from fitness_json:")
for k in ['alpha_win_rate','avg_annual_alpha','downside_capture','alpha_sharpe','obq_fund_score']:
    print(f"  {k}: {fitness.get(k)}")
print()
print("Q1 bucket metrics (what Q1 actually achieved):")
q1 = bm.get('1', {}) or {}
print(f"  Q1 CAGR:    {(q1.get('cagr') or 0)*100:.2f}%")
print(f"  Q1 Sharpe:  {q1.get('sharpe'):.3f}" if q1.get('sharpe') else "  Q1 Sharpe: N/A")
print(f"  Q1 Max DD:  {(q1.get('max_dd') or 0)*100:.1f}%")
print()
print("Universe benchmark (what alpha_win_rate is measured against):")
print(f"  Universe CAGR:   {(univ.get('cagr') or 0)*100:.2f}%")
print(f"  Universe Sharpe: {univ.get('sharpe'):.3f}" if univ.get('sharpe') else "  N/A")
print()
print("SPY (what we SHOULD be measuring against):")
print(f"  SPY CAGR:   {(spy.get('cagr') or 0)*100:.2f}%")
print(f"  SPY Sharpe: {spy.get('sharpe'):.3f}" if spy.get('sharpe') else "  N/A")
print()
print("=== THE GAP ===")
q1_cagr = (q1.get('cagr') or 0)
univ_cagr = (univ.get('cagr') or 0)
spy_cagr = (spy.get('cagr') or 0)
print(f"Alpha vs Universe: {(q1_cagr - univ_cagr)*100:.2f}%/yr")
print(f"Alpha vs SPY:      {(q1_cagr - spy_cagr)*100:.2f}%/yr")
print(f"Universe CAGR was: {univ_cagr*100:.2f}% (the bad benchmark)")
print(f"SPY CAGR was:      {spy_cagr*100:.2f}% (the real benchmark)")
