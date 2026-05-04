# -*- coding: utf-8 -*-
import sys; sys.path.insert(0,'.')
from engine.strategy_bank import get_all_models
from engine.portfolio_bank import get_all_portfolio_models

factor_models = get_all_models(limit=100)
port_models   = get_all_portfolio_models(limit=100)

print(f"Factor bank:    {len(factor_models)} models")
print(f"Portfolio bank: {len(port_models)} models")

print("\n=== FACTOR MODELS (sorted by ICIR) ===")
print(f"{'SID':<30} {'ICIR':>6} {'SPREAD':>8} {'Q1 CAGR':>8} {'LABEL'}")
print("-"*90)
for m in sorted(factor_models, key=lambda x: x.get('icir') or 0, reverse=True):
    label = (m.get('run_label') or '')[:45]
    print(f"  {m['strategy_id']:<28} {(m.get('icir') or 0):>6.3f} {(m.get('quintile_spread_cagr') or 0)*100:>7.2f}% {(m.get('q1_cagr') or 0)*100:>7.2f}%  {label}")

print("\n=== PORTFOLIO MODELS (sorted by Sharpe) ===")
print(f"{'SID':<30} {'CAGR':>7} {'SHARPE':>7} {'MAX DD':>8} {'LABEL'}")
print("-"*90)
for m in sorted(port_models, key=lambda x: x.get('sharpe') or 0, reverse=True):
    label = (m.get('run_label') or '')[:45]
    print(f"  {m['strategy_id']:<28} {(m.get('cagr') or 0)*100:>6.2f}% {(m.get('sharpe') or 0):>7.3f} {(m.get('max_dd') or 0)*100:>7.1f}%  {label}")
