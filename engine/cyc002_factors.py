"""
engine/cyc002_factors.py
========================
CYC-002 Factor Definitions â€” mapping every new factor to its source table,
column, and direction. Used by the batch runner.

All factors are point-in-time from PROD_OBQ_* tables computed monthly.
"""

# â”€â”€ Factor registry â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Each entry: (score_column, source_table, score_col_in_table, direction, display_name, group)
# score_column = the ID we'll use in backtesting engine
# source_table = where the raw signal lives
# score_col_in_table = exact column name in that table
# direction = higher_better | lower_better

CYC002_FACTORS = [

    # â”€â”€ GROUP A: PROFITABILITY â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("cyc2_roic",           "PROD_OBQ_Quality_Scores",   "roic",             "higher_better", "ROIC",                          "A_Profitability"),
    ("cyc2_cash_roc",       "PROD_LONGEQ_SCORES",        "cash_roc",         "higher_better", "Cash ROIC",                     "A_Profitability"),
    ("cyc2_roce",           "PROD_FUNDSMITH_SCORES",     "roce",             "higher_better", "ROCE (Fundsmith)",              "A_Profitability"),
    ("cyc2_roa",            "PROD_OBQ_Quality_Scores",   "roa",              "higher_better", "ROA",                           "A_Profitability"),
    ("cyc2_gpa",            "PROD_OBQ_Quality_Scores",   "gpa",              "higher_better", "GPA (Novy-Marx)",               "A_Profitability"),
    ("cyc2_op_margin",      "PROD_OBQ_Quality_Scores",   "op_margin",        "higher_better", "Operating Margin",              "A_Profitability"),
    ("cyc2_fcf_margin",     "PROD_OBQ_Quality_Scores",   "fcf_margin",       "higher_better", "FCF Margin",                    "A_Profitability"),
    ("cyc2_gross_margin",   "PROD_OBQ_Quality_Scores",   "gross_margin",     "higher_better", "Gross Margin",                  "A_Profitability"),
    ("cyc2_earn_quality",   "PROD_OBQ_Quality_Scores",   "earnings_quality", "higher_better", "Earnings Quality",              "A_Profitability"),

    # â”€â”€ GROUP B: FINANCIAL STRENGTH â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("cyc2_int_cov",        "PROD_OBQ_FinStr_Scores",    "interest_coverage","higher_better", "Interest Coverage",             "B_FinancialStrength"),
    ("cyc2_fcf_debt",       "PROD_OBQ_FinStr_Scores",    "fcf_debt",         "higher_better", "FCF / Debt",                    "B_FinancialStrength"),
    ("cyc2_nd_ebitda",      "PROD_OBQ_FinStr_Scores",    "net_debt_ebitda",  "lower_better",  "Net Debt / EBITDA",             "B_FinancialStrength"),
    ("cyc2_debt_assets",    "PROD_OBQ_FinStr_Scores",    "debt_assets",      "lower_better",  "Debt / Assets",                 "B_FinancialStrength"),
    ("cyc2_cash_assets",    "PROD_OBQ_FinStr_Scores",    "cash_assets",      "higher_better", "Cash / Assets",                 "B_FinancialStrength"),
    ("cyc2_int_pct_op",     "PROD_LONGEQ_SCORES",        "interest_pct_op",  "lower_better",  "Interest % of Op Income",       "B_FinancialStrength"),
    ("cyc2_capex_ocf",      "PROD_LONGEQ_SCORES",        "capex_pct_ocf",    "lower_better",  "CapEx % of OCF",                "B_FinancialStrength"),
    ("cyc2_share_chg",      "PROD_LONGEQ_SCORES",        "sharecount_chg_5yr","lower_better", "Share Count Change 5yr",        "B_FinancialStrength"),
    ("cyc2_cash_conv",      "PROD_FUNDSMITH_SCORES",     "cash_conversion",  "higher_better", "Cash Conversion (Fundsmith)",   "B_FinancialStrength"),
    ("cyc2_nd_ebit",        "PROD_FUNDSMITH_SCORES",     "net_debt_ebit",    "lower_better",  "Net Debt / EBIT (Fundsmith)",   "B_FinancialStrength"),

    # â”€â”€ GROUP C: VALUATION â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("cyc2_ev_ebitda",      "PROD_OBQ_Value_Scores",     "ev_ebitda_ttm",    "lower_better",  "EV/EBITDA",                     "C_Valuation"),
    ("cyc2_pfcf",           "PROD_OBQ_Value_Scores",     "pfcf_ttm",         "lower_better",  "P/FCF",                         "C_Valuation"),
    ("cyc2_fcf_yield",      "PROD_LONGEQ_SCORES",        "fcf_yield",        "higher_better", "FCF Yield",                     "C_Valuation"),
    ("cyc2_ps",             "PROD_OBQ_Value_Scores",     "ps_ttm",           "lower_better",  "P/Sales",                       "C_Valuation"),
    ("cyc2_pb",             "PROD_OBQ_Value_Scores",     "pb_mrq",           "lower_better",  "P/Book",                        "C_Valuation"),
    ("cyc2_pe",             "PROD_OBQ_Value_Scores",     "pe_ttm",           "lower_better",  "P/Earnings",                    "C_Valuation"),
    # ev_sales computed in factor_backtest via v_backtest_filings join â€” special case
    ("cyc2_rev_growth_3y",  "PROD_FUNDSMITH_SCORES",     "revenue_growth_3yr","higher_better","Revenue Growth 3yr",            "C_Valuation"),

    # â”€â”€ GROUP D: GROWTH â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("cyc2_rev_cagr_1y",    "PROD_OBQ_Growth_Scores",    "revenue_ps_cagr_1y","higher_better","Revenue/Shr CAGR 1yr",          "D_Growth"),
    ("cyc2_rev_cagr_3y",    "PROD_OBQ_Growth_Scores",    "revenue_ps_cagr_3y","higher_better","Revenue/Shr CAGR 3yr",          "D_Growth"),
    ("cyc2_rev_cagr_5y",    "PROD_OBQ_Growth_Scores",    "revenue_ps_cagr_5y","higher_better","Revenue/Shr CAGR 5yr",          "D_Growth"),
    ("cyc2_eps_cagr_1y",    "PROD_OBQ_Growth_Scores",    "eps_cagr_1y",      "higher_better", "EPS CAGR 1yr",                  "D_Growth"),
    ("cyc2_eps_cagr_3y",    "PROD_OBQ_Growth_Scores",    "eps_cagr_3y",      "higher_better", "EPS CAGR 3yr",                  "D_Growth"),
    ("cyc2_eps_cagr_5y",    "PROD_OBQ_Growth_Scores",    "eps_cagr_5y",      "higher_better", "EPS CAGR 5yr",                  "D_Growth"),
    ("cyc2_fcf_cagr_3y",    "PROD_OBQ_Growth_Scores",    "fcf_ps_cagr_3y",   "higher_better", "FCF/Shr CAGR 3yr",              "D_Growth"),
    ("cyc2_fcf_cagr_5y",    "PROD_LONGEQ_SCORES",        "fcf_cagr_5yr",     "higher_better", "FCF CAGR 5yr (LongEQ)",         "D_Growth"),

    # â”€â”€ GROUP E: MOMENTUM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("cyc2_mom_3m",         "PROD_OBQ_Momentum_Scores",  "af_r3m",           "higher_better", "3-Month Momentum",              "E_Momentum"),
    ("cyc2_mom_6m",         "PROD_OBQ_Momentum_Scores",  "af_r6m",           "higher_better", "6-Month Momentum",              "E_Momentum"),
    ("cyc2_mom_12m",        "PROD_OBQ_Momentum_Scores",  "af_r12m",          "higher_better", "12-Month Momentum (12-1)",      "E_Momentum"),
    ("cyc2_fip_6m",         "PROD_OBQ_Momentum_Scores",  "fip_6m",           "higher_better", "FIP 6-Month (smooth path)",     "E_Momentum"),
    ("cyc2_fip_12m",        "PROD_OBQ_Momentum_Scores",  "fip_12m",          "higher_better", "FIP 12-Month (Gray/Vogel)",     "E_Momentum"),
    ("cyc2_sys_score",      "PROD_OBQ_Momentum_Scores",  "systemscore",      "higher_better", "Systematic Score (RÂ²)",         "E_Momentum"),

    # â”€â”€ GROUP F: CAPITAL ALLOCATION â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("cyc2_rd_ratio",       "PROD_MOAT_SCORES",          "rd_ratio",         "higher_better", "R&D Ratio (Innovation)",        "F_Capital"),

    # â”€â”€ GROUP G: MOAT COMPONENTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("cyc2_moat_intangible","PROD_MOAT_SCORES",          "score_intangible", "higher_better", "Intangible Assets Moat",        "G_Moat"),
    ("cyc2_moat_switching", "PROD_MOAT_SCORES",          "score_switching",  "higher_better", "Switching Cost Moat",           "G_Moat"),
    ("cyc2_moat_network",   "PROD_MOAT_SCORES",          "score_network",    "higher_better", "Network Effect Moat",           "G_Moat"),
    ("cyc2_moat_cost",      "PROD_MOAT_SCORES",          "score_cost",       "higher_better", "Cost Advantage Moat",           "G_Moat"),
    ("cyc2_moat_scale",     "PROD_MOAT_SCORES",          "score_scale",      "higher_better", "Efficient Scale Moat",          "G_Moat"),
]

# â”€â”€ Two-Factor Combo Definitions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Each entry: (combo_id, factor_a_id, factor_b_id, display_name, source)
CYC002_COMBOS = [
    # Tortoriello combos
    ("T1", "cyc2_ev_ebitda",   "cyc2_cash_roc",    "EV/EBITDA + Cash ROIC",            "Tortoriello #1"),
    ("T2", "cyc2_ev_ebitda",   "cyc2_roic",        "EV/EBITDA + ROIC",                 "Tortoriello #2"),
    ("T3", "cyc2_pfcf",        "cyc2_roic",        "P/FCF + ROIC",                     "Tortoriello"),
    ("T4", "cyc2_ps",          "cyc2_fcf_margin",  "P/S + FCF Margin",                 "Tortoriello/O'S bridge"),
    ("T5", "cyc2_ev_ebitda",   "cyc2_int_cov",     "EV/EBITDA + Interest Coverage",    "Tortoriello"),
    ("T6", "cyc2_ev_ebitda",   "cyc2_op_margin",   "EV/EBITDA + Operating Margin",     "Tortoriello"),
    ("T7", "cyc2_fcf_yield",   "cyc2_nd_ebitda",   "FCF Yield + Net Debt/EBITDA",      "Tortoriello"),
    ("T8", "cyc2_ev_ebitda",   "cyc2_earn_quality","EV/EBITDA + Earnings Quality",     "Tortoriello"),
    # O'Shaughnessy combos
    ("O1", "cyc2_ps",          "cyc2_mom_12m",     "P/S + 12M Momentum (Runaways)",    "O'Shaughnessy #1"),
    ("O2", "cyc2_ev_ebitda",   "cyc2_mom_12m",     "EV/EBITDA + 12M Momentum",         "O'Shaughnessy"),
    ("O3", "cyc2_ps",          "cyc2_roa",         "P/S + ROA",                        "O'Shaughnessy"),
    ("O4", "cyc2_fcf_yield",   "cyc2_mom_12m",     "FCF Yield + 12M Momentum",         "O'Shaughnessy"),
    ("O5", "cyc2_ev_ebitda",   "cyc2_fip_12m",     "EV/EBITDA + FIP Momentum",         "O'Shaughnessy + Gray/Vogel"),
    # Academic / extended
    ("L1", "cyc2_roic",        "cyc2_mom_12m",     "ROIC + Momentum",                  "Asness/Moskowitz"),
    ("L2", "cyc2_gpa",         "cyc2_pb",          "GPA + P/B",                        "Novy-Marx (2013)"),
    ("L3", "cyc2_int_cov",     "cyc2_ev_ebitda",   "Interest Coverage + EV/EBITDA",    "Tortoriello extended"),
    ("L4", "cyc2_fcf_margin",  "cyc2_eps_cagr_3y", "FCF Margin + EPS CAGR 3yr",        "Quality compounder"),
    ("L5", "cyc2_op_margin",   "cyc2_rev_cagr_3y", "Operating Margin + Rev Growth 3yr","Growth quality"),
    ("L6", "cyc2_roic",        "cyc2_debt_assets", "ROIC + Debt/Assets",               "Quality + Safety"),
]

# â”€â”€ Source table date column mapping â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Different tables use different date column names
TABLE_DATE_COL = {
    "PROD_OBQ_Quality_Scores":   "month_date",
    "PROD_OBQ_Value_Scores":     "month_date",
    "PROD_OBQ_Growth_Scores":    "month_date",
    "PROD_OBQ_Momentum_Scores":  "month_date",
    "PROD_OBQ_FinStr_Scores":    "month_date",
    "PROD_LONGEQ_SCORES":        "month_date",
    "PROD_FUNDSMITH_SCORES":     "month_date",
    "PROD_MOAT_SCORES":          "month_date",
    "PROD_RULEBREAKER_SCORES":   "month_date",
}

# â”€â”€ Summary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
TOTAL_SINGLES = len(CYC002_FACTORS)
TOTAL_COMBOS  = len(CYC002_COMBOS)
TOTAL_RUNS    = (TOTAL_SINGLES + TOTAL_COMBOS) * 2  # factor + portfolio each

if __name__ == "__main__":
    print(f"CYC-002 Factor Registry")
    print(f"  Singles:  {TOTAL_SINGLES}")
    print(f"  Combos:   {TOTAL_COMBOS}")
    print(f"  Total runs (factor + portfolio): {TOTAL_RUNS}")
    print()
    from collections import Counter
    groups = Counter(f[5] for f in CYC002_FACTORS)
    for g, n in sorted(groups.items()):
        print(f"  {g}: {n} factors")

