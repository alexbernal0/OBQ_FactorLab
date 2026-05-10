# -*- coding: utf-8 -*-
"""
Encyclopedia v2 Chapter Renderer.

Takes a factor data bundle and renders a markdown chapter with:
  1. Factor header + universe/period context
  2. Cross-tier comparison table (R3K vs Large vs Mid as PEERS, not large-cap default)
  3. Per-bucket performance (Q1 / Q5 / Spread) per tier
  4. Sector attribution matrix (sector × tier × bucket)
  5. Bear / Bull regime decomposition
  6. Bucket-level Tortoriello stats (per cap tier, top tier highlighted)
  7. Interpretation (data-driven, no hand-wavy narrative)

OUTPUT: pure markdown string. Caller decides if it goes to stdout or a file.
"""
from __future__ import annotations
from typing import Optional


def _pct(v, d=2, sign=False):
    if v is None:
        return "—"
    s = f"{v*100:.{d}f}%"
    return ("+" + s) if sign and v >= 0 else s


def _num(v, d=2):
    if v is None:
        return "—"
    return f"{v:.{d}f}"


def _int(v):
    if v is None:
        return "—"
    return f"{int(v):,}"


def _fmt_cell(value, fmt: str) -> str:
    if value is None:
        return "—"
    if fmt == 'pct':   return _pct(value, 2)
    if fmt == 'pct3':  return _pct(value, 3)
    if fmt == 'num2':  return _num(value, 2)
    if fmt == 'num3':  return _num(value, 3)
    if fmt == 'num4':  return _num(value, 4)
    if fmt == 'int':   return _int(value)
    return str(value)


# Pretty display name for known factors
DISPLAY_NAMES = {
    'jcn_full_composite':   'JCN Full Composite',
    'jcn_qarp':             'JCN QARP (Quality at Reasonable Price)',
    'jcn_garp':             'JCN GARP (Growth at Reasonable Price)',
    'jcn_quality_momentum': 'JCN Quality-Momentum',
    'jcn_value_momentum':   'JCN Value-Momentum',
    'jcn_growth_quality_momentum': 'JCN GQM (Growth-Quality-Momentum)',
    'jcn_fortress':         'JCN Fortress',
    'jcn_alpha_trifecta':   'JCN Alpha Trifecta',
    'quality_score':        'OBQ Quality Score',
    'value_score':          'OBQ Value Score',
    'momentum_score':       'OBQ Momentum Score',
    'growth_score':         'OBQ Growth Score',
    'finstr_score':         'OBQ Financial Strength Score',
    'moat_score':           'Moat Score',
    'cyc2_ps':              'Price-to-Sales',
    'cyc2_pe':              'Price-to-Earnings',
    'cyc2_pb':              'Price-to-Book',
    'cyc2_ev_ebitda':       'EV/EBITDA',
    'cyc2_pfcf':            'Price-to-FCF',
    'cyc2_fcf_yield':       'FCF Yield',
    'cyc2_roic':            'Return on Invested Capital',
    'cyc2_roa':             'Return on Assets',
    'cyc2_roce':            'ROCE (Fundsmith)',
    'cyc2_gpa':             'Gross Profit / Assets (Novy-Marx)',
    'cyc2_op_margin':       'Operating Margin',
    'cyc2_fcf_margin':      'Free Cash Flow Margin',
    'cyc2_mom_12m':         '12-Month Momentum',
    'cyc2_mom_6m':          '6-Month Momentum',
    'cyc2_mom_3m':          '3-Month Momentum',
    # CYC-004 pure factor baselines
    'cyc4_accruals_ratio':       'Accruals Ratio (Sloan)',
    'cyc4_asset_turnover':       'Asset Turnover',
    'cyc4_ebit_assets':          'EBIT / Total Assets (Novy-Marx)',
    'cyc4_ocf_assets':           'Operating Cash Flow / Assets',
    'cyc4_roe':                  'Return on Equity',
    'cyc4_dividend_yield':       'Dividend Yield',
    'cyc4_net_payout_yield':     'Net Payout Yield',
    'cyc4_eps_stability':        'EPS Stability (20Q CoV)',
    'cyc4_sales_stability':      'Sales Stability (20Q CoV)',
    'cyc4_current_ratio':        'Current Ratio',
    'cyc4_fscore':               'Piotroski F-Score',
    'cyc4_realized_vol':         'Realized Volatility (Low-Vol Factor)',
    'cyc4_ebit_ev':              'EBIT / Enterprise Value (Greenblatt)',
    'cyc4_fcf_ev':               'FCF / Enterprise Value',
    'cyc4_sales_ev':             'Sales / Enterprise Value',
    'cyc4_pretax_margin':        'Pretax Profit Margin',
    'cyc4_pretax_margin_dev':    'Pretax Margin vs 5yr Average',
    'cyc4_wc_assets':            'Working Capital / Assets',
    'cyc4_tax_paid_sales':       'Tax Expense / Sales (Earnings Quality)',
    'cyc4_op_leverage':          'Operating Leverage (5yr Smoothed)',
    'cyc4_retained_earnings_ta': 'Retained Earnings / Total Assets',
    'cyc4_shareholder_yield':    'Shareholder Yield (Div + Buyback + Debt Repay)',
    'cyc4_market_beta':          'Market Beta (60m Rolling) — Low-Beta Factor',
    'cyc4_log_market_cap':       'Log Market Cap — Pure Size Factor',
    'cyc4_intangibles_pb':       'Intangibles-Adjusted Price-to-Book (Peters-Taylor)',
    'cyc4_cash_conv_cycle':      'Cash Conversion Cycle (Days)',
    'cyc4_change_ar_assets':     'Change in Accounts Receivable / Assets',
    'cyc4_change_inv_assets':    'Change in Inventory / Assets',
    'cyc4_skip_month_mom':       'Skip-Month Momentum (12m minus 1m)',
    'cyc4_gross_margin':         'Gross Profit Margin',
    'cyc4_debt_equity':          'Debt / Equity',
    'cyc4_quick_ratio':          'Quick Ratio',
    'cyc4_idio_vol':             'Idiosyncratic Volatility (60m)',
    'cyc4_altman_z':             'Altman Z-Score (Distress Signal)',
    'cyc4_repurchase_yield':     'Repurchase Yield (Buybacks Only)',
    'cyc4_roa_dev':              'ROA vs 5yr Average (Improving Trend)',
    'cyc4_roe_dev':              'ROE vs 5yr Average (Improving Trend)',
    # CYC-005 sector-specific novel factors
    'cyc5_energy_midcycle_fcf':    'Energy: Mid-Cycle FCF Yield (3yr Rolling Average)',
    'cyc5_materials_ccc':          'Materials: Cash Conversion Cycle',
    'cyc5_industrials_efficiency': 'Industrials: Efficiency Composite (Asset Turnover x OCF Margin)',
    'cyc5_consumer_disc_brand':    'Consumer Discretionary: Brand-Growth Score',
    'cyc5_staples_div_growth':     'Consumer Staples: Dividend Growth Rate',
    'cyc5_healthcare_rnd_yield':   'Health Care: R&D Yield',
    'cyc5_financials_capital':     'Financials: Capital Adequacy (Equity/Assets)',
    'cyc5_it_rule_of_40':          'Information Technology: Rule of 40',
    'cyc5_comms_content_roi':      'Communication Services: Content ROI',
    'cyc5_utilities_safe_yield':   'Utilities: Safe Dividend Yield',
    'cyc5_realestate_ffo_yield':   'Real Estate: FFO Proxy Yield',
}


# Plain-English factor definitions — what each factor actually measures and why it might predict returns
PLAIN_ENGLISH = {
    'cyc2_ps':            ("What it measures: How much the market is paying per $1 of a company's annual revenue (Market Cap ÷ Sales). Lower = cheaper. "
                           "Why it works: Investors systematically overpay for high-revenue-growth stories. When those stories fail to deliver, high-P/S stocks collapse. "
                           "Low-P/S stocks are unglamorous but often priced for value. This is the single strongest valuation factor in our study."),
    'cyc2_pe':            ("What it measures: How much investors pay per $1 of annual earnings (Price ÷ Earnings Per Share). Lower = cheaper. "
                           "Why it works: Cheap earnings-based stocks tend to be out-of-favour companies that the market has over-punished. They mean-revert. "
                           "Limitation: earnings can be manipulated; P/S and EV/EBITDA are more reliable."),
    'cyc2_pb':            ("What it measures: Market cap divided by book value of equity (assets minus liabilities). Lower = cheaper. "
                           "Why it works: Classic 'value' metric. Works less well than EV-based metrics because book value ignores intangible assets like brands and software. "
                           "Still useful as a secondary valuation check, especially for banks and financials."),
    'cyc2_ev_ebitda':     ("What it measures: Enterprise value (equity + debt − cash) divided by EBITDA (earnings before interest, tax, depreciation, amortisation). Lower = cheaper. "
                           "Why it works: Captures the full cost of buying a business including its debt. EBITDA is a proxy for cash generation. "
                           "This is the metric professional acquirers use when buying companies — strong real-world validation."),
    'cyc2_pfcf':          ("What it measures: Price divided by Free Cash Flow per share. Lower = cheaper. "
                           "Why it works: Free cash flow is real money the business generates — harder to fake than earnings. "
                           "Companies with low P/FCF are often genuinely cheap and self-funding. Tortoriello's #1 valuation factor in his S&P 1500 study."),
    'cyc2_fcf_yield':     ("What it measures: Free cash flow divided by market cap — expressed as a percentage yield, like a bond yield. Higher = better value. "
                           "Why it works: Same logic as P/FCF inverted. A high FCF yield means you're getting a large cash return relative to what you paid. "
                           "Particularly powerful in rising interest rate environments where cash-generating businesses are re-rated."),
    'cyc2_roic':          ("What it measures: Return on Invested Capital — how much profit a company generates per dollar of capital invested in the business. Higher = better. "
                           "Why it works: High ROIC means the business has a competitive moat — it can deploy capital and earn outsized returns. "
                           "Buffett's favourite metric. Companies with sustained high ROIC compound investor wealth over decades."),
    'cyc2_roa':           ("What it measures: Return on Assets — net income divided by total assets. Higher = better. "
                           "Why it works: Efficient companies squeeze more profit from their asset base. High ROA companies tend to be capital-light (software, brands, services) "
                           "which means they can grow without diluting shareholders."),
    'cyc2_roce':          ("What it measures: Return on Capital Employed — EBIT divided by (total assets − current liabilities). Higher = better. "
                           "Why it works: The Fundsmith variant of quality measurement. Terry Smith uses this as his primary screen — only businesses that can earn "
                           "high returns on the capital they employ can compound into great long-term investments."),
    'cyc2_gpa':           ("What it measures: Gross Profit divided by Total Assets (Novy-Marx). Higher = better. "
                           "Why it works: Gross profit is the cleanest profitability measure — it's before any management discretion on operating expenses. "
                           "Novy-Marx showed in 2013 that GPA predicts returns as powerfully as value, and the two signals are nearly uncorrelated — powerful in combination."),
    'cyc2_op_margin':     ("What it measures: Operating Income divided by Revenue. Higher = better. "
                           "Why it works: Companies with high operating margins have pricing power, cost discipline, or structural advantages. "
                           "These companies hold up better in recessions because they have a cushion before they become unprofitable."),
    'cyc2_fcf_margin':    ("What it measures: Free Cash Flow divided by Revenue. Higher = better. "
                           "Why it works: The ultimate test of whether a profitable-looking business actually turns revenue into real cash. "
                           "Companies with high FCF margins are self-funding — they don't need to keep issuing shares or borrowing to grow."),
    'cyc2_gross_margin':  ("What it measures: (Revenue − Cost of Goods Sold) divided by Revenue. Higher = better. "
                           "Why it works: Gross margin reflects the intrinsic economics of the product. High-gross-margin businesses have pricing power. "
                           "A declining gross margin is often the first warning sign that competition is eroding a business's moat."),
    'cyc2_earn_quality':  ("What it measures: How well accounting earnings convert to real cash flow — typically measured as Operating Cash Flow relative to Net Income. Higher = better. "
                           "Why it works: Companies with low earnings quality are using accounting tricks to inflate reported earnings. "
                           "High earnings quality companies are reporting conservatively — what they say they earned, they actually collected in cash."),
    'cyc2_int_cov':       ("What it measures: EBIT divided by Interest Expense — how many times over the company can cover its interest payments from earnings. Higher = safer. "
                           "Why it works: Low interest coverage is a warning sign for financial distress. "
                           "High-coverage companies have the financial buffer to survive recessions and reinvest when competitors are struggling."),
    'cyc2_fcf_debt':      ("What it measures: Free Cash Flow divided by Total Debt. Higher = better. "
                           "Why it works: This shows how quickly a company could pay off all its debt from its cash generation. "
                           "Companies with high FCF/Debt ratios carry very low financial risk and don't need to be bailed out by capital markets."),
    'cyc2_nd_ebitda':     ("What it measures: Net Debt (total debt minus cash) divided by EBITDA. Lower = better (less leverage). "
                           "Why it works: The standard leverage metric used by credit analysts and private equity. "
                           "Companies with Net Debt/EBITDA below 1x are considered conservatively financed. Above 4x signals potential distress."),
    'cyc2_nd_ebit':       ("What it measures: Net Debt divided by EBIT (earnings before interest and tax). Lower = better. "
                           "Why it works: A tighter leverage measure than ND/EBITDA because it excludes the add-back of depreciation. "
                           "Useful for capital-intensive industries where depreciation is a real economic cost."),
    'cyc2_debt_assets':   ("What it measures: Total Debt divided by Total Assets. Lower = better. "
                           "Why it works: Simple balance-sheet leverage check. Companies with low debt-to-assets have more of their financing from equity — "
                           "they won't be forced into fire sales of assets if conditions worsen."),
    'cyc2_cash_assets':   ("What it measures: Cash and Equivalents divided by Total Assets. Higher = more cash-rich. "
                           "Why it works: Cash-rich companies have optionality — they can make acquisitions, buy back shares, or survive downturns without external financing. "
                           "Excess cash can also be a return of capital catalyst."),
    'cyc2_int_pct_op':    ("What it measures: Interest Expense as a percentage of Operating Income. Lower = less of earnings consumed by debt service. "
                           "Why it works: Companies giving away a large fraction of their operating income to lenders are financially constrained — "
                           "they can't invest freely, pay dividends freely, or weather downturns without danger."),
    'cyc2_capex_ocf':     ("What it measures: Capital Expenditure as a percentage of Operating Cash Flow. Lower = more capital-light. "
                           "Why it works: Companies that consume most of their cash flow on maintenance capex have little free cash left for shareholders. "
                           "Low CapEx/OCF businesses are the 'toll road' model — cash flows out to owners without requiring constant reinvestment."),
    'cyc2_share_chg':     ("What it measures: 5-year change in shares outstanding. Lower (negative) = better, meaning the company is buying back shares. "
                           "Why it works: Share issuance dilutes existing shareholders. Buybacks return value. "
                           "Companies that consistently reduce their share count over 5 years are deploying capital in a shareholder-friendly way."),
    'cyc2_cash_conv':     ("What it measures: How efficiently a company converts accounting profits into actual cash — typically Net Income to Cash Flow ratio. Higher = better. "
                           "Why it works: Cash conversion quality separates real businesses from accounting mirages. "
                           "High cash conversion companies are exactly what they report; low cash conversion companies often have problems lurking in working capital or receivables."),
    'cyc2_mom_3m':        ("What it measures: 3-month price momentum — how much the stock has returned over the past 3 months. Higher = stronger recent momentum. "
                           "Why it works: Price momentum reflects genuine fundamental improvements and investor underreaction to good news. "
                           "3-month is the shortest momentum horizon and tends to have more noise than 12-month."),
    'cyc2_mom_6m':        ("What it measures: 6-month price momentum — how much the stock has returned over the past 6 months. Higher = stronger momentum. "
                           "Why it works: 6-month is the 'Goldilocks' momentum window — longer than short-term noise, shorter than the 12-month period where reversals start. "
                           "Jegadeesh and Titman's original 1993 paper showed 6-12 month momentum is the strongest."),
    'cyc2_mom_12m':       ("What it measures: 12-month price momentum (typically skipping the most recent month to avoid reversal). Higher = stronger longer-term trend. "
                           "Why it works: 12-month momentum has the strongest and most robust academic evidence. "
                           "It captures genuine fundamental trends that investors underreact to — earnings upgrades, margin expansion, sector rotation."),
    'cyc2_fip_6m':        ("What it measures: Fundamental Improvement in Price — captures stocks where fundamentals are improving AND price is following. "
                           "Why it works: Combines price signal with fundamental confirmation, reducing the risk of pure price-chasing. "
                           "FIP filters out stocks that are only moving on sentiment without underlying business improvement."),
    'cyc2_fip_12m':       ("What it measures: 12-month Fundamental Improvement in Price. Same logic as 6-month FIP but over a longer horizon. "
                           "Why it works: Longer-horizon fundamental-price alignment tends to be more persistent and less susceptible to short-term reversals."),
    'cyc2_rev_growth_3y': ("What it measures: 3-year revenue growth rate (total). Higher = faster-growing company. "
                           "Why it works: Revenue growth confirms a business is expanding its market. "
                           "However, growth must come alongside quality — pure revenue growth without profitability often destroys value."),
    'cyc2_rev_cagr_1y':   ("What it measures: 1-year revenue per share CAGR. Higher = faster recent growth. "
                           "Why it works: Shorter-horizon growth captures recent acceleration — companies gaining momentum in their business. "
                           "Revenue per share (not just revenue) controls for share dilution."),
    'cyc2_rev_cagr_3y':   ("What it measures: 3-year revenue per share CAGR. More stable than 1-year, captures sustained growth trends. "
                           "Why it works: 3-year smooths out one-off events. Companies sustaining 3-year revenue growth are executing consistently."),
    'cyc2_rev_cagr_5y':   ("What it measures: 5-year revenue per share CAGR. Long-run growth trend. "
                           "Why it works: Businesses that have compounded revenue over 5 years tend to have genuine structural advantages. "
                           "5-year CAGR is harder to fake or inflate than shorter periods."),
    'cyc2_eps_cagr_1y':   ("What it measures: 1-year Earnings Per Share CAGR. "
                           "Why it works: EPS growth drives stock prices over time. Recent EPS acceleration often leads to stock upgrades and multiple expansion. "
                           "Limitation: 1-year EPS is noisy due to one-off charges and tax changes."),
    'cyc2_eps_cagr_3y':   ("What it measures: 3-year EPS CAGR — a more reliable measure of sustained earnings growth. "
                           "Why it works: Businesses compounding earnings at high rates for 3 years are executing well. "
                           "This is the growth dimension of the QARP framework."),
    'cyc2_eps_cagr_5y':   ("What it measures: 5-year EPS CAGR — the long-run earnings compounding rate. "
                           "Why it works: Long-run earnings compounders create the most wealth. "
                           "A business growing EPS at 15%/year for 5 years has nearly doubled its earnings per share — the stock should follow."),
    'cyc2_fcf_cagr_3y':   ("What it measures: 3-year Free Cash Flow per Share CAGR. "
                           "Why it works: FCF growth is more reliable than earnings growth (harder to manipulate). "
                           "Companies growing FCF at high rates for 3 years are building genuine economic value."),
    'cyc2_fcf_cagr_5y':   ("What it measures: 5-year FCF CAGR — the most reliable long-run growth measure. "
                           "Why it works: Sustained FCF compounding over 5 years is the hallmark of a truly exceptional business. "
                           "This is what Buffett, Munger, and Terry Smith screen for."),
    'cyc2_rd_ratio':      ("What it measures: R&D spending as a ratio of revenue. Higher = more investment in future growth. "
                           "Why it works: Companies investing heavily in R&D may appear less profitable today but are building future competitive advantages. "
                           "R&D-intensive companies in technology and healthcare often have strong intellectual property moats."),
    'cyc2_moat_intangible':("What it measures: Whether the company's competitive advantage comes from intangible assets — brands, patents, regulatory licences. "
                            "Why it works: Intangible moats are highly durable. Coca-Cola's brand and AstraZeneca's drug patents are barriers that take decades to erode. "
                            "Companies with strong intangible moats can charge premium prices and maintain high returns on capital indefinitely."),
    'cyc2_moat_switching': ("What it measures: Whether the company benefits from high switching costs — customers would face significant expense, hassle, or risk to switch to a competitor. "
                            "Why it works: Switching costs create sticky revenue and high customer lifetime value. "
                            "Examples: enterprise software (SAP, Salesforce), banking infrastructure, medical device platforms."),
    'cyc2_moat_network':   ("What it measures: Whether the company benefits from network effects — its product becomes more valuable as more people use it. "
                            "Why it works: Network effects create winner-takes-most dynamics. "
                            "Examples: Visa/Mastercard payment networks, Microsoft Office, Airbnb, LinkedIn. Each new user makes the product more valuable for existing users."),
    'cyc2_moat_cost':      ("What it measures: Whether the company has a structural cost advantage over competitors — lower costs from scale, geography, or process innovation. "
                            "Why it works: Cost leaders can undercut competitors while still earning higher profits. "
                            "Examples: Walmart's logistics, Ryanair's point-to-point model, Amazon's AWS infrastructure leverage."),
    'cyc2_moat_scale':     ("What it measures: Efficient scale — the company operates in a niche large enough to support only one or two players, making entry economically unattractive. "
                            "Why it works: Efficient scale is perhaps the most underappreciated moat type. "
                            "Examples: local utilities, niche aerospace parts, specialist logistics in thin-margin markets where a third competitor would destroy economics for everyone."),
    'moat_score':          ("What it measures: Composite moat score combining all five moat types — intangible assets, switching costs, network effects, cost advantage, efficient scale. "
                            "Why it works: Companies with multiple overlapping moats are the most durable compounders. "
                            "This is the quantitative operationalisation of Buffett's competitive moat concept."),
    'moat_rank':           ("What it measures: Ranking of the composite moat score — higher rank = stronger competitive moat. "
                            "Why it works: By converting the score to a rank, we normalize it across the universe for cleaner quintile separation."),
    'fundsmith_rank':      ("What it measures: Ranking according to Terry Smith's Fundsmith criteria — high ROCE, consistent earnings, pricing power, low need for debt. Lower rank = better. "
                            "Why it works: Fundsmith's criteria have produced exceptional long-run returns in his live fund. "
                            "This factor operationalises the Fundsmith checklist systematically across the full R3000 universe."),
    'longeq_rank':         ("What it measures: Ranking according to Long-Term Equity criteria — sustainable competitive advantages, predictable earnings, long reinvestment runway. Higher = better. "
                            "Why it works: Long-equity thinking (10+ year horizon) filters for businesses with compounding characteristics, not just current cheapness or momentum."),
    'rulebreaker_rank':    ("What it measures: Ranking inspired by Motley Fool RuleBreaker criteria — high growth, disruptive innovation, first-mover advantages. Lower rank = better. "
                            "Why it works: While contrarian to classic value investing, disruptive companies that score highly here tend to have runway for multi-year outperformance before the disruption is fully priced."),
    'quality_score':       ("What it measures: OBQ composite quality score combining ROIC, ROA, ROCE, FCF Margin, Gross Margin, and Earnings Quality across all three scoring dimensions (universe/sector/history). "
                            "Why it works: Great businesses — the ones with durable competitive advantages — systematically outperform. "
                            "This composite is our most comprehensive single-lens view of business quality."),
    'quality_score_universe': ("What it measures: The 'universe rank' component of OBQ Quality — how the stock ranks for quality versus ALL ~3,000 stocks in the Russell 3000. "
                               "Why it works: Universe-rank scoring finds the absolute quality leaders regardless of sector. "
                               "These tend to be the companies that every quality-focused fund manager holds — and the evidence confirms they outperform."),
    'value_score':         ("What it measures: OBQ composite value score combining P/S, P/E, P/FCF, EV/EBITDA, and FCF Yield across all three scoring dimensions. "
                            "Why it works: Blending multiple valuation metrics reduces the impact of any single metric being distorted by accounting or sector effects. "
                            "A stock that appears cheap on all five metrics is far more reliably cheap than one that's cheap on only one."),
    'value_score_universe':("What it measures: Universe-rank component of OBQ Value — cheapness relative to the full R3000. "
                            "Why it works: The deepest-value stocks in the universe tend to be contrarian, out-of-favour holdings that institutional managers avoid for career-risk reasons. "
                            "This creates a structural mispricing that patient, rules-based investors can exploit."),
    'growth_score':        ("What it measures: OBQ composite growth score combining Revenue, EPS, and FCF CAGRs across 1, 3, and 5-year horizons. "
                            "Why it works: Sustainable growth compounds share prices. This composite identifies companies growing on multiple metrics simultaneously — "
                            "the best businesses are not one-trick growth stories but broad-based compounders."),
    'growth_score_universe':("What it measures: Universe-rank growth score — fastest growers in the full R3000. "
                             "Why it works: The highest-growth companies in the broad market tend to attract momentum buying and analyst upgrades. "
                             "Combined with quality checks (QARP framework), growth-universe-rank screens find growth at a reasonable quality threshold."),
    'finstr_score':        ("What it measures: OBQ Financial Strength composite combining interest coverage, FCF/debt, net debt/EBITDA, share count change, and debt/assets. "
                            "Why it works: Financially strong companies survive downturns, avoid dilutive capital raises, and can opportunistically acquire or buy back shares at cycle lows. "
                            "Financial weakness is the silent killer of otherwise good businesses."),
    'finstr_score_universe':("What it measures: Universe-rank financial strength score — the most conservatively financed companies in the R3000. "
                             "Why it works: In credit-tightening environments and recessions, the most financially strong companies disproportionately outperform as capital costs rise. "
                             "This is the defensive dimension of OBQ's multi-factor system."),
    'momentum_score':      ("What it measures: OBQ Momentum composite combining 3, 6, and 12-month price momentum across universe/sector/history dimensions. "
                            "Why it works: Momentum reflects genuine fundamental trends that analysts and investors are still underestimating. "
                            "Stocks in strong momentum tend to keep rising because the earnings upgrades haven't fully flowed through to estimates."),
    'momentum_sys_score':  ("What it measures: Systematic momentum — a model-based variant of momentum that filters for trend consistency and reduces noise from short-term volatility. "
                            "Why it works: Systematic filters remove the noisiest momentum signals, leaving only the cleanest trends."),
    'momentum_af_score':   ("What it measures: Adaptive Factor momentum — momentum adjusted for regime. Stronger in trending markets, muted in choppy/mean-reverting conditions. "
                            "Why it works: Pure momentum fails in high-volatility, trend-reversing markets. Adaptive momentum is designed to reduce drawdowns in those regimes."),
    'momentum_fip_score':  ("What it measures: Fundamental Improvement in Price momentum composite — combines both fundamental improvement and price confirmation. "
                            "Why it works: The most dangerous momentum stocks are those rising purely on sentiment without underlying fundamental improvement. "
                            "FIP filters for momentum backed by real business progress."),
    'af_universe_score':   ("What it measures: Adaptive Factor universe score — a cross-factor adaptive signal that adjusts weights based on prevailing market regime. "
                            "Why it works: Different factors dominate in different regimes. An adaptive signal tilts toward whatever is working, improving timing."),
    'jcn_full_composite':  ("What it measures: The JCN Full Composite — our flagship multi-factor score combining Quality (ROIC, margins), Value (P/S, EV/EBITDA), Momentum, Financial Strength, and Growth. "
                            "Why it works: Diversifying across multiple uncorrelated factor signals produces a smoother, more consistent alpha stream. "
                            "No single factor works in every period; the composite wins more periods than any individual factor."),
    'jcn_qarp':            ("What it measures: JCN QARP (Quality at Reasonable Price) — Quality-first screening combined with value validation. "
                            "Why it works: Finding high-quality businesses at reasonable prices is the investment philosophy of Buffett, Munger, Greenblatt, and Fisher in one systematic framework. "
                            "This is our top-performing factor by OBQ Master Score at large-cap level."),
    'jcn_garp':            ("What it measures: JCN GARP (Growth at Reasonable Price) — Growth-first with price discipline. "
                            "Why it works: Pure growth investors overpay; pure value investors miss great businesses. "
                            "GARP finds the middle ground — companies growing fast enough to justify a premium, but not at any price."),
    'jcn_quality_momentum':("What it measures: JCN Quality-Momentum — combines Quality score with Momentum signal. "
                            "Why it works: High-quality businesses in price momentum tend to be executing on genuine business improvements. "
                            "The quality screen reduces momentum crashes by filtering out low-quality 'story stocks' that can reverse sharply."),
    'jcn_value_momentum':  ("What it measures: JCN Value-Momentum — cheap stocks that are already moving. "
                            "Why it works: Value investing's biggest risk is the 'value trap' — a cheap stock that stays cheap forever. "
                            "Adding momentum confirmation means we wait for the market to agree before buying deep value."),
    'jcn_growth_quality_momentum': ("What it measures: JCN GQM — three-way combination of Growth, Quality, and Momentum. "
                                    "Why it works: Companies growing fast, doing so profitably, and already in price uptrends are the market's compounders-in-progress. "
                                    "This triple-confirmation filter is one of our most selective and consistent signals."),
    'jcn_fortress':        ("What it measures: JCN Fortress — Quality + Financial Strength composite. The most defensively positioned factor. "
                            "Why it works: Companies that combine high profitability with balance-sheet strength are nearly indestructible in downturns. "
                            "The 'fortress' metaphor: high walls (quality), deep moat (financial strength), no vulnerabilities."),
    'jcn_alpha_trifecta':  ("What it measures: JCN Alpha Trifecta — our highest-conviction combination of Quality + Value + Momentum. Three independent signal confirmations. "
                            "Why it works: When a company is simultaneously high-quality, cheap, and in price momentum, it has the maximum convergence of factor tailwinds. "
                            "This is our flagship 3-factor signal and our top OBQ Master Score in the all-cap universe."),
}

# For combos, generate a description dynamically
PLAIN_ENGLISH.update({
    'cyc5_energy_midcycle_fcf': (
        "What it measures: A 3-year rolling average of Free Cash Flow divided by Market Cap — "
        "designed to smooth out the extreme volatility caused by oil and gas price swings. "
        "By averaging FCF over 6 semi-annual periods, this metric captures 'mid-cycle' cash generation "
        "rather than peak-cycle euphoria or trough-cycle distress. "
        "Why it works in Energy: E&P companies' single-year FCF can swing from massively positive to deeply negative "
        "purely based on commodity price moves that have nothing to do with operational quality. "
        "The 3yr average removes this noise and identifies companies that generate genuine cash "
        "across the full commodity cycle — the ones that survive and compound through both up and down cycles."
    ),
    'cyc5_materials_ccc': (
        "What it measures: Cash Conversion Cycle = Days Sales Outstanding + Days Inventory Outstanding "
        "− Days Payables Outstanding. Measures how many days it takes a company to convert its investments "
        "in inventory and receivables into cash from sales. Lower = more capital-efficient. "
        "Why it matters for Materials: Mining, chemical, and commodity processing companies are highly capital-intensive. "
        "Companies that manage their working capital cycle tightly — collecting receivables fast, "
        "turning inventory quickly, and stretching payables — free up cash for reinvestment and dividends. "
        "Note: This factor showed INVERTED signal in our R3000 backtest — Materials working capital "
        "efficiency is dominated by commodity price effects that overwhelm the operational signal."
    ),
    'cyc5_industrials_efficiency': (
        "What it measures: The product of Asset Turnover (Revenue/Assets) and OCF Margin (OCF/Revenue), "
        "forming a composite that captures both capital efficiency AND cash conversion in a single metric. "
        "Economically equivalent to OCF/Assets but preserves the intuition that great industrial companies "
        "achieve high revenue per dollar of assets AND high cash flow per dollar of revenue simultaneously. "
        "Why it works in Industrials: The best industrial compounders — think Fastenal, Roper Technologies, "
        "Danaher — are lean, asset-light relative to peers AND convert their revenue to cash efficiently. "
        "This composite captures both dimensions. Cross-sectionally it achieved OBQ 0.785 with ICIR 1.89, "
        "making it a top-15 factor in the entire study."
    ),
    'cyc5_consumer_disc_brand': (
        "What it measures: Average percentile rank of Gross Margin and 3-year Revenue CAGR — "
        "a composite that rewards companies with both pricing power (high gross margins from brand value) "
        "AND growth delivery (sustained revenue expansion). "
        "Why it works in Consumer Discretionary: The sector bifurcates between premium brands "
        "(Lululemon, LVMH equivalent US stocks) that command high margins and grow into them, "
        "versus commodity-like retailers that compete on price with thin margins. "
        "The Brand-Growth composite separates them. Within Consumer Discretionary it showed OBQ 0.247 "
        "— moderate within the sector, suggesting the signal is partially captured by existing quality metrics."
    ),
    'cyc5_staples_div_growth': (
        "What it measures: The 2-year annualized dividend per share growth rate, derived from "
        "changes in total dividends paid from the cash flow statement. "
        "Why it matters for Consumer Staples: Staples are priced as bond-like income instruments. "
        "Management teams that grow dividends consistently signal confidence in earnings durability "
        "— they are effectively committing to a higher payout that requires higher future earnings. "
        "The result in our study showed weak signal (OBQ 0.132), suggesting that the market "
        "already prices dividend growth expectations efficiently in Staples, leaving little systematic alpha."
    ),
    'cyc5_healthcare_rnd_yield': (
        "What it measures: (R&D Expense / Market Cap) × (1 + Revenue CAGR 3yr). "
        "The first term measures R&D intensity relative to enterprise value — how much pipeline investment "
        "per dollar of market cap. The second term weights this by whether that R&D is actually delivering "
        "revenue growth. High R&D yield = productive R&D investment. Low R&D yield = expensive pipeline "
        "with poor commercialization, or cheap stock relative to R&D. "
        "Why it dominates Healthcare: This is the #1 CYC-005 finding. Within Healthcare, R&D Yield "
        "achieved OBQ 0.689, ICIR 1.24, +19.2% Q1-Q5 spread, and PERFECT 100% monotonicity (every bucket "
        "in descending order). Q5 terminal wealth: $140 from $10K start (collapses to near zero). "
        "Companies with high market caps relative to their R&D productivity are destruction machines."
    ),
    'cyc5_financials_capital': (
        "What it measures: Total Equity / Total Assets — the simplest measure of a bank's capital buffer. "
        "Higher = more conservatively capitalised, lower leverage. "
        "Why it INVERTED in Financials: This is the most instructive failure in the study. "
        "Well-capitalised banks with high equity ratios are typically the slow-growth, conservative, "
        "already-mature institutions. The market prices leverage INTO banks because that IS their "
        "business model — a bank with 15% equity/assets is leaving money on the table. "
        "The highest-returning bank stocks over 30 years were the ones that used their capital "
        "aggressively (ROE-focused management), not the ones sitting on excess equity. "
        "Lesson: For Financials, ROE is the right signal, not capital adequacy."
    ),
    'cyc5_it_rule_of_40': (
        "What it measures: Revenue Growth % + FCF Margin % — the 'Rule of 40' metric used universally "
        "in software/SaaS investing. A score of 40+ is considered healthy; 60+ is exceptional. "
        "A company growing revenue 50% annually with -10% FCF margin scores 40. "
        "A mature company growing 5% with 35% FCF margin also scores 40. "
        "Why it works in IT: Within the Information Technology sector, Rule of 40 achieved OBQ 0.322 "
        "and +10.4% Q1-Q5 spread — meaningfully stronger than its cross-sectional OBQ of 0.190. "
        "The within-sector restriction is critical: comparing Rule of 40 across tech AND industrials "
        "creates noise. Within IT alone, the metric cleanly separates high-quality compounders "
        "from 'zombie growth' stocks burning cash without delivering margins."
    ),
    'cyc5_comms_content_roi': (
        "What it measures: Revenue / (Operating Income + R&D Expense + SGA Expense). "
        "Captures how efficiently a communications/media company converts its total value-creation spending "
        "(content, technology, sales & marketing) into revenue. Higher = more efficient content model. "
        "Why it matters for Comm Services: Netflix vs. a traditional broadcaster, Spotify vs. radio — "
        "the digital transformation of media creates companies with wildly different cost structures. "
        "This metric identifies platforms that generate revenue efficiently from their content and "
        "distribution investment. OBQ 0.324 within-sector with +5.5% spread — moderate but real signal."
    ),
    'cyc5_utilities_safe_yield': (
        "What it measures: Dividend Yield × min(OCF/Dividends, 3.0). "
        "Multiplies the dividend yield by a coverage ratio (how many times over operating cash flow "
        "covers the dividend), capped at 3x to avoid outlier distortion. "
        "Why it INVERTED in Utilities: Counterintuitively, the 'safest' dividend payers in Utilities "
        "underperformed. High-coverage, low-yield utilities are the fully-regulated, slow-growth "
        "companies where all future returns are already priced by the bond market. "
        "The 'riskier' dividend payers often have more growth capex and higher future allowed returns. "
        "Lesson: In Utilities, yield alone (without safety adjustment) is actually a better signal "
        "than yield × safety — confirmed by OCF/Assets winning the sector champion run."
    ),
    'cyc5_realestate_ffo_yield': (
        "What it measures: Operating Cash Flow / Market Cap — a proxy for Funds From Operations (FFO) Yield, "
        "the correct valuation metric for Real Estate Investment Trusts. "
        "Standard P/E and EV/EBITDA fail for REITs because depreciation of real estate assets "
        "creates accounting earnings that bear little relation to economic reality. "
        "FFO (operating cash flow before property depreciation add-backs) is the industry-standard "
        "metric. Our OCF/Market Cap is an approximation. "
        "Cross-sectionally it achieved OBQ 0.688 — a strong standalone factor. Within Real Estate alone "
        "the signal was weak (OBQ 0.172), likely because the sector is small (112 stocks) and "
        "sub-sector differences (industrial REITs vs retail REITs vs healthcare REITs) overwhelm "
        "the within-sector ranking."
    ),
})


def _combo_plain_english(sc: str) -> str:
    return (f"What it measures: A two-factor combination signal that rank-averages two individual factor scores. "
            f"The composite rank is computed as the average percentile rank of each factor — so a stock must score well on BOTH factors to rank in Q1. "
            f"Why it works: Two uncorrelated factors combined reduce noise and false signals from either individual factor acting alone. "
            f"Factor combinations with low correlation between them provide the most diversification benefit.")


def _obq_grade(score: Optional[float]) -> str:
    if score is None:        return "n/a"
    if score >= 0.7:          return "Exceptional"
    if score >= 0.5:          return "Strong"
    if score >= 0.3:          return "Moderate"
    if score >= 0.1:          return "Weak"
    if score >= 0:            return "Marginal"
    return "Inverted"


def _staircase_grade(score: Optional[float]) -> str:
    """Staircase score: spread × monotonicity × step uniformity. Higher=cleaner Q1->Qn ladder."""
    if score is None:        return "n/a"
    if score >= 0.20:         return "Exceptional ladder"
    if score >= 0.10:         return "Clean ladder"
    if score >= 0.05:         return "Modest ladder"
    if score >= 0:            return "Weak ladder"
    return "Inverted ladder"


def render_chapter(bundle: dict, chapter_num: Optional[int] = None) -> str:
    """Render a v2 chapter as markdown string with OBQ + Staircase featured throughout."""
    sc = bundle['score_column']
    name = DISPLAY_NAMES.get(sc, sc)
    tiers = bundle['tiers']
    tiers_present = bundle['tiers_present']

    # Use 'all' as base case; if missing, fall back to first available
    base_tier = 'all' if 'all' in tiers else tiers_present[0]
    base = tiers[base_tier]

    # Determine best tier by OBQ score for highlighting
    best_tier = max(tiers_present,
                    key=lambda t: tiers[t].get('obq_fund_score') or -1)

    # Pre-compute hero metrics for use throughout
    base_obq = base.get('obq_fund_score')
    base_stair = base.get('staircase_score')
    best_obq = tiers[best_tier].get('obq_fund_score')
    best_stair = tiers[best_tier].get('staircase_score')

    out = []
    chap_num = f"Chapter {chapter_num}" if chapter_num else "Factor Profile"
    out.append(f"# {chap_num}: {name}\n")
    out.append(f"*Score column: `{sc}`*\n")
    out.append(f"*Universe: Russell 3000 (PIT, survivorship-bias free) | "
               f"Period: {base['start_date']} → {base['end_date']} | "
               f"Rebalance: {base['rebalance_freq']} | "
               f"Hold: {base['hold_months']}mo*\n")

    # ── Plain-English definition ─────────────────────────────────────────────
    plain = PLAIN_ENGLISH.get(sc)
    if plain is None and sc.startswith('combo_'):
        plain = _combo_plain_english(sc)
    if plain:
        out.append("## What This Factor Measures\n")
        out.append(plain + "\n")
        out.append("---\n")

    # ── Hero badges ─────────────────────────────────────────────────────────
    out.append(f"### Hero Metrics — {base_tier.upper()} Base Case\n")
    out.append(f"| OBQ Master Score | Staircase Score | Verdict |")
    out.append(f"|---|---|---|")
    out.append(f"| **{_num(base_obq, 3)}** ({_obq_grade(base_obq)}) | "
               f"**{_num(base_stair, 4)}** ({_staircase_grade(base_stair)}) | "
               f"{_obq_grade(base_obq)} factor with {_staircase_grade(base_stair).lower()} |")
    out.append("")
    out.append("> **OBQ Master** = 25% ICIR + 25% Staircase + 20% AlphaWin + 20% AlphaMag + 10% IC-Hit. "
               "Range: -1.0 (inverted) → +1.0 (exceptional).  \n"
               "> **Staircase** = Q1-Q5 spread × monotonicity × step-uniformity. "
               "Measures how cleanly buckets ladder Q1>Q2>Q3>Q4>Q5.\n")
    out.append("---\n")

    # ── Section 1: At-a-glance ──────────────────────────────────────────────
    out.append("## At a Glance — All Three Cap Tiers as Peers\n")
    out.append("Per the v2 doctrine, each cap tier is presented on equal footing. "
               "There is no \"default\" tier — the right tier depends on your investable universe.\n")
    out.append("")

    # Cross-tier table
    out.append("| Metric | " + " | ".join(t.upper() for t in tiers_present) + " |")
    out.append("|" + "---|" * (len(tiers_present) + 1))
    for row in bundle['cross_tier_table']:
        cells = [_fmt_cell(row.get(t), row['fmt']) for t in tiers_present]
        # Bold the best tier for OBQ Master row
        if row['metric'] == 'OBQ Master':
            cells = [f"**{c}**" if tiers_present[i] == best_tier else c
                     for i, c in enumerate(cells)]
        out.append(f"| {row['metric']} | " + " | ".join(cells) + " |")
    out.append("")

    # Tier verdict
    obq_by_tier = {t: tiers[t].get('obq_fund_score') for t in tiers_present}
    obq_str = ", ".join([f"{t}={_num(v, 3)}" for t, v in obq_by_tier.items() if v is not None])
    out.append(f"**Best tier: `{best_tier}`** (OBQ Master {_num(obq_by_tier[best_tier], 3)}). "
               f"All three tiers: {obq_str}.\n")
    out.append("---\n")

    # ── Section 2: Per-Bucket Performance per Tier ──────────────────────────
    out.append("## Quintile Performance by Cap Tier\n")
    out.append("Each tier's quintile bucket CAGRs. Tier-by-tier monotonicity reveals "
               "where the factor is structurally clean (smooth Q1→Q5 progression) "
               "vs. compressed (flat or noisy buckets).\n")
    # Staircase commentary per tier
    stair_per_tier = ", ".join(
        f"{t.upper()}={_num(tiers[t].get('staircase_score'), 3)} ({_staircase_grade(tiers[t].get('staircase_score'))})"
        for t in tiers_present
    )
    out.append(f"**Staircase scores by tier**: {stair_per_tier}.\n")
    out.append("")

    out.append("| Bucket | " + " | ".join(t.upper() for t in tiers_present) + " |")
    out.append("|" + "---|" * (len(tiers_present) + 1))
    n_buckets = base.get('n_buckets', 5) or 5
    for b in range(1, n_buckets + 1):
        cells = []
        for t in tiers_present:
            bm = (tiers[t].get('bucket_metrics') or {}).get(str(b), {})
            cells.append(_pct(bm.get('cagr'), 2, sign=True) if bm else "—")
        label = f"Q{b}" + (" (best)" if b == 1 else (" (worst)" if b == n_buckets else ""))
        out.append(f"| {label} | " + " | ".join(cells) + " |")
    # Spread row
    spread_cells = [_pct(tiers[t].get('quintile_spread_cagr'), 2, sign=True)
                    for t in tiers_present]
    out.append(f"| **Q1-Q5 Spread** | " + " | ".join(f"**{c}**" for c in spread_cells) + " |")
    out.append("")
    out.append("---\n")

    # ── Section 3: Sector Attribution Matrix ────────────────────────────────
    out.append("## Sector Attribution — Where the Factor Works\n")
    out.append("Q1 minus Q5 spread per GICS sector, per cap tier. "
               "Positive spread = factor discriminates correctly (Q1 outperforms Q5 in that sector). "
               "Negative spread = factor inverts in that sector (avoid using here).\n")
    out.append(f"*Reference: factor's overall OBQ Master Score is "
               f"**{_num(base_obq, 3)}** at {base_tier.upper()} ({_obq_grade(base_obq)}); "
               f"sector inversions below would degrade it if those sectors were over-weighted.*\n")
    out.append("")

    sec_matrix = bundle['sector_matrix']
    # Build header
    out.append("| Sector | " + " | ".join(f"{t.upper()}<br/>Spread" for t in tiers_present) + " |")
    out.append("|" + "---|" * (len(tiers_present) + 1))

    # Sort sectors by base-tier spread descending (where factor works best to where it doesn't)
    sorted_sectors = sorted(
        sec_matrix.items(),
        key=lambda kv: kv[1].get(base_tier, {}).get('spread') or -99,
        reverse=True,
    )
    for sec_name, tier_data in sorted_sectors:
        if sec_name == 'Unknown':
            continue  # skip unknown sector — sample size noise
        cells = []
        for t in tiers_present:
            spread = tier_data.get(t, {}).get('spread')
            cells.append(_pct(spread, 1, sign=True) if spread is not None else "—")
        out.append(f"| {sec_name} | " + " | ".join(cells) + " |")
    out.append("")

    # Find sectors where factor INVERTS (negative spread) in any tier
    invert_warnings = []
    for sec_name, tier_data in sec_matrix.items():
        if sec_name == 'Unknown':
            continue
        for t in tiers_present:
            sp = tier_data.get(t, {}).get('spread')
            if sp is not None and sp < 0:
                invert_warnings.append(f"{sec_name} ({t.upper()}: {_pct(sp, 1, sign=True)})")
    if invert_warnings:
        out.append("**Inversion warnings**: factor produces negative spread (Q5 beats Q1) in: "
                   + "; ".join(invert_warnings) + "\n")
    out.append("---\n")

    # ── Section 4: Regime decomposition ─────────────────────────────────────
    out.append("## Bear / Bull Regime Decomposition\n")
    out.append("How does the factor hold up across major equity regimes? "
               "Bear/bull scores are the average Q1-vs-universe excess return across all "
               "labeled bear or bull windows (1990 recession through 2024).\n")
    out.append(f"*Bear-window performance feeds OBQ Master's downside-protection signal. "
               f"A factor with a strong OBQ ({_num(base_obq, 3)} here) AND a positive bear score "
               f"is the gold standard — it pays in good times AND protects in bad.*\n")
    out.append("")

    out.append("| Regime | " + " | ".join(t.upper() for t in tiers_present) + " |")
    out.append("|" + "---|" * (len(tiers_present) + 1))
    bear_cells = [_pct(tiers[t].get('bear_score'), 2, sign=True) for t in tiers_present]
    bull_cells = [_pct(tiers[t].get('bull_score'), 2, sign=True) for t in tiers_present]
    out.append(f"| Bear-window avg excess | " + " | ".join(bear_cells) + " |")
    out.append(f"| Bull-window avg excess | " + " | ".join(bull_cells) + " |")
    out.append("")

    # Identify bear-resilient tiers
    bear_pos = [t for t in tiers_present if (tiers[t].get('bear_score') or 0) > 0]
    if bear_pos:
        out.append(f"**Bear-resilient in**: {', '.join(t.upper() for t in bear_pos)} "
                   "(positive average excess during bear windows).\n")
    else:
        out.append("**Note**: factor underperforms universe during bear windows on average across all tiers. "
                   "Use only as part of a diversified composite.\n")
    out.append("---\n")

    # ── Section 5: Bucket-Level Tortoriello (best tier) ─────────────────────
    out.append(f"## Tortoriello Bucket Stats — {best_tier.upper()} Tier (Best OBQ {_num(best_obq, 3)})\n")
    out.append("Per-bucket annualized risk/return decomposition. Use this to validate that "
               "Q1's outperformance comes from genuine alpha, not just higher beta.\n")
    out.append(f"*This tier's Staircase is **{_num(best_stair, 4)}** ({_staircase_grade(best_stair)}). "
               f"A high Staircase + Q1 alpha > 0 + Q1 beta < 1.0 is the defensive-quality signature — "
               f"verify in the Q1 row below.*\n")
    out.append("")

    tort = (tiers[best_tier].get('tortoriello') or {})
    if tort:
        out.append("| Bucket | Terminal $ (10K start) | Annualized Excess | Std Dev | Beta | Alpha (annual) | Max Loss | % 3yr Beat Univ |")
        out.append("|---|---|---|---|---|---|---|---|")
        for b in range(1, n_buckets + 1):
            bs = tort.get(str(b), {}) or {}
            row = (f"| Q{b} | "
                   f"${_int(bs.get('terminal_wealth'))} | "
                   f"{_pct(bs.get('avg_excess_vs_univ'), 2, sign=True)} | "
                   f"{_pct(bs.get('std_dev_ann'), 1)} | "
                   f"{_num(bs.get('beta_vs_univ'), 2)} | "
                   f"{_pct(bs.get('alpha_vs_univ'), 2, sign=True)} | "
                   f"{_pct(bs.get('max_loss'), 1)} | "
                   f"{_pct(bs.get('pct_3y_beats_univ'), 0)} |")
            out.append(row)
    else:
        out.append("*Tortoriello data not available for this tier.*")
    out.append("")
    out.append("---\n")

    # ── Section 6: Interpretation (data-driven only) ───────────────────────
    out.append("## Interpretation\n")

    # Data-driven verdict
    obq = base.get('obq_fund_score') or 0
    icir = base.get('icir') or 0
    spread = base.get('quintile_spread_cagr') or 0
    alpha_win = base.get('alpha_win_rate') or 0

    if obq >= 0.7:
        verdict = "**Exceptional factor** — primary signal candidate"
    elif obq >= 0.5:
        verdict = "**Strong factor** — viable as standalone or composite anchor"
    elif obq >= 0.3:
        verdict = "**Moderate factor** — useful in a multi-factor composite"
    elif obq >= 0:
        verdict = "**Weak factor** — marginal standalone value"
    else:
        verdict = "**Inverted/broken factor** — consider sign flip or exclusion"

    stair = base.get('staircase_score') or 0

    out.append(f"- {verdict} — **OBQ Master {_num(obq, 3)}** at {base_tier.upper()}.")
    out.append(f"- **Staircase {_num(stair, 4)}** ({_staircase_grade(stair)}): "
               f"the Q1→Q5 ladder is {'cleanly monotonic' if stair >= 0.10 else 'modestly ordered' if stair >= 0.05 else 'noisy/compressed'}. "
               f"This is the second of OBQ Master's two largest weights (25%) and dictates whether "
               f"buckets can be trusted as a clean signal gradient.")
    out.append(f"- Ranking consistency (ICIR={_num(icir, 2)}) is "
               f"{'strong' if icir >= 1.5 else 'moderate' if icir >= 0.8 else 'weak'} — "
               f"factor sorts stocks correctly in approximately "
               f"{_pct(base.get('ic_hit_rate'), 0)} of rebalance periods (ICIR is the other 25% of OBQ).")
    out.append(f"- Q1-Q5 spread of {_pct(spread, 1, sign=True)} CAGR over "
               f"{base.get('n_obs', 0)} periods. Q1 beats universe in "
               f"{_pct(alpha_win, 0)} of calendar years.")

    # Tier consistency commentary — OBQ + Staircase across tiers
    obq_vals = [tiers[t].get('obq_fund_score') for t in tiers_present
                if tiers[t].get('obq_fund_score') is not None]
    stair_vals = [tiers[t].get('staircase_score') for t in tiers_present
                  if tiers[t].get('staircase_score') is not None]
    if len(obq_vals) >= 2:
        obq_range = max(obq_vals) - min(obq_vals)
        stair_range = max(stair_vals) - min(stair_vals) if len(stair_vals) >= 2 else 0
        if obq_range < 0.05:
            out.append(f"- **Tier-stable** (OBQ range {_num(obq_range, 2)}, Staircase range {_num(stair_range, 4)}): "
                       "factor works similarly across cap tiers — safe to deploy across the full R3000.")
        elif obq_range > 0.15:
            out.append(f"- **Tier-sensitive**: OBQ varies by {_num(obq_range, 2)}, Staircase by {_num(stair_range, 4)} across tiers. "
                       f"Best tier is **{best_tier.upper()}** (OBQ {_num(best_obq, 3)}, Staircase {_num(best_stair, 4)}) — "
                       f"restrict deployment to that universe.")
        else:
            out.append(f"- **Modestly tier-sensitive**: OBQ range {_num(obq_range, 2)}, Staircase range {_num(stair_range, 4)}. "
                       f"Slight preference for **{best_tier.upper()}** (OBQ {_num(best_obq, 3)}, "
                       f"Staircase {_num(best_stair, 4)}).")

    # Sector concentration
    if sec_matrix:
        non_unk = {s: d for s, d in sec_matrix.items() if s != 'Unknown'}
        spreads_at_base = [(s, d.get(base_tier, {}).get('spread') or 0)
                           for s, d in non_unk.items()]
        spreads_at_base.sort(key=lambda x: x[1], reverse=True)
        if spreads_at_base:
            top3 = ", ".join(f"{s} ({_pct(sp, 1, sign=True)})" for s, sp in spreads_at_base[:3])
            bot3 = ", ".join(f"{s} ({_pct(sp, 1, sign=True)})" for s, sp in spreads_at_base[-3:])
            out.append(f"- **Strongest sectors** ({base_tier.upper()}): {top3}.")
            out.append(f"- **Weakest sectors** ({base_tier.upper()}): {bot3}.")
    out.append("")
    out.append("---\n")

    # ── Plain-English Results Summary ─────────────────────────────────────────
    out.append("## Plain-English Results Summary\n")
    out.append("*This section translates the numbers above into plain language for quick reference.*\n")
    out.append("")

    # Verdict line
    obq_v = base.get('obq_fund_score') or 0
    stair_v = base.get('staircase_score') or 0
    spread_v = base.get('quintile_spread_cagr') or 0
    alpha_win_v = base.get('alpha_win_rate') or 0
    icir_v = base.get('icir') or 0
    q1_cagr_v = base.get('q1_cagr') or 0
    n_obs_v = base.get('n_obs') or 60
    bear_v = base.get('bear_score') or 0
    n_stocks_v = int(base.get('n_stocks_avg') or 0)

    # Overall verdict
    if obq_v >= 0.7:
        overall = f"**{name} is one of the strongest factors in this study.** An OBQ Master Score of {_num(obq_v,3)} (Exceptional) means it has consistently and cleanly separated winning from losing stocks across 30 years of Russell 3000 data."
    elif obq_v >= 0.5:
        overall = f"**{name} is a strong, reliable factor.** An OBQ Master Score of {_num(obq_v,3)} (Strong) means it has meaningfully predicted stock returns across most market conditions."
    elif obq_v >= 0.3:
        overall = f"**{name} is a useful but not dominant factor.** An OBQ Master Score of {_num(obq_v,3)} (Moderate) means it adds value when combined with stronger signals, but is less reliable on its own."
    else:
        overall = f"**{name} produced limited predictive power in this study.** An OBQ Master Score of {_num(obq_v,3)} suggests this factor is weak or inconsistent over the test period."

    out.append(overall + "\n")

    # What the numbers mean in plain English
    out.append(f"**The Q1-Q5 spread of {_pct(spread_v,1,sign=True)} CAGR** means: if you had held the cheapest/best 20% of stocks by this metric "
               f"and shorted the worst 20%, you would have earned approximately {_pct(spread_v,1,sign=True)} per year more than your short basket, "
               f"on average across {n_obs_v} semi-annual periods dating back to 1995. "
               f"The top quintile (Q1) alone delivered approximately {_pct(q1_cagr_v,1,sign=True)} per year.\n")

    out.append(f"**The Staircase Score of {_num(stair_v,4)} ({_staircase_grade(stair_v)})** means: "
               f"{'the factor cleanly separates all five quintile buckets — Q1 best, Q5 worst, with a smooth progression in between. You can trust relative rankings across all stocks, not just the top 20%.' if stair_v >= 0.10 else 'the factor most reliably separates the top and bottom quintiles but the middle buckets are noisier. Use it primarily to identify the top 20% and avoid the bottom 20%, rather than as a fine-grained ranking signal.'}\n")

    # Alpha win rate translation
    alpha_yrs = int(round(alpha_win_v * 30))
    out.append(f"**Q1 beat the market in {_pct(alpha_win_v,0)} of calendar years** ({alpha_yrs} out of approximately 30 years). "
               f"{'This level of consistency is rare — the factor works across different economic regimes, interest rate cycles, and market conditions.' if alpha_win_v >= 0.8 else 'There are periods where the factor underperforms, but the long-run edge is consistent.' if alpha_win_v >= 0.65 else 'The factor has significant periods of underperformance and requires patience to hold through.'}\n")

    # Bear score translation
    if bear_v > 0.02:
        out.append(f"**Bear-market behaviour:** The Q1 basket outperformed the universe by an average of {_pct(bear_v,1,sign=True)} during bear market windows (2000–02 dot-com crash, 2007–09 GFC, 2020 COVID, 2022 rate shock). "
                   f"This means the factor is not only a return generator but a partial hedge — stocks that rank well here tend to hold up better when markets fall.\n")
    elif bear_v > 0:
        out.append(f"**Bear-market behaviour:** Small positive outperformance ({_pct(bear_v,1,sign=True)}) during bear windows. "
                   f"The factor provides modest downside protection but is not primarily defensive.\n")
    else:
        out.append(f"**Bear-market behaviour:** Q1 underperforms the universe slightly during bear windows ({_pct(bear_v,1,sign=True)}). "
                   f"Do not rely on this factor for downside protection — pair with a defensive factor in volatile regimes.\n")

    # Tier recommendation
    best_t = max(tiers_present, key=lambda t: tiers[t].get('obq_fund_score') or -1)
    best_obq_val = tiers[best_t].get('obq_fund_score') or 0
    out.append(f"**Best universe to deploy this factor:** {best_t.upper()} (OBQ {_num(best_obq_val,3)}). "
               f"{'The factor works well across all cap tiers — you can apply it to the full Russell 3000 without significant loss of signal quality.' if max(tiers[t].get('obq_fund_score',0) or 0 for t in tiers_present) - min(tiers[t].get('obq_fund_score',0) or 0 for t in tiers_present) < 0.08 else f'Strongest in the {best_t.upper()} universe — if you run a large-cap or mid-cap portfolio, check your applicable tier above.'}\n")

    out.append("---\n")
    out.append(f"*Generated from `{base['strategy_id']}` and tier siblings. "
               f"Source: OBQ FactorLab strategy bank, CYC-003-GPU run on R3000 universe.*\n")

    return "\n".join(out)


# ── Self-test ─────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    from factor_data import load_factor_bundle
    bundle = load_factor_bundle('jcn_qarp')
    md = render_chapter(bundle, chapter_num=1)
    print(md)
