#!/usr/bin/env python3
"""
pull_r3k_fundamentals.py
========================
Pulls historical fundamentals from EODHD API for R3000 symbols
that are missing pre-2007 data in PROD_EOD_Fundamentals.

Stages data into STAGING_EOD_Fundamentals_Pre2007 table in the mirror DB.
Does NOT touch PROD_EOD_Fundamentals until validated.

Safe to re-run — skips already-staged symbols.

Usage:
    python pull_r3k_fundamentals.py [--dry-run] [--limit N] [--workers N]
"""
import sys, os, json, time, argparse, logging, traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import requests
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)
load_dotenv(r'C:\Users\admin\Desktop\OBQ_AI\OBQ_FactorLab\.env')

API_KEY  = "6939d7058509c1.59763781"
BASE_URL = "https://eodhd.com/api"
MIRROR_DB = os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# All 196 columns matching PROD_EOD_Fundamentals exactly
COLS = [
    'symbol','date','filing_date','company_name','exchange','country',
    'sector','industry','gic_sector','gic_group','gic_industry','gic_sub_industry',
    'shares_outstanding','beta',
    'bs_totalAssets','bs_totalLiab','bs_totalStockholderEquity','bs_cash',
    'bs_shortTermInvestments','bs_netReceivables','bs_inventory','bs_otherCurrentAssets',
    'bs_totalCurrentAssets','bs_longTermInvestments','bs_propertyPlantEquipment',
    'bs_goodWill','bs_intangibleAssets','bs_otherAssets','bs_deferredLongTermAssetCharges',
    'bs_accountsPayable','bs_shortLongTermDebt','bs_otherCurrentLiab',
    'bs_totalCurrentLiabilities','bs_longTermDebt','bs_otherLiab',
    'bs_deferredLongTermLiab','bs_minorityInterest','bs_negativeGoodwill',
    'bs_commonStock','bs_retainedEarnings','bs_treasuryStock','bs_capitalSurplus',
    'bs_otherStockholderEquity','bs_preferredStock','bs_accumulatedAmortization',
    'bs_netTangibleAssets','bs_shortTermDebt','bs_capitalLeaseObligations',
    'bs_netDebt','bs_netWorkingCapital','bs_netInvestedCapital',
    'bs_commonStockTotalEquity','bs_preferredStockTotalEquity',
    'bs_liabilitiesAndStockholdersEquity','bs_cashAndShortTermInvestments',
    'bs_accumulatedOtherComprehensiveIncome','bs_commonStockSharesOutstanding',
    'bs_warrants','bs_redeemablePreferredStock','bs_capitalSurpluse',
    'bs_additionalPaidInCapital','bs_preferredStockRedeemable',
    'bs_longTermDebtNoncurrent','bs_accumulatedDepreciation',
    'bs_nonCurrentAssetsTotal','bs_nonCurrentLiabilitiesTotal',
    'bs_nonCurrentLiabilitiesOther',
    'bs_noncontrollingInterestInConsolidatedEntity',
    'bs_temporaryEquityRedeemableNoncontrollingInterests',
    'bs_deferredLongTermLiabCharges','bs_liabilitiesAndShareholdersEquity',
    'bs_cashAndEquivalents','bs_otherNonCurrentAssets',
    'is_totalRevenue','is_costOfRevenue','is_grossProfit','is_researchDevelopment',
    'is_sellingGeneralAdministrative','is_operatingExpense','is_operatingIncome',
    'is_totalOtherIncomeExpenseNet','is_ebit','is_interestExpense',
    'is_incomeBeforeTax','is_incomeTaxExpense','is_minorityInterest',
    'is_netIncomeFromContinuingOps','is_discontinuedOperations',
    'is_extraordinaryItems','is_effectOfAccountingCharges','is_otherItems',
    'is_netIncome','is_netIncomeApplicableToCommonShares',
    'is_preferredStockAndOtherAdjustments','is_nonRecurring',
    'is_otherOperatingExpenses','is_depreciation','is_depreciationAndAmortization',
    'is_interestIncome','is_nonOperatingIncomeNetOther',
    'is_sellingAndMarketingExpenses','is_reconciledDepreciation','is_ebitda',
    'is_netInterestIncome',
    'cf_totalCashFromOperatingActivities','cf_depreciation',
    'cf_adjustmentsToNetIncome','cf_changesInAccountsReceivables',
    'cf_changesInLiabilities','cf_changesInInventories',
    'cf_changesInOtherOperatingActivities','cf_capitalExpenditures',
    'cf_investments','cf_otherCashflowsFromInvestingActivities',
    'cf_dividendsPaid','cf_salePurchaseOfStock','cf_netBorrowings',
    'cf_otherCashflowsFromFinancingActivities','cf_changeInCashAndCashEquivalents',
    'cf_changeToLiabilities','cf_changeToNetincome','cf_changeToOperatingActivities',
    'cf_changeToAccountReceivables','cf_changeToInventory',
    'cf_totalCashFromOperations','cf_issuanceOfCapitalStock','cf_freeCashFlow',
    'cf_beginPeriodCashFlow','cf_endPeriodCashFlow','cf_effectOfExchangeRate',
    'cf_exchangeRateChanges','cf_netIncome','cf_otherNonCashItems',
    'cf_stockBasedCompensation','cf_totalCashFromFinancingActivities',
    'cf_totalCashflowsFromInvestingActivities',
    'analyst_buy','analyst_hold','analyst_rating','analyst_sell',
    'analyst_strong_buy','analyst_strong_sell','analyst_target_price','analyst_total',
    'book_value','diluted_eps_ttm','dividend_share','dividend_yield',
    'earnings_before_after_market','earnings_eps_actual','earnings_eps_difference',
    'earnings_eps_estimate','earnings_quarter_date','earnings_report_date',
    'earnings_share','earnings_surprise_percent','earnings_trend_avg',
    'earnings_trend_growth','earnings_trend_high','earnings_trend_low',
    'earnings_trend_num_analysts','ebitda','enterprise_value',
    'enterprise_value_ebitda','enterprise_value_revenue',
    'eps_estimate_current_quarter','eps_estimate_current_year',
    'eps_estimate_next_quarter','eps_estimate_next_year',
    'eps_revisions_down_30d','eps_revisions_down_7d',
    'eps_revisions_up_30d','eps_revisions_up_7d',
    'forward_pe','gross_profit_ttm','market_cap','market_cap_mln',
    'most_recent_quarter','operating_margin_ttm','pe_ratio','peg_ratio',
    'price_book_mrq','price_sales_ttm','profit_margin',
    'quarterly_earnings_growth_yoy','quarterly_revenue_growth_yoy',
    'return_on_assets_ttm','return_on_equity_ttm',
    'revenue_estimate_avg','revenue_estimate_growth',
    'revenue_estimate_high','revenue_estimate_low',
    'revenue_per_share_ttm','revenue_ttm','trailing_pe','wall_street_target_price',
]


def safe_float(v):
    if v is None or v == 'None' or v == '':
        return None
    try:
        f = float(v)
        return None if (f != f) else f  # NaN check
    except (ValueError, TypeError):
        return None


def safe_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() != 'none' else None


def extract_quarterly_rows(symbol: str, data: dict) -> list[dict]:
    """
    Parse EODHD fundamentals JSON into flat quarterly rows
    matching PROD_EOD_Fundamentals schema.
    Returns one dict per quarter-end date.
    """
    financials = data.get('Financials', {})
    highlights  = data.get('Highlights', {})
    valuation   = data.get('Valuation', {})
    share_stats = data.get('SharesStats', {})
    tech        = data.get('Technicals', {})
    analyst     = data.get('AnalystRatings', {})
    earnings    = data.get('Earnings', {})
    etd         = data.get('ETFData', {})

    # Quarterly statements
    bs_q  = financials.get('Balance_Sheet', {}).get('quarterly', {})
    is_q  = financials.get('Income_Statement', {}).get('quarterly', {})
    cf_q  = financials.get('Cash_Flow', {}).get('quarterly', {})

    # Get union of all quarterly dates
    all_dates = set(bs_q) | set(is_q) | set(cf_q)
    if not all_dates:
        return []

    general = data.get('General', {})
    company_name = safe_str(general.get('Name'))
    exchange     = safe_str(general.get('Exchange'))
    country      = safe_str(general.get('CountryISO'))
    sector       = safe_str(general.get('Sector'))
    industry     = safe_str(general.get('Industry'))
    gic_sector   = safe_str(general.get('GicSector'))
    gic_group    = safe_str(general.get('GicGroup'))
    gic_industry = safe_str(general.get('GicIndustry'))
    gic_sub      = safe_str(general.get('GicSubIndustry'))

    # Highlights (current snapshot — applied to all rows, some stale)
    shares_out   = safe_float(share_stats.get('SharesOutstanding') or highlights.get('SharesOutstanding'))
    beta         = safe_float(tech.get('Beta'))
    market_cap   = safe_float(highlights.get('MarketCapitalization'))
    mktcap_mln   = safe_float(highlights.get('MarketCapitalizationMln'))
    ebitda_hl    = safe_float(highlights.get('EBITDA'))
    pe_ratio     = safe_float(highlights.get('PERatio'))
    peg_ratio    = safe_float(highlights.get('PEGRatio'))
    eps_ttm      = safe_float(highlights.get('DilutedEpsTTM'))
    book_val     = safe_float(highlights.get('BookValue'))
    div_share    = safe_float(highlights.get('DividendShare'))
    div_yield    = safe_float(highlights.get('DividendYield'))
    profit_mg    = safe_float(highlights.get('ProfitMargin'))
    op_mg_ttm    = safe_float(highlights.get('OperatingMarginTTM'))
    roa_ttm      = safe_float(highlights.get('ReturnOnAssetsTTM'))
    roe_ttm      = safe_float(highlights.get('ReturnOnEquityTTM'))
    rev_ttm      = safe_float(highlights.get('RevenueTTM'))
    rev_ps_ttm   = safe_float(highlights.get('RevenuePerShareTTM'))
    qtr_eg_yoy   = safe_float(highlights.get('QuarterlyEarningsGrowthYOY'))
    qtr_rv_yoy   = safe_float(highlights.get('QuarterlyRevenueGrowthYOY'))
    gp_ttm       = safe_float(highlights.get('GrossProfitTTM'))
    trail_pe     = safe_float(highlights.get('TrailingPE'))
    fwd_pe       = safe_float(highlights.get('ForwardPE'))
    ps_ttm_hl    = safe_float(valuation.get('PriceSalesTTM'))
    pb_mrq       = safe_float(valuation.get('PriceBookMRQ'))
    ev_val       = safe_float(valuation.get('EnterpriseValue'))
    ev_eb        = safe_float(valuation.get('EnterpriseValueEbitda'))
    ev_rev       = safe_float(valuation.get('EnterpriseValueRevenue'))
    mrq          = safe_str(highlights.get('MostRecentQuarter'))
    ws_target    = safe_float(analyst.get('TargetPrice'))
    ar_buy       = safe_float(analyst.get('Buy'))
    ar_hold      = safe_float(analyst.get('Hold'))
    ar_sell      = safe_float(analyst.get('Sell'))
    ar_sbuy      = safe_float(analyst.get('StrongBuy'))
    ar_ssell     = safe_float(analyst.get('StrongSell'))
    ar_rating    = safe_float(analyst.get('Rating'))
    ar_total_raw = (ar_buy or 0) + (ar_hold or 0) + (ar_sell or 0) + (ar_sbuy or 0) + (ar_ssell or 0)
    ar_total     = ar_total_raw if ar_total_raw > 0 else None

    # Earnings data
    earnings_hist = earnings.get('History', {})
    earnings_trend = earnings.get('Trend', {})
    ern_trend_latest = next(iter(earnings_trend.values()), {}) if earnings_trend else {}

    rows = []
    for q_date in sorted(all_dates):
        bs = bs_q.get(q_date, {})
        is_ = is_q.get(q_date, {})
        cf  = cf_q.get(q_date, {})

        # Filing date: use the date from the statement data if available
        filing_date = safe_str(bs.get('date') or is_.get('date') or cf.get('date'))
        # Fallback: estimate ~45 days after quarter end
        if not filing_date:
            filing_date = None

        row = {
            'symbol':         symbol,
            'date':           q_date,
            'filing_date':    filing_date,
            'company_name':   company_name,
            'exchange':       exchange,
            'country':        country,
            'sector':         sector,
            'industry':       industry,
            'gic_sector':     gic_sector,
            'gic_group':      gic_group,
            'gic_industry':   gic_industry,
            'gic_sub_industry': gic_sub,
            'shares_outstanding': shares_out,
            'beta':           beta,
            # Balance Sheet
            'bs_totalAssets':            safe_float(bs.get('totalAssets')),
            'bs_totalLiab':              safe_float(bs.get('totalLiab')),
            'bs_totalStockholderEquity': safe_float(bs.get('totalStockholderEquity')),
            'bs_cash':                   safe_float(bs.get('cash')),
            'bs_shortTermInvestments':   safe_float(bs.get('shortTermInvestments')),
            'bs_netReceivables':         safe_float(bs.get('netReceivables')),
            'bs_inventory':              safe_float(bs.get('inventory')),
            'bs_otherCurrentAssets':     safe_float(bs.get('otherCurrentAssets')),
            'bs_totalCurrentAssets':     safe_float(bs.get('totalCurrentAssets')),
            'bs_longTermInvestments':    safe_float(bs.get('longTermInvestments')),
            'bs_propertyPlantEquipment': safe_float(bs.get('propertyPlantEquipment')),
            'bs_goodWill':               safe_float(bs.get('goodWill')),
            'bs_intangibleAssets':       safe_float(bs.get('intangibleAssets')),
            'bs_otherAssets':            safe_float(bs.get('otherAssets')),
            'bs_deferredLongTermAssetCharges': safe_float(bs.get('deferredLongTermAssetCharges')),
            'bs_accountsPayable':        safe_float(bs.get('accountsPayable')),
            'bs_shortLongTermDebt':      safe_float(bs.get('shortLongTermDebt')),
            'bs_otherCurrentLiab':       safe_float(bs.get('otherCurrentLiab')),
            'bs_totalCurrentLiabilities':safe_float(bs.get('totalCurrentLiabilities')),
            'bs_longTermDebt':           safe_float(bs.get('longTermDebt')),
            'bs_otherLiab':              safe_float(bs.get('otherLiab')),
            'bs_deferredLongTermLiab':   safe_float(bs.get('deferredLongTermLiab')),
            'bs_minorityInterest':       safe_float(bs.get('minorityInterest')),
            'bs_negativeGoodwill':       safe_float(bs.get('negativeGoodwill')),
            'bs_commonStock':            safe_float(bs.get('commonStock')),
            'bs_retainedEarnings':       safe_float(bs.get('retainedEarnings')),
            'bs_treasuryStock':          safe_float(bs.get('treasuryStock')),
            'bs_capitalSurplus':         safe_float(bs.get('capitalSurplus')),
            'bs_otherStockholderEquity': safe_float(bs.get('otherStockholderEquity')),
            'bs_preferredStock':         safe_float(bs.get('preferredStock')),
            'bs_accumulatedAmortization':safe_float(bs.get('accumulatedAmortization')),
            'bs_netTangibleAssets':      safe_float(bs.get('netTangibleAssets')),
            'bs_shortTermDebt':          safe_float(bs.get('shortTermDebt')),
            'bs_capitalLeaseObligations':safe_float(bs.get('capitalLeaseObligations')),
            'bs_netDebt':                safe_float(bs.get('netDebt')),
            'bs_netWorkingCapital':      safe_float(bs.get('netWorkingCapital')),
            'bs_netInvestedCapital':     safe_float(bs.get('netInvestedCapital')),
            'bs_commonStockTotalEquity': safe_float(bs.get('commonStockTotalEquity')),
            'bs_preferredStockTotalEquity': safe_float(bs.get('preferredStockTotalEquity')),
            'bs_liabilitiesAndStockholdersEquity': safe_float(bs.get('liabilitiesAndStockholdersEquity')),
            'bs_cashAndShortTermInvestments': safe_float(bs.get('cashAndShortTermInvestments')),
            'bs_accumulatedOtherComprehensiveIncome': safe_float(bs.get('accumulatedOtherComprehensiveIncome')),
            'bs_commonStockSharesOutstanding': safe_float(bs.get('commonStockSharesOutstanding')),
            'bs_warrants':               safe_float(bs.get('warrants')),
            'bs_redeemablePreferredStock': safe_float(bs.get('redeemablePreferredStock')),
            'bs_capitalSurpluse':        safe_float(bs.get('capitalSurpluse')),
            'bs_additionalPaidInCapital':safe_float(bs.get('additionalPaidInCapital')),
            'bs_preferredStockRedeemable': safe_float(bs.get('preferredStockRedeemable')),
            'bs_longTermDebtNoncurrent': safe_float(bs.get('longTermDebtNoncurrent')),
            'bs_accumulatedDepreciation':safe_float(bs.get('accumulatedDepreciation')),
            'bs_nonCurrentAssetsTotal':  safe_float(bs.get('nonCurrentAssetsTotal')),
            'bs_nonCurrentLiabilitiesTotal': safe_float(bs.get('nonCurrentLiabilitiesTotal')),
            'bs_nonCurrentLiabilitiesOther': safe_float(bs.get('nonCurrentLiabilitiesOther')),
            'bs_noncontrollingInterestInConsolidatedEntity': safe_float(bs.get('noncontrollingInterestInConsolidatedEntity')),
            'bs_temporaryEquityRedeemableNoncontrollingInterests': safe_float(bs.get('temporaryEquityRedeemableNoncontrollingInterests')),
            'bs_deferredLongTermLiabCharges': safe_float(bs.get('deferredLongTermLiabCharges')),
            'bs_liabilitiesAndShareholdersEquity': safe_float(bs.get('liabilitiesAndShareholdersEquity')),
            'bs_cashAndEquivalents':     safe_float(bs.get('cashAndEquivalents')),
            'bs_otherNonCurrentAssets':  safe_float(bs.get('otherNonCurrentAssets')),
            # Income Statement
            'is_totalRevenue':           safe_float(is_.get('totalRevenue')),
            'is_costOfRevenue':          safe_float(is_.get('costOfRevenue')),
            'is_grossProfit':            safe_float(is_.get('grossProfit')),
            'is_researchDevelopment':    safe_float(is_.get('researchDevelopment')),
            'is_sellingGeneralAdministrative': safe_float(is_.get('sellingGeneralAdministrative')),
            'is_operatingExpense':       safe_float(is_.get('operatingExpense')),
            'is_operatingIncome':        safe_float(is_.get('operatingIncome')),
            'is_totalOtherIncomeExpenseNet': safe_float(is_.get('totalOtherIncomeExpenseNet')),
            'is_ebit':                   safe_float(is_.get('ebit')),
            'is_interestExpense':        safe_float(is_.get('interestExpense')),
            'is_incomeBeforeTax':        safe_float(is_.get('incomeBeforeTax')),
            'is_incomeTaxExpense':       safe_float(is_.get('incomeTaxExpense')),
            'is_minorityInterest':       safe_float(is_.get('minorityInterest')),
            'is_netIncomeFromContinuingOps': safe_float(is_.get('netIncomeFromContinuingOps')),
            'is_discontinuedOperations': safe_float(is_.get('discontinuedOperations')),
            'is_extraordinaryItems':     safe_float(is_.get('extraordinaryItems')),
            'is_effectOfAccountingCharges': safe_float(is_.get('effectOfAccountingCharges')),
            'is_otherItems':             safe_float(is_.get('otherItems')),
            'is_netIncome':              safe_float(is_.get('netIncome')),
            'is_netIncomeApplicableToCommonShares': safe_float(is_.get('netIncomeApplicableToCommonShares')),
            'is_preferredStockAndOtherAdjustments': safe_float(is_.get('preferredStockAndOtherAdjustments')),
            'is_nonRecurring':           safe_float(is_.get('nonRecurring')),
            'is_otherOperatingExpenses': safe_float(is_.get('otherOperatingExpenses')),
            'is_depreciation':           safe_float(is_.get('depreciation')),
            'is_depreciationAndAmortization': safe_float(is_.get('depreciationAndAmortization')),
            'is_interestIncome':         safe_float(is_.get('interestIncome')),
            'is_nonOperatingIncomeNetOther': safe_float(is_.get('nonOperatingIncomeNetOther')),
            'is_sellingAndMarketingExpenses': safe_float(is_.get('sellingAndMarketingExpenses')),
            'is_reconciledDepreciation': safe_float(is_.get('reconciledDepreciation')),
            'is_ebitda':                 safe_float(is_.get('ebitda')),
            'is_netInterestIncome':      safe_float(is_.get('netInterestIncome')),
            # Cash Flow
            'cf_totalCashFromOperatingActivities': safe_float(cf.get('totalCashFromOperatingActivities')),
            'cf_depreciation':           safe_float(cf.get('depreciation')),
            'cf_adjustmentsToNetIncome': safe_float(cf.get('adjustmentsToNetIncome')),
            'cf_changesInAccountsReceivables': safe_float(cf.get('changesInAccountsReceivables')),
            'cf_changesInLiabilities':   safe_float(cf.get('changesInLiabilities')),
            'cf_changesInInventories':   safe_float(cf.get('changesInInventories')),
            'cf_changesInOtherOperatingActivities': safe_float(cf.get('changesInOtherOperatingActivities')),
            'cf_capitalExpenditures':    safe_float(cf.get('capitalExpenditures')),
            'cf_investments':            safe_float(cf.get('investments')),
            'cf_otherCashflowsFromInvestingActivities': safe_float(cf.get('otherCashflowsFromInvestingActivities')),
            'cf_dividendsPaid':          safe_float(cf.get('dividendsPaid')),
            'cf_salePurchaseOfStock':    safe_float(cf.get('salePurchaseOfStock')),
            'cf_netBorrowings':          safe_float(cf.get('netBorrowings')),
            'cf_otherCashflowsFromFinancingActivities': safe_float(cf.get('otherCashflowsFromFinancingActivities')),
            'cf_changeInCashAndCashEquivalents': safe_float(cf.get('changeInCashAndCashEquivalents')),
            'cf_changeToLiabilities':    safe_float(cf.get('changeToLiabilities')),
            'cf_changeToNetincome':      safe_float(cf.get('changeToNetincome')),
            'cf_changeToOperatingActivities': safe_float(cf.get('changeToOperatingActivities')),
            'cf_changeToAccountReceivables': safe_float(cf.get('changeToAccountReceivables')),
            'cf_changeToInventory':      safe_float(cf.get('changeToInventory')),
            'cf_totalCashFromOperations':safe_float(cf.get('totalCashFromOperations')),
            'cf_issuanceOfCapitalStock': safe_float(cf.get('issuanceOfCapitalStock')),
            'cf_freeCashFlow':           safe_float(cf.get('freeCashFlow')),
            'cf_beginPeriodCashFlow':    safe_float(cf.get('beginPeriodCashFlow')),
            'cf_endPeriodCashFlow':      safe_float(cf.get('endPeriodCashFlow')),
            'cf_effectOfExchangeRate':   safe_float(cf.get('effectOfExchangeRate')),
            'cf_exchangeRateChanges':    safe_float(cf.get('exchangeRateChanges')),
            'cf_netIncome':              safe_float(cf.get('netIncome')),
            'cf_otherNonCashItems':      safe_float(cf.get('otherNonCashItems')),
            'cf_stockBasedCompensation': safe_float(cf.get('stockBasedCompensation')),
            'cf_totalCashFromFinancingActivities': safe_float(cf.get('totalCashFromFinancingActivities')),
            'cf_totalCashflowsFromInvestingActivities': safe_float(cf.get('totalCashflowsFromInvestingActivities')),
            # Highlights / Valuation (snapshot - same for all quarters, some stale)
            'analyst_buy':    ar_buy,   'analyst_hold':  ar_hold,
            'analyst_rating': ar_rating,'analyst_sell':  ar_sell,
            'analyst_strong_buy': ar_sbuy,'analyst_strong_sell': ar_ssell,
            'analyst_target_price': ws_target,'analyst_total': ar_total,
            'book_value':          book_val,
            'diluted_eps_ttm':     eps_ttm,
            'dividend_share':      div_share,
            'dividend_yield':      div_yield,
            'earnings_before_after_market': None,
            'earnings_eps_actual':    None,
            'earnings_eps_difference':None,
            'earnings_eps_estimate':  None,
            'earnings_quarter_date':  None,
            'earnings_report_date':   None,
            'earnings_share':         None,
            'earnings_surprise_percent': None,
            'earnings_trend_avg':     safe_float(ern_trend_latest.get('earningsEstimateAvg')),
            'earnings_trend_growth':  safe_float(ern_trend_latest.get('earningsEstimateGrowth')),
            'earnings_trend_high':    safe_float(ern_trend_latest.get('earningsEstimateHigh')),
            'earnings_trend_low':     safe_float(ern_trend_latest.get('earningsEstimateLow')),
            'earnings_trend_num_analysts': safe_float(ern_trend_latest.get('earningsEstimateNumberOfAnalysts')),
            'ebitda':                 ebitda_hl,
            'enterprise_value':       ev_val,
            'enterprise_value_ebitda':ev_eb,
            'enterprise_value_revenue':ev_rev,
            'eps_estimate_current_quarter': None,
            'eps_estimate_current_year':    None,
            'eps_estimate_next_quarter':    None,
            'eps_estimate_next_year':       None,
            'eps_revisions_down_30d': None,'eps_revisions_down_7d': None,
            'eps_revisions_up_30d':   None,'eps_revisions_up_7d':   None,
            'forward_pe':             fwd_pe,
            'gross_profit_ttm':       gp_ttm,
            'market_cap':             market_cap,
            'market_cap_mln':         mktcap_mln,
            'most_recent_quarter':    mrq,
            'operating_margin_ttm':   op_mg_ttm,
            'pe_ratio':               pe_ratio,
            'peg_ratio':              peg_ratio,
            'price_book_mrq':         pb_mrq,
            'price_sales_ttm':        ps_ttm_hl,
            'profit_margin':          profit_mg,
            'quarterly_earnings_growth_yoy': qtr_eg_yoy,
            'quarterly_revenue_growth_yoy':  qtr_rv_yoy,
            'return_on_assets_ttm':   roa_ttm,
            'return_on_equity_ttm':   roe_ttm,
            'revenue_estimate_avg':   None,'revenue_estimate_growth': None,
            'revenue_estimate_high':  None,'revenue_estimate_low':    None,
            'revenue_per_share_ttm':  rev_ps_ttm,
            'revenue_ttm':            rev_ttm,
            'trailing_pe':            trail_pe,
            'wall_street_target_price': ws_target,
        }
        rows.append(row)
    return rows


def fetch_symbol(symbol: str, session: requests.Session) -> tuple[str, list[dict], str | None]:
    """Fetch fundamentals for one symbol. Returns (symbol, rows, error)."""
    ticker = symbol.replace('.US', '')
    try:
        resp = session.get(
            f"{BASE_URL}/fundamentals/{ticker}.US",
            params={"api_token": API_KEY, "fmt": "json"},
            timeout=30
        )
        if resp.status_code == 404:
            return symbol, [], "404_not_found"
        resp.raise_for_status()
        data = resp.json()
        rows = extract_quarterly_rows(symbol, data)
        return symbol, rows, None
    except requests.exceptions.Timeout:
        return symbol, [], "timeout"
    except Exception as e:
        return symbol, [], str(e)[:120]


def ensure_staging_table(con):
    """Create staging table if not exists — same schema as PROD_EOD_Fundamentals."""
    col_defs = ", ".join(
        f"{c} VARCHAR" if c in ('symbol','date','filing_date','company_name','exchange',
                                'country','sector','industry','gic_sector','gic_group',
                                'gic_industry','gic_sub_industry','most_recent_quarter',
                                'earnings_before_after_market','earnings_quarter_date',
                                'earnings_report_date')
        else f"{c} DOUBLE"
        for c in COLS
    )
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS STAGING_EOD_Fundamentals_Pre2007 (
            {col_defs},
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS STAGING_Pull_Log (
            symbol VARCHAR PRIMARY KEY,
            status VARCHAR,  -- 'ok','no_data','error','skipped'
            rows_pulled INTEGER,
            error_msg VARCHAR,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def run(target_syms: list[str], dry_run: bool = False, limit: int = None, workers: int = 3):
    if limit:
        target_syms = target_syms[:limit]

    con = duckdb.connect(MIRROR_DB)
    ensure_staging_table(con)

    # Which symbols already processed?
    done = set(r[0] for r in con.execute(
        "SELECT symbol FROM STAGING_Pull_Log WHERE status IN ('ok','no_data','error')"
    ).fetchall())
    remaining = [s for s in target_syms if s not in done]

    log.info(f"Target: {len(target_syms)} | Already done: {len(done)} | Remaining: {len(remaining)}")
    if not remaining:
        log.info("Nothing to do — all symbols already processed.")
        con.close()
        return

    if dry_run:
        log.info(f"[DRY RUN] Would pull {len(remaining)} symbols. First 5: {remaining[:5]}")
        con.close()
        return

    total_rows = 0
    ok_count = errors = no_data = 0
    t_start = time.time()
    batch = []

    session = requests.Session()
    session.headers.update({"User-Agent": "OBQ-Research/1.0"})

    def flush_batch(b):
        if not b:
            return
        import pandas as pd
        df = pd.DataFrame(b, columns=COLS + ['pulled_at'])
        # Ensure VARCHAR cols are string
        for c in ('symbol','date','filing_date','company_name','exchange','country',
                  'sector','industry','gic_sector','gic_group','gic_industry',
                  'gic_sub_industry','most_recent_quarter','earnings_before_after_market',
                  'earnings_quarter_date','earnings_report_date'):
            if c in df.columns:
                df[c] = df[c].astype(str).where(df[c].notna(), None)
        con.execute("INSERT INTO STAGING_EOD_Fundamentals_Pre2007 SELECT * FROM df")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_symbol, sym, session): sym for sym in remaining}
        for i, fut in enumerate(as_completed(futures), 1):
            sym, rows, err = fut.result()

            if err == '404_not_found':
                status, msg = 'no_data', None
                no_data += 1
            elif err:
                status, msg = 'error', err
                errors += 1
                log.warning(f"  ERROR {sym}: {err}")
            elif not rows:
                status, msg = 'no_data', None
                no_data += 1
            else:
                status, msg = 'ok', None
                ok_count += 1
                total_rows += len(rows)
                for r in rows:
                    r['pulled_at'] = datetime.utcnow().isoformat()
                    # Ensure all cols present
                    for c in COLS:
                        if c not in r:
                            r[c] = None
                batch.extend(rows)

            # Log to pull log
            now = datetime.utcnow().isoformat()
            con.execute("""
                INSERT OR REPLACE INTO STAGING_Pull_Log (symbol, status, rows_pulled, error_msg, pulled_at)
                VALUES (?, ?, ?, ?, ?)
            """, [sym, status, len(rows), msg, now])

            # Flush batch every 50 symbols
            if len(batch) >= 2000:
                flush_batch(batch)
                batch = []

            # Progress every 50
            if i % 50 == 0 or i == len(remaining):
                elapsed = round(time.time() - t_start)
                rate = i / max(elapsed, 1)
                eta = round((len(remaining) - i) / max(rate, 0.1))
                log.info(f"  {i}/{len(remaining)} | ok={ok_count} no_data={no_data} err={errors} "
                         f"rows={total_rows:,} | {elapsed}s elapsed | ETA {eta//60}m{eta%60}s")

            time.sleep(0.8)  # ~1.25 req/sec — safe rate to avoid 429s

    # Final flush
    flush_batch(batch)

    elapsed = round(time.time() - t_start)
    log.info(f"\n{'='*60}")
    log.info(f"PULL COMPLETE in {elapsed//60}m{elapsed%60}s")
    log.info(f"  Symbols: {ok_count} ok | {no_data} no_data | {errors} errors")
    log.info(f"  Total rows staged: {total_rows:,}")

    # Summary of what's in staging
    staged = con.execute("""
        SELECT COUNT(*) as rows, COUNT(DISTINCT symbol) as syms,
               MIN(date) as min_d, MAX(date) as max_d
        FROM STAGING_EOD_Fundamentals_Pre2007
    """).fetchone()
    log.info(f"  Staging table: {staged[0]:,} rows | {staged[1]:,} symbols | {staged[2]} → {staged[3]}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of symbols (for testing)")
    ap.add_argument("--workers", type=int, default=3, help="Parallel API workers (default 3)")
    ap.add_argument("--targets", type=str, default=r'C:\Users\admin\AppData\Local\Temp\opencode\r3k_targets.json')
    args = ap.parse_args()

    with open(args.targets) as f:
        targets = json.load(f)

    log.info(f"Starting R3000 fundamentals pull: {len(targets)} target symbols")
    run(targets, dry_run=args.dry_run, limit=args.limit, workers=args.workers)
