"""Data loading layer — reads from obq_eodhd_mirror.duckdb (local PROD mirror)."""
import os
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict
from datetime import date

import duckdb
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("factorlab.data")

# ── Paths ─────────────────────────────────────────────────────────────────
LOCAL_MIRROR = os.environ.get("OBQ_EODHD_MIRROR_DB", r"D:/OBQ_AI/obq_eodhd_mirror.duckdb")
LOCAL_AI     = os.environ.get("OBQ_AI_DB",            r"D:/OBQ_AI/obq_ai.duckdb")
BACKTEST_DB  = os.environ.get("BACKTEST_DB",           r"D:/OBQ_AI/backtest.duckdb")

FACTOR_MAP = {
    "value":    ("PROD_OBQ_Value_Scores",    "value_score_composite"),
    "quality":  ("PROD_OBQ_Quality_Scores",  "quality_score_composite"),
    "growth":   ("PROD_OBQ_Growth_Scores",   "growth_score_composite"),
    "finstr":   ("PROD_OBQ_FinStr_Scores",   "finstr_score_composite"),
    "momentum": ("PROD_OBQ_Momentum_Scores", "momentum_score_composite"),
    "jcn":      ("PROD_JCN_Composite_Scores","jcn_full_composite"),
    "qarp":     ("PROD_JCN_Composite_Scores","jcn_qarp"),
    "garp":     ("PROD_JCN_Composite_Scores","jcn_garp"),
    "longeq":   ("PROD_LONGEQ_SCORES",       "longeq_score"),
    "moat":     ("PROD_MOAT_SCORES",         "moat_score"),
    "rulebreaker": ("PROD_RULEBREAKER_SCORES","rulebreaker_score"),
    "fundsmith":("PROD_FUNDSMITH_SCORES",    "fundsmith_score"),
}

SECTOR_CHOICES = [
    "Energy", "Materials", "Industrials", "Consumer Discretionary",
    "Consumer Staples", "Health Care", "Financials", "Information Technology",
    "Communication Services", "Utilities", "Real Estate",
]

INDEX_CHOICES = [
    "Russell 3000 (Broad)",
    "Russell 1000 (Large Cap)",
    "Russell 2000 (Small Cap)",
    "S&P 500",
    "Top 1500 by Market Cap",
    "OBQ Investable Universe (Top 3000)",
]

_RECON_MAP = {
    "Russell 3000 (Broad)":            (3000, None),
    "Russell 1000 (Large Cap)":        (1000, None),
    "Russell 2000 (Small Cap)":        (2000, None),
    "S&P 500":                         (500,  None),
    "Top 1500 by Market Cap":          (1500, None),
    "OBQ Investable Universe (Top 3000)": (3000, None),
}


@dataclass
class UniverseConfig:
    index: str = "OBQ Investable Universe (Top 3000)"
    factor: str = "value"
    start_date: str = "1990-07-31"
    end_date:   str = "latest"
    sector_exclusions: List[str] = None
    min_market_cap_B: Optional[float] = None   # $B
    min_price: float = 2.0
    liquidity_adv_M: Optional[float] = None     # $M avg daily vol
    na_handling: str = "Exclude"               # Exclude / Worst / Neutral
    winsorize: bool = True                     # 1-99 percentile clip
    sector_neutral: bool = False
    rebalance: str = "Monthly"                 # Monthly / Quarterly / Semi-Annual / Annual
    direction: str = "Long Only"              # Long Only / Long-Short
    # Model type specifics
    model_type: str = "quintile"              # quintile | topn
    n_quintiles: int = 5
    top_n: int = 30
    position_sizing: str = "Equal"           # Equal / Vol-Parity
    commission_bps: float = 5.0
    slippage_bps: float = 10.0
    initial_capital: float = 1_000_000.0
    rf_annual: float = 0.04


def open_mirror(read_only=True):
    return duckdb.connect(LOCAL_MIRROR, read_only=read_only)


def get_score_table_range(factor: str) -> dict:
    """Return min/max month_date for the given factor."""
    tbl, col = FACTOR_MAP.get(factor.lower(), (None, None))
    if not tbl:
        return {}
    try:
        c = open_mirror()
        r = c.execute(f'SELECT MIN(month_date::DATE), MAX(month_date::DATE), COUNT(DISTINCT symbol) FROM "{tbl}"').fetchone()
        c.close()
        return {"min_date": str(r[0])[:10], "max_date": str(r[1])[:10], "symbols": r[2]}
    except Exception as e:
        return {"error": str(e)}


def get_available_factors() -> List[Dict]:
    """Return list of factors with their date ranges."""
    c = open_mirror()
    result = []
    for key, (tbl, col) in FACTOR_MAP.items():
        try:
            r = c.execute(f'SELECT MIN(month_date::DATE), MAX(month_date::DATE), COUNT(*) FROM "{tbl}"').fetchone()
            result.append({"key": key, "table": tbl, "col": col,
                           "min_date": str(r[0])[:10], "max_date": str(r[1])[:10], "rows": r[2]})
        except:
            pass
    c.close()
    return result


def load_scores(cfg: UniverseConfig, cb=None) -> pd.DataFrame:
    """
    Load factor scores filtered to universe.
    Returns: DataFrame with (symbol, month_date, score, gic_sector)
    """
    tbl, col = FACTOR_MAP.get(cfg.factor.lower(), ("PROD_OBQ_Value_Scores", "value_score_composite"))
    if cb: cb("info", f"Loading {tbl}...")
    c = open_mirror()

    end_dt = date.today().strftime("%Y-%m-%d") if (cfg.end_date in (None, "latest", "null", "None")) else cfg.end_date
    start_dt = cfg.start_date

    # Momentum uses bare tickers -- handle symbol format
    is_momentum = cfg.factor.lower() in ("momentum",)

    q = f"""
    SELECT
        u.symbol,
        s.month_date::DATE AS month_date,
        {"s." + col} AS score,
        s.gic_sector,
        u.recon_year,
        u.gics_sector AS universe_sector,
        u.market_cap_at_recon,
        u.rank_at_recon
    FROM PROD_OBQ_Investable_Universe u
    JOIN "{tbl}" s
        ON {"REPLACE(s.symbol,'.US','')=REPLACE(u.symbol,'.US','')" if is_momentum else "u.symbol = s.symbol"}
        AND s.month_date::DATE BETWEEN u.effective_start::DATE AND u.effective_end::DATE
    WHERE s.month_date::DATE BETWEEN '{start_dt}'::DATE AND '{end_dt}'::DATE
      AND s.{col} IS NOT NULL
    """

    if cfg.sector_exclusions:
        excl = "', '".join(cfg.sector_exclusions)
        q += f"  AND COALESCE(s.gic_sector, u.gics_sector) NOT IN ('{excl}')\n"

    if cfg.min_market_cap_B:
        q += f"  AND u.market_cap_at_recon >= {cfg.min_market_cap_B * 1e9}\n"

    # Universe size filter
    top_n, _ = _RECON_MAP.get(cfg.index, (3000, None))
    q += f"  AND u.rank_at_recon <= {top_n}\n"
    q += "ORDER BY s.month_date, s." + col + " DESC"

    df = c.execute(q).fetchdf()
    c.close()

    if cb: cb("info", f"Loaded {len(df):,} score records across {df['symbol'].nunique():,} symbols")
    return df


def load_prices(symbols: List[str], start_date: str, end_date: str, cb=None) -> pd.DataFrame:
    """Load monthly prices from PROD_EOD_survivorship."""
    if cb: cb("info", "Loading price data...")
    c = open_mirror()
    sym_list = "', '".join(symbols[:5000])
    q = f"""
    SELECT symbol, date::DATE AS price_date, adjusted_close
    FROM PROD_EOD_survivorship
    WHERE symbol IN ('{sym_list}')
      AND date::DATE BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE
      AND adjusted_close IS NOT NULL AND adjusted_close > 0.01
    ORDER BY symbol, date
    """
    df = c.execute(q).fetchdf()
    c.close()
    if cb: cb("info", f"Loaded {len(df):,} price records")
    return df


def build_monthly_price_matrix(prices_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot to (month_end, symbol) price matrix."""
    p = prices_df.copy()
    p["ym"] = pd.to_datetime(p["price_date"]).dt.to_period("M")
    # Last close per symbol per month
    last_p = p.sort_values("price_date").groupby(["symbol", "ym"])["adjusted_close"].last().reset_index()
    # Canonical month-end date
    canonical = last_p.groupby("ym").apply(
        lambda g: p[p["ym"] == g.name]["price_date"].max()
    ).reset_index(name="canon")
    last_p = last_p.merge(canonical, on="ym")
    pivot = last_p.pivot_table(index="canon", columns="symbol", values="adjusted_close", aggfunc="last")
    pivot.index = pd.to_datetime(pivot.index)
    pivot.sort_index(inplace=True)
    return pivot


def compute_forward_returns(price_matrix: pd.DataFrame, periods: int = 1) -> pd.DataFrame:
    """Compute N-period forward returns from price matrix."""
    return price_matrix.pct_change(periods).shift(-periods)
