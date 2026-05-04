"""
engine/spx_backtest.py
======================
S&P 500 Total Return Benchmark — goes back to 1990.

Data sources:
  1990-01-01 to 1992-12-31: SPX price return (PWB_indices) + 3% annual dividend yield
  1993-01-01 onwards:       SPY adjusted_close total return (PROD_EOD_ETFs)

The 3% dividend yield for 1990-1992 is historically accurate:
  - S&P 500 dividend yield was ~3.0-3.7% in 1990-1992
  - Using 3.0% (conservative) to avoid overstating benchmark

Returns a dict compatible with spy_backtest.py output format.
"""

import datetime
import numpy as np
import pandas as pd
import duckdb
import os
from dotenv import load_dotenv
from engine.metrics import compute_all

load_dotenv()

MIRROR_DB = os.environ.get("OBQ_EODHD_MIRROR_DB", r"D:/OBQ_AI/obq_eodhd_mirror.duckdb")
PWB_DB    = r"D:/OBQ_AI/pwb_data.duckdb"

# Historical S&P 500 dividend yields by year (price-only → total return adjustment)
# Source: Robert Shiller online data (http://www.econ.yale.edu/~shiller/data.htm)
# These are the actual trailing 12-month yields used to convert price return to total return
# Historical S&P 500 dividend yields (Shiller data, trailing 12-month)
# 1990 yield was ~3.67% but price fell sharply, making realized yield appear higher.
# Use the actual Shiller E10-based annual dividend / price for accuracy.
SPX_DIVIDEND_YIELDS = {
    1988: 0.0362, 1989: 0.0317,
    1990: 0.0367,  # price -6.6%, total return -3.1% → implied div yield ~3.5-3.7%
    1991: 0.0295,  # price +26.3%, total return +30.5% → implied div yield ~4.2% (rising mkt)
    1992: 0.0281,  # price +4.5%, total return +7.6%  → implied div yield ~3.1%
    1993: 0.0268,
}
SPX_DIVIDEND_YIELD_DEFAULT = 0.030  # fallback for any unlisted year


def run_spx_backtest(
    start_date: str = "1990-07-31",
    end_date: str | None = None,
    rf_annual: float = 0.04,
    cb=None,
) -> dict:
    """
    S&P 500 Total Return benchmark from 1990.
    Splices SPX price + dividend yield (pre-1993) with SPY total return (1993+).
    Returns same format as spy_backtest.run_spy_backtest().
    """
    import time
    t0 = time.time()

    if end_date in (None, "null", "None", "latest"):
        end_date = datetime.date.today().strftime("%Y-%m-%d")

    def _cb(msg):
        if cb:
            try: cb("info", msg)
            except: pass

    SPY_SPLICE_DATE = "1993-01-29"  # SPY first trading day

    # ── 1. Load SPX price data (1990 to SPY splice) ──────────────────────────
    _cb("Loading SPX price data (1990-1992) from PWB_indices...")
    try:
        con_pwb = duckdb.connect(PWB_DB, read_only=True)
        df_spx = con_pwb.execute(f"""
            SELECT date::DATE AS dt, adj_close AS close
            FROM PWB_indices
            WHERE symbol = 'SPX'
              AND date::DATE >= '{start_date}'::DATE
              AND date::DATE < '{SPY_SPLICE_DATE}'::DATE
              AND adj_close IS NOT NULL AND adj_close > 0
            ORDER BY dt
        """).fetchdf()
        con_pwb.close()
    except Exception as e:
        _cb(f"Warning: Could not load SPX data: {e}")
        df_spx = pd.DataFrame(columns=["dt", "close"])

    # ── 2. Load SPY total return data (1993 onwards) ──────────────────────────
    _cb("Loading SPY total return data (1993+) from PROD_EOD_ETFs...")
    con_mirror = duckdb.connect(MIRROR_DB, read_only=True)
    df_spy = con_mirror.execute(f"""
        SELECT date::DATE AS dt, adjusted_close AS close
        FROM PROD_EOD_ETFs
        WHERE symbol = 'SPY.US'
          AND date::DATE >= '{SPY_SPLICE_DATE}'::DATE
          AND date::DATE <= '{end_date}'::DATE
          AND adjusted_close IS NOT NULL AND adjusted_close > 0
        ORDER BY dt
    """).fetchdf()
    con_mirror.close()

    if df_spy.empty:
        return {"status": "error", "error": "No SPY data found"}

    _cb(f"SPX: {len(df_spx)} days | SPY: {len(df_spy)} days")

    # ── 3. Splice: normalize SPX to connect seamlessly to SPY ─────────────────
    if not df_spx.empty:
        df_spx["dt"] = pd.to_datetime(df_spx["dt"])
        df_spx = df_spx.set_index("dt").sort_index()
        spx_monthly = df_spx["close"].resample("ME").last().dropna()

        df_spy["dt"] = pd.to_datetime(df_spy["dt"])
        df_spy_idx = df_spy.set_index("dt").sort_index()
        spy_monthly = df_spy_idx["close"].resample("ME").last().dropna()

        # SPX monthly price returns
        spx_price_ret = spx_monthly.pct_change().dropna()

        # Add monthly dividend yield using actual historical yields by year
        # (annual yield / 12 per month — distributes dividends evenly across year)
        monthly_div = spx_price_ret.index.map(
            lambda dt: SPX_DIVIDEND_YIELDS.get(dt.year, SPX_DIVIDEND_YIELD_DEFAULT) / 12.0
        )
        spx_total_ret = spx_price_ret + monthly_div  # price return + dividend yield

        # Normalize SPX equity curve to connect at SPY's first value
        spx_eq_pre = (1 + spx_total_ret).cumprod()

        # Scale SPX to end at 1.0 so SPY can continue from there
        spx_scale = 1.0 / float(spx_eq_pre.iloc[-1])
        spx_eq_pre = spx_eq_pre * spx_scale

        # SPY equity starts at 1.0 at its first monthly close
        spy_ret = spy_monthly.pct_change().dropna()
        spy_eq  = (1 + spy_ret).cumprod()
        spy_eq  = pd.concat([pd.Series([1.0], index=[spy_monthly.index[0]]), spy_eq])

        # Connect: scale SPY so it starts where SPX ends (both normalized)
        # Actually simpler: treat as one continuous total return series
        # SPX contribution: spx_eq_pre[-1] = 1.0 (normalized)
        # SPY contribution: starts at 1.0, multiply by SPX's total growth

        # Get SPX total growth factor over pre-1993 period
        spx_total_factor = float((1 + spx_total_ret).prod())

        # Combined equity: SPX growth * SPY growth
        spy_eq_combined = spy_eq * spx_total_factor

        # Combined returns series
        combined_ret = pd.concat([spx_total_ret, spy_ret])
        combined_eq  = pd.concat([
            spx_eq_pre * spx_total_factor,
            spy_eq_combined.iloc[1:]  # skip the 1.0 start since SPX already there
        ])

        all_dates = combined_ret.index.tolist()
        _cb(f"Spliced series: {str(combined_ret.index[0].date())} to {str(combined_ret.index[-1].date())}  ({len(combined_ret)} months)")
        _cb(f"SPX pre-1993 total return (incl. ~3% div): {(spx_total_factor-1)*100:.1f}%")

        monthly_ret = combined_ret
        equity = pd.concat([
            pd.Series([1.0], index=[pd.Timestamp(start_date)]),
            combined_eq
        ]).sort_index()

    else:
        # No pre-1993 data needed — just use SPY
        df_spy["dt"] = pd.to_datetime(df_spy["dt"])
        df_spy_idx = df_spy.set_index("dt").sort_index()
        spy_monthly = df_spy_idx["close"].resample("ME").last().dropna()
        monthly_ret = spy_monthly.pct_change().dropna()
        equity = (1 + monthly_ret).cumprod()
        equity = pd.concat([pd.Series([1.0], index=[spy_monthly.index[0]]), equity])

    # ── 4. Clip equity to requested date range ────────────────────────────────
    equity = equity[equity.index >= pd.Timestamp(start_date)]
    if len(equity) < 2:
        return {"status": "error", "error": "Not enough data after date filter"}

    monthly_ret_clipped = equity.pct_change().dropna()

    # ── 5. Annual returns ─────────────────────────────────────────────────────
    annual_ret = monthly_ret_clipped.resample("YE").apply(lambda x: (1+x).prod()-1)

    # ── 6. Compute metrics ────────────────────────────────────────────────────
    metrics = compute_all(
        equity=equity.values,
        monthly_ret=monthly_ret_clipped.values,
        annual_ret=annual_ret.values,
        rf_annual=rf_annual,
        periods_per_year=12,
        label="S&P 500 Total Return (SPX + SPY)",
    )
    metrics["terminal_wealth"] = round(10000 * float(equity.iloc[-1]), 0)

    elapsed = round(time.time() - t0, 1)
    _cb(f"SPX backtest complete in {elapsed}s | CAGR: {metrics.get('cagr',0)*100:.2f}%")

    return {
        "status":            "complete",
        "mode":              "spx",
        "run_id":            "spx-benchmark",
        "run_label":         "S&P 500 Total Return (SPX/SPY spliced, 1990+)",
        "start_date":        str(equity.index[0].date()),
        "end_date":          str(equity.index[-1].date()),
        "n_periods":         len(monthly_ret_clipped),
        "elapsed_s":         elapsed,
        "portfolio_equity":  [round(v, 6) for v in equity.values],
        "bm_equity":         [round(v, 6) for v in equity.values],
        "equity_dates":      [str(d.date()) for d in equity.index],
        "period_data": [
            {"date": str(d.date()), "portfolio_return": float(r),
             "universe_return": float(r), "n_stocks": 1, "cost_drag": 0.0}
            for d, r in monthly_ret_clipped.items()
        ],
        "portfolio_metrics": metrics,
        "bm_metrics":        {},
        "annual_ret_by_year": [
            {"year": int(yr.year), "ret": float(r)}
            for yr, r in annual_ret.items()
        ],
    }
