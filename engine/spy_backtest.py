"""Lightweight SPY long-only benchmark backtest.
Reads PROD_EOD_survivorship for SPY.US directly — no factor engine needed.
Returns full tearsheet metrics + monthly equity curve in <5 seconds.
"""
import datetime
import numpy as np
import pandas as pd
import duckdb
import os
from dotenv import load_dotenv
from engine.metrics import compute_all

load_dotenv()
MIRROR = os.environ.get("OBQ_EODHD_MIRROR_DB", r"D:/OBQ_AI/obq_eodhd_mirror.duckdb")


def run_spy_backtest(
    start_date: str = "1993-01-29",
    end_date: str | None = None,
    rf_annual: float = 0.04,
    cb=None,
) -> dict:
    """
    S&P 500 total return benchmark.
    If start_date < 1993-01-29, automatically delegates to spx_backtest
    which splices SPX price + historical dividend yield for pre-SPY period.
    """
    # Auto-route to SPX splice if start pre-dates SPY
    if start_date and start_date < "1993-01-29":
        try:
            from engine.spx_backtest import run_spx_backtest
            return run_spx_backtest(start_date=start_date, end_date=end_date,
                                    rf_annual=rf_annual, cb=cb)
        except Exception as e:
            # Fall through to SPY-only if SPX data unavailable
            if cb:
                try: cb("info", f"SPX splice failed ({e}), using SPY from 1993")
                except: pass
    import time
    t0 = time.time()

    if end_date in (None, "null", "None", "latest"):
        end_date = datetime.date.today().strftime("%Y-%m-%d")

    def _cb(msg):
        if cb:
            try: cb("info", msg)
            except: pass

    _cb("Loading SPY price data from PROD_EOD_survivorship...")
    con = duckdb.connect(MIRROR, read_only=True)

    # Pull SPY daily adjusted close
    df = con.execute(f"""
        SELECT date::DATE AS dt, adjusted_close AS close
        FROM PROD_EOD_ETFs
        WHERE symbol = 'SPY.US'
          AND date::DATE BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE
          AND adjusted_close IS NOT NULL
          AND adjusted_close > 0.01
        ORDER BY dt
    """).fetchdf()
    con.close()

    if df.empty:
        return {"status": "error", "error": "No SPY price data found"}

    _cb(f"Loaded {len(df):,} SPY daily bars  {df['dt'].min()} -> {df['dt'].max()}")

    # Monthly resample — last close of month
    df["dt"] = pd.to_datetime(df["dt"])
    df = df.set_index("dt").sort_index()
    monthly = df["close"].resample("ME").last().dropna()

    if len(monthly) < 12:
        return {"status": "error", "error": f"Not enough monthly data: {len(monthly)} months"}

    # Monthly returns
    monthly_ret = monthly.pct_change().dropna()

    # Equity curve (starting at 1.0)
    equity = (1 + monthly_ret).cumprod()
    equity = pd.concat([pd.Series([1.0], index=[monthly.index[0]]), equity])

    # Annual returns
    annual_ret = monthly_ret.groupby(monthly_ret.index.year).apply(
        lambda x: (1 + x).prod() - 1
    )

    _cb(f"Computing tearsheet metrics  ({len(monthly_ret)} monthly periods)...")

    metrics = compute_all(
        equity=equity.values,
        monthly_ret=monthly_ret.values,
        annual_ret=annual_ret.values,
        rf_annual=rf_annual,
        periods_per_year=12,
        label="SPY Long-Only",
    )

    # Monthly heatmap
    heatmap = _build_heatmap(monthly_ret)

    elapsed = round(time.time() - t0, 1)
    _cb(f"SPY backtest complete in {elapsed}s")

    return {
        "status":            "complete",
        "mode":              "spy",
        "run_id":            "spy-benchmark",
        "run_label":         "SPY Long-Only Benchmark",
        "factor":            "spy",
        "start_date":        str(equity.index[0].date()),
        "end_date":          str(equity.index[-1].date()),
        "n_periods":         len(monthly_ret),
        "gpu_used":          False,
        "elapsed_s":         elapsed,
        # Equity curve
        "portfolio_equity":  [round(v, 6) for v in equity.values],
        "bm_equity":         [round(v, 6) for v in equity.values],
        "equity_dates":      [str(d.date()) for d in equity.index],
        # Period data for table
        "period_data": [
            {"date": str(d.date()), "portfolio_return": float(r),
             "universe_return": float(r), "n_stocks": 1, "cost_drag": 0.0}
            for d, r in monthly_ret.items()
        ],
        # Metrics
        "portfolio_metrics": metrics,
        "bm_metrics":        {},
        "monthly_heatmap":   heatmap,
        # Annual returns for bar chart [{year, ret}]
        "annual_ret_by_year": [
            {"year": int(yr), "ret": float(r)}
            for yr, r in annual_ret.items()
        ],
    }


def run_benchmark(
    symbol: str,
    start_date: str,
    end_date: str | None = None,
    rf_annual: float = 0.04,
    cb=None,
) -> dict:
    """
    Generic ETF benchmark backtest.
    Supports: QQQ.US (Nasdaq 100), MDY.US (S&P 400 Mid-Cap),
              IWM.US (Russell 2000 Small-Cap), SPY.US (S&P 500)
    Returns same format as run_spy_backtest().
    """
    import time
    t0 = time.time()

    if end_date in (None, "null", "None", "latest"):
        end_date = datetime.date.today().strftime("%Y-%m-%d")

    def _cb(msg):
        if cb:
            try: cb("info", msg)
            except: pass

    LABELS = {
        "QQQ.US": "Nasdaq 100 (QQQ)",
        "MDY.US": "S&P 400 Mid-Cap (MDY)",
        "IWM.US": "Russell 2000 Small-Cap (IWM)",
        "SPY.US": "S&P 500 (SPY)",
        "IVV.US": "S&P 500 (IVV)",
        "DIA.US": "Dow Jones (DIA)",
    }
    label = LABELS.get(symbol, symbol)

    _cb(f"Loading {label} from {start_date}...")
    con = duckdb.connect(MIRROR, read_only=True)

    df = con.execute(f"""
        SELECT date::DATE AS dt, adjusted_close AS close
        FROM PROD_EOD_ETFs
        WHERE symbol = '{symbol}'
          AND date::DATE BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE
          AND adjusted_close IS NOT NULL AND adjusted_close > 0.01
        ORDER BY dt
    """).fetchdf()
    con.close()

    if df.empty:
        return {"status": "error", "error": f"No data found for {symbol}"}

    _cb(f"Loaded {len(df):,} {symbol} daily bars  {df['dt'].min()} -> {df['dt'].max()}")

    df["dt"] = pd.to_datetime(df["dt"])
    df = df.set_index("dt").sort_index()
    monthly = df["close"].resample("ME").last().dropna()

    if len(monthly) < 12:
        return {"status": "error", "error": f"Not enough data for {symbol}: {len(monthly)} months"}

    monthly_ret = monthly.pct_change().dropna()
    equity = (1 + monthly_ret).cumprod()
    equity = pd.concat([pd.Series([1.0], index=[monthly.index[0]]), equity])

    annual_ret = monthly_ret.groupby(monthly_ret.index.year).apply(
        lambda x: (1 + x).prod() - 1
    )

    metrics = compute_all(
        equity=equity.values,
        monthly_ret=monthly_ret.values,
        annual_ret=annual_ret.values,
        rf_annual=rf_annual,
        periods_per_year=12,
        label=label,
    )
    metrics["terminal_wealth"] = round(10000 * float(equity.iloc[-1]), 0)

    heatmap = _build_heatmap(monthly_ret)
    elapsed = round(time.time() - t0, 1)
    _cb(f"{label} complete in {elapsed}s | CAGR: {metrics.get('cagr',0)*100:.2f}%")

    return {
        "status":            "complete",
        "mode":              "benchmark",
        "run_id":            symbol.replace(".", "_").lower() + "-benchmark",
        "run_label":         label,
        "start_date":        str(equity.index[0].date()),
        "end_date":          str(equity.index[-1].date()),
        "n_periods":         len(monthly_ret),
        "elapsed_s":         elapsed,
        "portfolio_equity":  [round(v, 6) for v in equity.values],
        "bm_equity":         [round(v, 6) for v in equity.values],
        "equity_dates":      [str(d.date()) for d in equity.index],
        "period_data": [
            {"date": str(d.date()), "portfolio_return": float(r),
             "universe_return": float(r), "n_stocks": 1, "cost_drag": 0.0}
            for d, r in monthly_ret.items()
        ],
        "portfolio_metrics": metrics,
        "bm_metrics":        {},
        "monthly_heatmap":   heatmap,
        "annual_ret_by_year": [
            {"year": int(yr), "ret": float(r)}
            for yr, r in annual_ret.items()
        ],
    }


def _build_heatmap(monthly_ret: pd.Series) -> dict:
    rows = {}
    for dt, r in monthly_ret.items():
        rows.setdefault(dt.year, {})[dt.month] = round(float(r) * 100, 2)
    years = sorted(rows.keys())
    months = list(range(1, 13))
    data = [[rows.get(yr, {}).get(mo) for mo in months] for yr in years]
    return {"years": years, "months": months, "data": data}
