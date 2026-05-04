"""
engine/portfolio_backtest.py
============================
OBQ FactorLab — Portfolio Model Backtest Engine

Strategy: rank stocks by score → buy Top-N → hold until next rebalance
  OR stop-loss is hit per-position.

Key features vs factor_backtest.py:
  - Equal-weight Top-N portfolio (not quintile bins)
  - Sector concentration cap (max K stocks per sector)
  - Optional per-position stop-loss (% from entry)
  - SPY benchmark comparison
  - Standard tearsheet output (portfolio_equity, portfolio_metrics,
    period_data, monthly_heatmap, annual_ret_by_year, holdings_log)

Data sources: same v_backtest_scores + v_backtest_prices as factor engine.
"""

from __future__ import annotations

import os
import time
import math
import hashlib
import datetime
import traceback
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
from dotenv import load_dotenv

load_dotenv()

MIRROR_DB = os.environ.get(
    "OBQ_EODHD_MIRROR_DB",
    r"D:/OBQ_AI/obq_eodhd_mirror.duckdb"
)

SCORE_COLUMNS = {
    "jcn_full_composite":         "JCN Composite",
    "jcn_qarp":                   "JCN QARP",
    "jcn_garp":                   "JCN GARP",
    "jcn_quality_momentum":       "JCN Quality-Momentum",
    "jcn_value_momentum":         "JCN Value-Momentum",
    "jcn_growth_quality_momentum":"JCN Growth-Quality-Momentum",
    "jcn_fortress":               "JCN Fortress",
    "jcn_alpha_trifecta":         "JCN Alpha Trifecta",
    "value_score":                "Value Score",
    "quality_score":              "Quality Score",
    "growth_score":               "Growth Score",
    "momentum_score":             "Momentum Score",
    "finstr_score":               "Financial Strength",
    "momentum_af_score":          "Momentum (Alpha Factor)",
    "momentum_fip_score":         "Momentum (FIP)",
    "momentum_sys_score":         "Momentum (Systematic)",
    "value_score_universe":       "Value Score (Universe)",
    "quality_score_universe":     "Quality Score (Universe)",
    "growth_score_universe":      "Growth Score (Universe)",
    "finstr_score_universe":      "FinStr Score (Universe)",
    "af_universe_score":          "Alpha Factor (Universe)",
    # Separate-table scores
    "longeq_rank":                "LongEQ Rank",
    "rulebreaker_rank":           "Rulebreaker Rank",
    "fundsmith_rank":             "Fundsmith Rank",
    "moat_score":                 "Moat Score",
    "moat_rank":                  "Moat Rank",
}

# Separate factor tables (same as factor_backtest.py)
SEPARATE_SCORE_TABLES_PM = {
    "longeq_rank":      ("PROD_LONGEQ_SCORES",     "lower_better"),
    "longeq_score":     ("PROD_LONGEQ_SCORES",     "higher_better"),
    "rulebreaker_rank": ("PROD_RULEBREAKER_SCORES","lower_better"),
    "rulebreaker_score":("PROD_RULEBREAKER_SCORES","higher_better"),
    "fundsmith_rank":   ("PROD_FUNDSMITH_SCORES",  "lower_better"),
    "fundsmith_score":  ("PROD_FUNDSMITH_SCORES",  "higher_better"),
    "moat_score":       ("PROD_MOAT_SCORES",       "higher_better"),
    "moat_rank":        ("PROD_MOAT_SCORES",       "lower_better"),
}


@dataclass
class PortfolioBacktestConfig:
    # Score / ranking
    score_column: str = "jcn_full_composite"
    score_direction: str = "higher_better"   # higher_better | lower_better

    # Portfolio construction
    top_n: int = 20                          # number of stocks to hold
    sector_max: int = 5                      # max stocks per GIC sector (0 = no limit)
    weight_scheme: str = "equal"             # equal | score_weighted (future)

    # Rebalance schedule
    rebalance_freq: str = "quarterly"        # monthly | quarterly | semi-annual | annual
    start_date: str = "1990-07-31"
    end_date: str = "2024-12-31"

    # Filters
    min_price: float = 5.0
    min_adv_usd: float = 1_000_000.0
    min_market_cap: float = 0.0              # 0 = no filter; 10e9 = $10B+ (top ~1000)
    cap_tier: str = "all"                    # all | micro | small | mid | large | mega
    transaction_cost_bps: float = 15.0       # one-way round-trip in bps

    # Stop loss
    stop_loss_pct: float = 0.0               # 0 = disabled; e.g. 0.20 = 20% stop from entry

    # Label
    run_label: str = ""
    strategy_id: str = ""                    # PM-xxx assigned at save time


CAP_TIERS = {
    "all":   (0, float("inf")),
    "micro": (0, 300e6),
    "small": (300e6, 2e9),
    "mid":   (2e9, 10e9),
    "large": (10e9, 200e9),
    "mega":  (200e9, float("inf")),
}

REBAL_MONTHS = {
    "monthly":     None,                    # every month
    "quarterly":   {3, 6, 9, 12},
    "semi-annual": {6, 12},
    "annual":      {12},
}


def _get_con():
    return duckdb.connect(MIRROR_DB, read_only=True)


def run_portfolio_backtest(cfg: PortfolioBacktestConfig, cb=None) -> dict:
    """
    Run a Top-N portfolio backtest.

    Returns dict with:
      status, run_label, config
      portfolio_equity    — [float]  starting at 1.0
      bm_equity           — [float]  equal-weight universe benchmark
      spy_equity          — [float]  SPY buy-and-hold benchmark
      equity_dates        — [str]
      portfolio_metrics   — full compute_all dict
      bm_metrics          — universe benchmark metrics
      spy_metrics         — SPY benchmark metrics
      period_data         — [{date, portfolio_return, universe_return, n_stocks, cost_drag, holdings}]
      annual_ret_by_year  — [{year, portfolio_ret, universe_ret, spy_ret}]
      monthly_heatmap     — {year: {month: ret%}}
      holdings_log        — [{date, holdings: [{symbol, score, sector, weight}]}]
      n_periods, elapsed_s
    """
    t0 = time.time()

    def _cb(msg):
        if cb:
            try: cb("info", msg)
            except: pass

    try:
        con = _get_con()

        # ── 1. Rebalance dates ───────────────────────────────────────────────
        rebal_months = REBAL_MONTHS.get(cfg.rebalance_freq.lower())
        month_filter = ""
        if rebal_months:
            months_sql = ",".join(str(m) for m in sorted(rebal_months))
            month_filter = f"AND MONTH(month_date) IN ({months_sql})"

        src_tbl = SEPARATE_SCORE_TABLES_PM.get(cfg.score_column, (None,))[0] or "v_backtest_scores"
        dates_rows = con.execute(f"""
            SELECT DISTINCT month_date::VARCHAR as d
            FROM {src_tbl}
            WHERE month_date >= '{cfg.start_date}'::DATE
              AND month_date <= '{cfg.end_date}'::DATE
              AND {cfg.score_column} IS NOT NULL
              {month_filter}
            ORDER BY d
        """).fetchall()
        dates = [r[0] for r in dates_rows]

        if len(dates) < 3:
            return {"status": "error", "error": f"Not enough rebalance dates: {len(dates)}"}

        _cb(f"Rebalance dates: {len(dates)} ({dates[0]} → {dates[-1]})")

        # ── 2. Load scores + prices ──────────────────────────────────────────
        _cb(f"Loading scores and prices...")
        cap_lo, cap_hi = CAP_TIERS.get(cfg.cap_tier, (0, float("inf")))

        # AS OF join: for each score date, use the most recent price on or before that date
        # DuckDB ASOF JOIN matches each left row to the largest right key <= left key
        cap_hi_clause = "" if cap_hi == float("inf") else f"AND p.market_cap <= {cap_hi}"
        effective_mktcap_min = max(cap_lo, cfg.min_market_cap)
        mktcap_min_clause = f"AND p.market_cap >= {effective_mktcap_min}" if effective_mktcap_min > 0 else ""

        is_sep = cfg.score_column in SEPARATE_SCORE_TABLES_PM
        if is_sep:
            src_table = SEPARATE_SCORE_TABLES_PM[cfg.score_column][0]
            score_direction = SEPARATE_SCORE_TABLES_PM[cfg.score_column][1]
            if score_direction == "lower_better" and cfg.score_direction == "higher_better":
                cfg.score_direction = "lower_better"
            score_from = f"""
            FROM {src_table} s
            ASOF JOIN v_backtest_prices p
              ON s.symbol = p.symbol
             AND p.price_date <= s.month_date
            LEFT JOIN v_backtest_scores vs
              ON s.symbol = vs.symbol AND s.month_date = vs.month_date
            """
            sector_col = "COALESCE(vs.gic_sector, p.gics_sector, 'Unknown')"
        else:
            score_from = f"""
            FROM v_backtest_scores s
            ASOF JOIN v_backtest_prices p
              ON s.Symbol = p.symbol
             AND p.price_date <= s.month_date
            """
            sector_col = "COALESCE(s.gic_sector, p.gics_sector, 'Unknown')"

        df = con.execute(f"""
            SELECT
                s.month_date::VARCHAR as month_date,
                s.symbol              as symbol,
                s.{cfg.score_column}  as score,
                p.adjusted_close      as price,
                p.market_cap,
                {sector_col} as gic_sector
            {score_from}
            WHERE s.month_date >= '{dates[0]}'::DATE
              AND s.month_date <= '{dates[-1]}'::DATE
              AND s.{cfg.score_column} IS NOT NULL
              AND p.adjusted_close >= {cfg.min_price}
              {mktcap_min_clause}
              {cap_hi_clause}
              AND p.price_date >= (s.month_date::DATE - INTERVAL '10 days')
            ORDER BY s.month_date, s.symbol
        """).fetchdf()

        con.close()

        if df.empty:
            return {"status": "error", "error": "No score/price data found"}

        _cb(f"Loaded {len(df):,} records, {df['symbol'].nunique()} unique symbols")

        # ── 3. Build forward price lookup ────────────────────────────────────
        # For each date pair, we need next-period prices to compute actual returns
        # Use adjusted_close at next rebalance date
        price_pivot = df.pivot_table(
            index="month_date", columns="symbol", values="price", aggfunc="first"
        )

        # ── 4. Run period-by-period portfolio simulation ──────────────────────
        _cb("Running portfolio simulation...")

        cost_per_trade = cfg.transaction_cost_bps / 10000.0  # one-way

        portfolio_equity = [1.0]
        equity_dates     = [dates[0]]
        period_data      = []
        holdings_log     = []
        holdings         = {}   # {symbol: entry_price}  current positions

        score_groups = {d: grp for d, grp in df.groupby("month_date")}

        for i in range(len(dates) - 1):
            d_cur  = dates[i]
            d_next = dates[i + 1]

            month_df = score_groups.get(d_cur)
            if month_df is None or len(month_df) < cfg.top_n:
                continue

            # Sort by score
            if cfg.score_direction == "lower_better":
                month_df = month_df.sort_values("score", ascending=True)
            else:
                month_df = month_df.sort_values("score", ascending=False)

            month_df = month_df.dropna(subset=["score"])

            # Apply sector cap
            selected = _select_with_sector_cap(month_df, cfg.top_n, cfg.sector_max)
            if len(selected) == 0:
                continue

            selected_syms = selected["symbol"].tolist()

            # Equal weight
            weight = 1.0 / len(selected_syms)
            new_weights = {s: weight for s in selected_syms}

            # Compute turnover cost
            prev_syms = set(holdings.keys())
            new_syms  = set(selected_syms)
            exits     = prev_syms - new_syms
            entries   = new_syms - prev_syms
            turnover  = (len(exits) + len(entries)) / max(len(new_syms), 1)
            cost_drag = turnover * cost_per_trade

            # Get prices at d_cur and d_next for return calculation
            prices_cur  = price_pivot.loc[d_cur]  if d_cur  in price_pivot.index else pd.Series(dtype=float)
            prices_next = price_pivot.loc[d_next] if d_next in price_pivot.index else pd.Series(dtype=float)

            # Portfolio return: weighted avg of individual stock returns
            port_ret_components = []
            for sym in selected_syms:
                p0 = prices_cur.get(sym)
                p1 = prices_next.get(sym)
                if p0 and p1 and p0 > 0 and p1 > 0:
                    ret = (p1 - p0) / p0

                    # Stop loss check
                    if cfg.stop_loss_pct > 0 and ret < -cfg.stop_loss_pct:
                        ret = -cfg.stop_loss_pct

                    # Per-stock sanity cap: clip individual returns to [-95%, +300%]
                    # Prevents split-adjusted price errors or extreme data artifacts
                    ret = float(np.clip(ret, -0.95, 3.0))

                    port_ret_components.append(ret * weight)

            if not port_ret_components:
                continue

            # Sanity cap: clip portfolio period return to [-80%, +150%]
            # Prevents data artifacts (bad prices, splits, delistings) from
            # corrupting the equity curve. Real quarterly returns above 150%
            # are almost always data errors.
            raw_ret = sum(port_ret_components)
            portfolio_ret = float(np.clip(raw_ret, -0.80, 1.50)) - cost_drag

            portfolio_equity.append(portfolio_equity[-1] * (1 + portfolio_ret))
            equity_dates.append(d_next)

            # Holdings log — include market_cap for distribution chart
            hold_detail = []
            for _, row in selected.iterrows():
                mc = row.get("market_cap")
                hold_detail.append({
                    "symbol":     row["symbol"],
                    "score":      round(float(row["score"]), 4),
                    "sector":     row.get("gic_sector", "Unknown"),
                    "weight":     round(weight, 4),
                    "market_cap": round(float(mc)/1e9, 3) if mc and not pd.isna(mc) else None,  # in $B
                })
            holdings_log.append({"date": d_cur, "holdings": hold_detail})

            period_data.append({
                "date":             d_cur,
                "next_date":        d_next,
                "portfolio_return": round(portfolio_ret, 6),
                "n_stocks":         len(selected_syms),
                "cost_drag":        round(cost_drag, 6),
                "turnover_pct":     round(turnover * 100, 1),
                "top5":             [h["symbol"] for h in hold_detail[:5]],
            })

            holdings = {s: prices_next.get(s, 0) for s in selected_syms}

        if not period_data:
            return {"status": "error", "error": "No valid periods computed"}

        _cb(f"Simulated {len(period_data)} periods")

        # ── 5. Compute metrics ───────────────────────────────────────────────
        from engine.metrics import compute_all

        ppy = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}.get(
            cfg.rebalance_freq.lower(), 4
        )

        port_eq  = np.array(portfolio_equity, dtype=np.float64)
        port_ret = np.diff(port_eq) / port_eq[:-1]

        # ── 4b. Build trade log from holdings_log ────────────────────────────────
        # Each row = one stock held for one period: entry_date, symbol, entry_price,
        # exit_date, exit_price, return, sector, score, market_cap
        trade_log = []
        for i, period_hold in enumerate(holdings_log):
            d_entry = period_hold["date"]
            d_exit  = dates[i + 1] if i + 1 < len(dates) else d_entry
            p_entry = price_pivot.loc[d_entry] if d_entry in price_pivot.index else pd.Series(dtype=float)
            p_exit  = price_pivot.loc[d_exit]  if d_exit  in price_pivot.index else pd.Series(dtype=float)

            for h in period_hold["holdings"]:
                sym = h["symbol"]
                ep  = float(p_entry.get(sym) or 0)
                xp  = float(p_exit.get(sym)  or 0)
                if ep > 0 and xp > 0:
                    ret = float(np.clip((xp - ep) / ep, -0.95, 3.0))
                else:
                    ret = None
                trade_log.append({
                    "entry_date":  d_entry,
                    "symbol":      sym.replace(".US", ""),
                    "entry_price": round(ep, 4) if ep else None,
                    "exit_date":   d_exit,
                    "exit_price":  round(xp, 4) if xp else None,
                    "return_pct":  round(ret * 100, 2) if ret is not None else None,
                    "sector":      h.get("sector", "Unknown"),
                    "score":       h.get("score"),
                    "weight":      h.get("weight"),
                    "market_cap_B":h.get("market_cap"),
                })

        # ── 5a. SPY benchmark — resample to portfolio's rebalance dates ─────────
        # We resample SPY to EXACTLY the same quarterly dates as the portfolio
        # so the equity chart is directly comparable without index interpolation.
        spy_metrics   = {}
        spy_equity    = []   # resampled to portfolio dates
        spy_equity_raw= []   # original monthly (for storage)
        spy_ret_arr   = None
        spy_eq_arr    = None
        try:
            from engine.spy_backtest import run_spy_backtest
            spy_result = run_spy_backtest(
                start_date=equity_dates[0],
                end_date=equity_dates[-1],
            )
            if spy_result.get("status") == "complete":
                spy_metrics    = spy_result.get("portfolio_metrics", {})
                spy_equity_raw = spy_result.get("portfolio_equity", [])
                spy_dates_raw  = spy_result.get("equity_dates", [])
                spy_metrics["terminal_wealth"] = round(10000 * spy_equity_raw[-1], 0) if spy_equity_raw else 10000.0

                # Build date → spy_equity map from monthly data
                spy_date_map = {}
                for d, v in zip(spy_dates_raw, spy_equity_raw):
                    spy_date_map[str(d)[:10]] = v

                # Resample: for each portfolio date, find nearest SPY value on or before
                spy_equity_resampled = []
                sorted_spy_dates = sorted(spy_date_map.keys())
                for port_date in equity_dates:
                    pd_str = str(port_date)[:10]
                    # Find latest SPY date <= portfolio date
                    candidates = [d for d in sorted_spy_dates if d <= pd_str]
                    if candidates:
                        spy_equity_resampled.append(spy_date_map[candidates[-1]])
                    elif sorted_spy_dates:
                        spy_equity_resampled.append(spy_date_map[sorted_spy_dates[0]])
                    else:
                        spy_equity_resampled.append(1.0)

                # Renormalize SPY to start at 1.0 at portfolio start date
                if spy_equity_resampled and spy_equity_resampled[0] > 0:
                    spy_base = spy_equity_resampled[0]
                    spy_equity = [round(v / spy_base, 6) for v in spy_equity_resampled]
                else:
                    spy_equity = spy_equity_resampled

                spy_eq_arr  = np.array(spy_equity, dtype=np.float64)
                spy_ret_arr = np.diff(spy_eq_arr) / spy_eq_arr[:-1]
        except Exception as spy_err:
            _cb(f"SPY metrics skipped: {spy_err}")

        # ── 5b. Portfolio metrics vs SPY benchmark ───────────────────────────
        portfolio_metrics = compute_all(
            equity=port_eq, monthly_ret=port_ret,
            bm_equity=spy_eq_arr if spy_eq_arr is not None else None,
            bm_monthly=spy_ret_arr if spy_ret_arr is not None else None,
            periods_per_year=ppy,
            label=cfg.run_label or f"Top-{cfg.top_n} {cfg.score_column}",
        )

        # ── 6. Annual returns ────────────────────────────────────────────────
        annual_ret_by_year = _compute_annual_returns(equity_dates, portfolio_equity, [], spy_equity)

        # ── 7. Monthly heatmap ───────────────────────────────────────────────
        monthly_heatmap = _build_monthly_heatmap(equity_dates, port_ret.tolist(), ppy)

        elapsed = round(time.time() - t0, 1)
        _cb(f"Portfolio backtest complete in {elapsed}s")

        return {
            "status":            "complete",
            "run_label":         cfg.run_label or f"Top-{cfg.top_n} {SCORE_COLUMNS.get(cfg.score_column, cfg.score_column)}",
            "config": {
                "score_column":   cfg.score_column,
                "top_n":          cfg.top_n,
                "sector_max":     cfg.sector_max,
                "rebalance_freq": cfg.rebalance_freq,
                "start_date":     cfg.start_date,
                "end_date":       cfg.end_date,
                "min_price":      cfg.min_price,
                "cap_tier":       cfg.cap_tier,
                "cost_bps":       cfg.transaction_cost_bps,
                "stop_loss_pct":  cfg.stop_loss_pct,
                "weight_scheme":  cfg.weight_scheme,
            },
            "portfolio_equity":   [round(v, 6) for v in portfolio_equity],
            "spy_equity":         [round(v, 6) for v in spy_equity] if spy_equity else [],
            "equity_dates":       equity_dates,
            "portfolio_metrics":  portfolio_metrics,
            "spy_metrics":        spy_metrics,
            "period_data":        period_data,
            "annual_ret_by_year": annual_ret_by_year,
            "monthly_heatmap":    monthly_heatmap,
            "holdings_log":       holdings_log,
            "trade_log":          trade_log,
            "n_periods":          len(period_data),
            "elapsed_s":          elapsed,
        }

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


def _select_with_sector_cap(df: pd.DataFrame, top_n: int, sector_max: int) -> pd.DataFrame:
    """
    Select top_n stocks with a per-sector cap.
    Iterates ranked stocks in order; skips if sector already at cap.
    """
    if sector_max <= 0:
        return df.head(top_n)

    selected = []
    sector_counts: dict = {}

    for _, row in df.iterrows():
        if len(selected) >= top_n:
            break
        sector = row.get("gic_sector") or "Unknown"
        if sector_counts.get(sector, 0) >= sector_max:
            continue
        selected.append(row)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    return pd.DataFrame(selected) if selected else pd.DataFrame(columns=df.columns)


def _compute_annual_returns(dates, port_eq, bm_eq_unused, spy_eq) -> list:
    """
    Build annual returns by compounding period returns within each calendar year.
    Uses the ENDING equity value of each period — correct for all frequencies.
    
    Logic: for each year, find the equity at the start of the year (= last equity
    value of the prior year) and at the end of the year (= last equity value in
    that year). Annual return = end/start - 1.
    """
    if len(dates) < 2 or len(port_eq) < 2:
        return []

    # Build a {date_str: equity_value} map — one equity value per rebalance date
    # dates[i] = end of period i, port_eq[i] = equity AFTER period i
    date_to_port = {}
    date_to_spy  = {}
    for i, d in enumerate(dates):
        ds = str(d)[:10]
        date_to_port[ds] = port_eq[i]
        if spy_eq and i < len(spy_eq):
            date_to_spy[ds] = spy_eq[i]

    # Get all unique years sorted
    years = sorted(set(str(d)[:4] for d in dates))
    all_dates_sorted = sorted(date_to_port.keys())

    rows = []
    for yr in years:
        # All dates that fall in this year
        yr_dates = sorted(d for d in all_dates_sorted if d[:4] == yr)
        if not yr_dates:
            continue

        # Start of year equity = equity at last date of PRIOR year
        prior_dates = [d for d in all_dates_sorted if d[:4] < yr]
        if prior_dates:
            start_eq_p = date_to_port[prior_dates[-1]]
            start_eq_s = date_to_spy.get(prior_dates[-1])
        else:
            # First year — use the first equity value
            start_eq_p = port_eq[0]
            start_eq_s = spy_eq[0] if spy_eq else None

        # End of year equity = equity at last date of this year
        end_eq_p = date_to_port[yr_dates[-1]]
        end_eq_s = date_to_spy.get(yr_dates[-1])

        p_ret = (end_eq_p / start_eq_p - 1) if start_eq_p and start_eq_p > 0 else None
        s_ret = (end_eq_s / start_eq_s - 1) if start_eq_s and start_eq_s > 0 and end_eq_s else None

        rows.append({
            "year":          int(yr),
            "portfolio_ret": round(float(p_ret), 4) if p_ret is not None else None,
            "spy_ret":       round(float(s_ret), 4) if s_ret is not None else None,
        })
    return rows


def _build_monthly_heatmap(dates, period_rets, ppy) -> dict:
    """
    Build heatmap dict: {year: {period_label: ret_pct}}.

    For quarterly (ppy=4): use Q1/Q2/Q3/Q4 column labels — no fake monthly distribution.
    For semi-annual (ppy=2): use H1/H2.
    For monthly (ppy=12): use Jan/Feb/.../Dec.
    For annual (ppy=1): use the year directly (single column).
    """
    heatmap: dict = {}

    # Month → period label mapping
    month_to_quarter = {3:"Q1", 6:"Q2", 9:"Q3", 12:"Q4"}
    month_to_half    = {6:"H1", 12:"H2"}
    month_names      = ["Jan","Feb","Mar","Apr","May","Jun",
                        "Jul","Aug","Sep","Oct","Nov","Dec"]

    for d, r in zip(dates[1:], period_rets):
        yr  = str(d)[:4]
        mon = int(str(d)[5:7])
        ret_pct = round(r * 100, 2)

        if ppy == 4:
            label = month_to_quarter.get(mon, f"M{mon:02d}")
        elif ppy == 2:
            label = month_to_half.get(mon, f"H{(mon>6)+1}")
        elif ppy == 1:
            label = yr
        else:
            # Monthly
            label = month_names[mon - 1]

        heatmap.setdefault(yr, {})[label] = ret_pct

    return heatmap
