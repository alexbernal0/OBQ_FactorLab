"""
engine/tranche_portfolio_backtest.py
=====================================
OBQ FactorLab — Staggered Tranche Portfolio Model

Design:
  - N_TRANCHES tranches (default 4), each holding top_n / N_TRANCHES stocks
  - Each tranche rebalances once per year on its own quarter-end date
  - Tranche 1 → Mar 31, Tranche 2 → Jun 30, Tranche 3 → Sep 30, Tranche 4 → Dec 31
  - Combined portfolio = equal-weight blend of all tranche equity curves
  - Within each tranche, stocks are equal-weighted at rebalance time
  - No sector caps (sector_max = 0 means unlimited)
  - Each tranche independently picks the top_per_tranche highest-scoring stocks
    from the full universe at its rebalance date (overlaps allowed across tranches)

Result dict matches run_portfolio_backtest() exactly so it saves to portfolio_bank
with no schema changes.
"""

from __future__ import annotations

import os
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import duckdb
from dotenv import load_dotenv

load_dotenv()

MIRROR_DB = os.environ.get(
    "OBQ_EODHD_MIRROR_DB",
    r"D:/OBQ_AI/obq_eodhd_mirror.duckdb"
)

# Re-use registries from portfolio_backtest
from engine.portfolio_backtest import (
    SEPARATE_SCORE_TABLES_PM,
    SCORE_COLUMNS,
    CAP_TIERS,
    _select_with_sector_cap,
    _compute_annual_returns,
    _build_monthly_heatmap,
)

# Quarter-end months for the 4 tranches
_TRANCHE_MONTHS = {1: 3, 2: 6, 3: 9, 4: 12}   # tranche_idx(1-4) → month


def _get_con():
    return duckdb.connect(MIRROR_DB, read_only=True)


@dataclass
class TrancheCfg:
    """Configuration for the staggered-tranche portfolio model."""
    score_column:   str   = "jcn_full_composite"
    score_direction: str  = "higher_better"   # higher_better | lower_better

    # Portfolio construction
    total_stocks:   int   = 28     # total portfolio size
    n_tranches:     int   = 4      # number of tranches (default 4)
    sector_max:     int   = 0      # 0 = no sector limit
    weight_scheme:  str   = "equal"

    # Rebalance / hold
    # Each tranche rebalances once per year; 4 tranches → quarterly stagger
    start_date:     str   = "1995-03-31"
    end_date:       str   = "2024-12-31"

    # Filters
    min_price:      float = 5.0
    min_adv_usd:    float = 1_000_000.0
    min_market_cap: float = 0.0
    cap_tier:       str   = "all"
    transaction_cost_bps: float = 15.0
    stop_loss_pct:  float = 0.0

    run_label:      str   = ""
    strategy_id:    str   = ""


def run_tranche_portfolio_backtest(cfg: TrancheCfg, cb=None) -> dict:
    """
    Run the staggered-tranche portfolio backtest.

    Returns a result dict compatible with save_portfolio_model().
    """
    t0 = time.time()

    def _cb(msg):
        if cb:
            try:
                cb("info", msg)
            except Exception:
                pass

    try:
        con = _get_con()

        top_per_tranche = max(1, cfg.total_stocks // cfg.n_tranches)
        _cb(f"Tranche config: {cfg.n_tranches} tranches × {top_per_tranche} stocks "
            f"= {cfg.n_tranches * top_per_tranche} total slots")

        # ── 1. Resolve score column / table ──────────────────────────────────
        _sep_entry = SEPARATE_SCORE_TABLES_PM.get(cfg.score_column)
        if _sep_entry:
            src_tbl    = _sep_entry[0]
            _actual_col = _sep_entry[2] if len(_sep_entry) == 3 else cfg.score_column
            if _sep_entry[1] == "lower_better" and cfg.score_direction == "higher_better":
                cfg.score_direction = "lower_better"
        else:
            src_tbl    = "v_backtest_scores"
            _actual_col = cfg.score_column

        # ── 2. Load all score dates ───────────────────────────────────────────
        # Quarterly dates (all quarter-ends)
        dates_rows = con.execute(f"""
            SELECT DISTINCT month_date::VARCHAR as d
            FROM {src_tbl}
            WHERE month_date >= '{cfg.start_date}'::DATE
              AND month_date <= '{cfg.end_date}'::DATE
              AND {_actual_col} IS NOT NULL
              AND MONTH(month_date) IN (3, 6, 9, 12)
            ORDER BY d
        """).fetchall()
        all_dates = [r[0] for r in dates_rows]

        if len(all_dates) < 5:
            return {"status": "error", "error": f"Not enough quarterly dates: {len(all_dates)}"}

        _cb(f"Quarterly dates: {len(all_dates)} ({all_dates[0]} → {all_dates[-1]})")

        # ── 3. Load scores + prices for all quarterly dates ──────────────────
        cap_lo, cap_hi = CAP_TIERS.get(cfg.cap_tier, (0, float("inf")))
        cap_hi_clause  = "" if cap_hi == float("inf") else f"AND p.market_cap <= {cap_hi}"
        eff_mktcap_min = max(cap_lo, cfg.min_market_cap)
        mktcap_clause  = f"AND p.market_cap >= {eff_mktcap_min}" if eff_mktcap_min > 0 else ""

        is_sep = cfg.score_column in SEPARATE_SCORE_TABLES_PM
        if is_sep:
            _pm_entry   = SEPARATE_SCORE_TABLES_PM[cfg.score_column]
            src_table   = _pm_entry[0]
            _pm_col     = _pm_entry[2] if len(_pm_entry) == 3 else cfg.score_column
            try:
                _sym_s = con.execute(f"""
                    SELECT symbol FROM {src_table}
                    WHERE month_date = (SELECT MAX(month_date) FROM {src_table})
                    LIMIT 1
                """).fetchone()
                _no_us = _sym_s and not str(_sym_s[0]).endswith('.US')
            except Exception:
                _no_us = False
            _sym_expr = "CONCAT(s.symbol, '.US')" if _no_us else "s.symbol"
            score_from = f"""
            FROM {src_table} s
            ASOF JOIN v_backtest_prices p
              ON {_sym_expr} = p.symbol
             AND p.price_date <= s.month_date
            LEFT JOIN v_backtest_scores vs
              ON {_sym_expr} = vs.symbol AND s.month_date = vs.month_date
            """
            sector_col  = "COALESCE(vs.gic_sector, p.gics_sector, 'Unknown')"
            score_col_q = _pm_col
        else:
            score_from  = """
            FROM v_backtest_scores s
            ASOF JOIN v_backtest_prices p
              ON s.Symbol = p.symbol
             AND p.price_date <= s.month_date
            """
            sector_col  = "COALESCE(s.gic_sector, p.gics_sector, 'Unknown')"
            score_col_q = cfg.score_column

        _cb("Loading scores + prices...")
        df = con.execute(f"""
            SELECT
                s.month_date::VARCHAR as month_date,
                s.symbol              as symbol,
                s.{score_col_q}       as score,
                p.adjusted_close      as price,
                p.market_cap,
                {sector_col}          as gic_sector
            {score_from}
            WHERE s.month_date >= '{all_dates[0]}'::DATE
              AND s.month_date <= '{all_dates[-1]}'::DATE
              AND s.{score_col_q} IS NOT NULL
              AND p.adjusted_close >= {cfg.min_price}
              {mktcap_clause}
              {cap_hi_clause}
              AND p.price_date >= (s.month_date::DATE - INTERVAL '10 days')
            ORDER BY s.month_date, s.symbol
        """).fetchdf()
        con.close()

        if df.empty:
            return {"status": "error", "error": "No score/price data found"}

        _cb(f"Loaded {len(df):,} records, {df['symbol'].nunique()} unique symbols")

        # ── 4. Build price pivot ──────────────────────────────────────────────
        price_pivot  = df.pivot_table(
            index="month_date", columns="symbol", values="price", aggfunc="first"
        )
        score_groups = {d: grp for d, grp in df.groupby("month_date")}

        cost_per_trade = cfg.transaction_cost_bps / 10000.0

        # ── 5. Simulate each tranche independently ────────────────────────────
        # Tranche k (1-indexed) rebalances at dates where MONTH == _TRANCHE_MONTHS[k]
        # and holds until same month next year.

        # Build a common observation grid of all quarterly dates (for blending)
        # We'll compute a per-tranche equity on each observation date using
        # hold-period price returns.

        tranche_results = {}

        for t_idx in range(1, cfg.n_tranches + 1):
            rebal_month = _TRANCHE_MONTHS.get(t_idx, t_idx * 3)
            # Dates where this tranche rebalances
            t_rebal = [d for d in all_dates if int(d[5:7]) == rebal_month]
            if len(t_rebal) < 2:
                _cb(f"  Tranche {t_idx}: only {len(t_rebal)} rebal dates, skipping")
                tranche_results[t_idx] = {"rebal_dates": [], "period_rets": [],
                                           "period_dates": [], "holdings_log": []}
                continue

            _cb(f"  Tranche {t_idx} (month={rebal_month}): {len(t_rebal)} rebal dates "
                f"{t_rebal[0]} → {t_rebal[-1]}")

            t_period_rets = []
            t_period_dates = []
            t_holdings_log = []
            t_holdings = {}  # {symbol: entry_price}

            for i in range(len(t_rebal) - 1):
                d_cur  = t_rebal[i]
                d_next = t_rebal[i + 1]

                month_df = score_groups.get(d_cur)
                if month_df is None or len(month_df) < top_per_tranche:
                    # Not enough stocks — carry forward 0 return
                    t_period_rets.append(0.0)
                    t_period_dates.append(d_next)
                    continue

                # Sort by score
                ascending = (cfg.score_direction == "lower_better")
                month_df = month_df.sort_values("score", ascending=ascending).dropna(subset=["score"])

                # Select top_per_tranche (with optional sector cap)
                selected = _select_with_sector_cap(month_df, top_per_tranche, cfg.sector_max)
                if len(selected) == 0:
                    t_period_rets.append(0.0)
                    t_period_dates.append(d_next)
                    continue

                selected_syms = selected["symbol"].tolist()
                weight = 1.0 / len(selected_syms)

                # Turnover cost
                prev_syms = set(t_holdings.keys())
                new_syms  = set(selected_syms)
                exits     = prev_syms - new_syms
                entries   = new_syms - prev_syms
                turnover  = (len(exits) + len(entries)) / max(len(new_syms), 1)
                cost_drag = turnover * cost_per_trade

                # Compute 12-month return (d_cur → d_next which is ~12 months later)
                prices_cur  = price_pivot.loc[d_cur]  if d_cur  in price_pivot.index else pd.Series(dtype=float)
                prices_next = price_pivot.loc[d_next] if d_next in price_pivot.index else pd.Series(dtype=float)

                port_ret_components = []
                for sym in selected_syms:
                    p0 = prices_cur.get(sym)
                    p1 = prices_next.get(sym)
                    if p0 and p1 and float(p0) > 0 and float(p1) > 0:
                        ret = float(np.clip((float(p1) - float(p0)) / float(p0), -0.95, 3.0))
                        # Stop loss
                        if cfg.stop_loss_pct > 0 and ret < -cfg.stop_loss_pct:
                            ret = -cfg.stop_loss_pct
                        port_ret_components.append(ret * weight)

                if not port_ret_components:
                    t_period_rets.append(0.0)
                    t_period_dates.append(d_next)
                    continue

                raw_ret = sum(port_ret_components)
                period_ret = float(np.clip(raw_ret, -0.80, 1.50)) - cost_drag

                t_period_rets.append(period_ret)
                t_period_dates.append(d_next)
                t_holdings = {s: prices_next.get(s, 0) for s in selected_syms}

                # Holdings log
                hold_detail = []
                for _, row in selected.iterrows():
                    mc = row.get("market_cap")
                    hold_detail.append({
                        "symbol":     row["symbol"],
                        "score":      round(float(row["score"]), 4),
                        "sector":     row.get("gic_sector", "Unknown"),
                        "weight":     round(weight, 4),
                        "market_cap": round(float(mc) / 1e9, 3) if mc and not pd.isna(mc) else None,
                    })
                t_holdings_log.append({"date": d_cur, "next_date": d_next, "holdings": hold_detail})

            tranche_results[t_idx] = {
                "rebal_dates":  t_rebal,
                "period_rets":  t_period_rets,
                "period_dates": t_period_dates,
                "holdings_log": t_holdings_log,
            }
            _cb(f"    → {len(t_period_rets)} annual periods computed")

        # ── 6. Blend tranches on a common quarterly observation grid ──────────
        # For each quarterly observation date, each active tranche contributes
        # its current holding-period return (interpolated from its last rebal).
        # We track the VALUE of each tranche separately, then take equal-weight
        # average of all tranche values to get blended portfolio value.
        #
        # Implementation: build per-tranche equity curves on quarterly grid,
        # then blend.

        # For each tranche, build a step-function equity on all_dates:
        # equity stays flat until next rebalance, then compounds.
        # Actually, simplest and most accurate:
        # At each quarterly date, the tranche holds its last rebalanced stocks.
        # We compute the current value vs last-rebalance entry prices.

        _cb("Blending tranche equity curves on quarterly grid...")

        # Build {date: price_row} for all quarterly dates
        def _get_price(d, sym):
            if d not in price_pivot.index:
                return None
            v = price_pivot.loc[d].get(sym)
            return float(v) if v is not None and not pd.isna(v) else None

        # For each tranche, compute value index on every quarterly date
        tranche_equities = {}  # t_idx -> {date: value}

        for t_idx, tres in tranche_results.items():
            rebal_dates = tres["rebal_dates"]
            holdings_log = tres["holdings_log"]
            if not rebal_dates or not holdings_log:
                tranche_equities[t_idx] = {}
                continue

            # Build map: rebal_date → selected holdings {sym: entry_price}
            rebal_holdings = {}
            for entry in holdings_log:
                d_e   = entry["date"]
                syms  = [h["symbol"] for h in entry["holdings"]]
                n     = len(syms)
                if n == 0:
                    continue
                prices = {}
                for sym in syms:
                    p = _get_price(d_e, sym)
                    if p:
                        prices[sym] = p
                rebal_holdings[d_e] = prices

            # Walk quarterly dates; for each, find the active rebalance
            eq_val = {}
            base_val = 1.0

            # We track the portfolio in "units" — each rebal resets to equal weight
            # Current tranche value = base_val * product of returns since last rebal
            cur_rebal_date = None
            cur_holdings   = {}   # {sym: entry_price at rebal}
            cur_base       = 1.0

            for q_date in all_dates:
                # Check if this is a rebalance date for this tranche
                if q_date in rebal_holdings:
                    # Compound to exit of previous period first (already done via period_rets),
                    # then reset base
                    if cur_rebal_date is not None:
                        # The value at this date is compounded by the period return
                        # We already have the period return in tres["period_rets"]
                        # Find the period index
                        try:
                            pd_idx = tres["period_dates"].index(q_date)
                            period_r = tres["period_rets"][pd_idx]
                            cur_base = cur_base * (1.0 + period_r)
                        except ValueError:
                            pass  # date not in period dates, keep cur_base

                    cur_rebal_date = q_date
                    cur_holdings   = rebal_holdings[q_date]
                    # Record value at this rebal date (start of new period)
                    eq_val[q_date] = cur_base
                else:
                    if cur_rebal_date is None:
                        # Before first rebal — no data
                        eq_val[q_date] = None
                        continue

                    # Interpolate within holding period: compute current value
                    # relative to last-rebal entry prices
                    if not cur_holdings:
                        eq_val[q_date] = cur_base
                        continue

                    rets = []
                    for sym, ep in cur_holdings.items():
                        cp = _get_price(q_date, sym)
                        if cp and ep and ep > 0:
                            rets.append((cp - ep) / ep)

                    if rets:
                        mid_ret = float(np.clip(np.mean(rets), -0.80, 1.50))
                        eq_val[q_date] = cur_base * (1.0 + mid_ret)
                    else:
                        eq_val[q_date] = cur_base

            tranche_equities[t_idx] = eq_val

        # ── 7. Blend: for each quarterly date, average active tranche values ──
        blended_equity = []
        equity_dates   = []

        for q_date in all_dates:
            vals = [v for t_eq in tranche_equities.values()
                    if (v := t_eq.get(q_date)) is not None and v > 0]
            if vals:
                blended_equity.append(float(np.mean(vals)))
                equity_dates.append(q_date)

        if len(blended_equity) < 4:
            return {"status": "error", "error": "Not enough blended equity points"}

        # Renormalise to start at 1.0
        base = blended_equity[0]
        blended_equity = [v / base for v in blended_equity]

        _cb(f"Blended equity: {len(blended_equity)} quarterly points")

        # ── 8. Build period_data and holdings_log for storage ─────────────────
        # Flatten all tranche holdings into combined quarterly holdings_log
        period_data  = []
        holdings_log = []

        for i in range(len(equity_dates) - 1):
            d_cur  = equity_dates[i]
            d_next = equity_dates[i + 1]
            port_ret = (blended_equity[i + 1] / blended_equity[i]) - 1.0

            # Collect all holdings active at d_cur (from all tranches)
            combined_holdings = []
            for t_idx, tres in tranche_results.items():
                for hlog in tres["holdings_log"]:
                    if hlog["date"] <= d_cur < hlog.get("next_date", d_next):
                        combined_holdings.extend(hlog["holdings"])

            if not combined_holdings:
                # Fallback: use most recent holdings from any tranche
                for t_idx, tres in tranche_results.items():
                    for hlog in reversed(tres["holdings_log"]):
                        if hlog["date"] <= d_cur:
                            combined_holdings.extend(hlog["holdings"])
                            break

            holdings_log.append({"date": d_cur, "holdings": combined_holdings})

            period_data.append({
                "date":             d_cur,
                "next_date":        d_next,
                "portfolio_return": round(port_ret, 6),
                "n_stocks":         len(combined_holdings),
                "cost_drag":        0.0,
                "turnover_pct":     0.0,
                "top5":             [h["symbol"] for h in combined_holdings[:5]],
            })

        if not period_data:
            return {"status": "error", "error": "No valid periods in blended series"}

        # ── 9. Metrics ────────────────────────────────────────────────────────
        from engine.metrics import compute_all

        ppy = 4  # quarterly observation, 4 periods per year

        port_eq  = np.array(blended_equity, dtype=np.float64)
        port_ret = np.diff(port_eq) / port_eq[:-1]

        # SPY benchmark
        spy_metrics   = {}
        spy_equity    = []
        spy_eq_arr    = None
        spy_ret_arr   = None
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
                spy_date_map   = {str(d)[:10]: v for d, v in zip(spy_dates_raw, spy_equity_raw)}
                sorted_spy_d   = sorted(spy_date_map.keys())

                spy_eq_res = []
                for pd_str in [str(d)[:10] for d in equity_dates]:
                    cands = [d for d in sorted_spy_d if d <= pd_str]
                    spy_eq_res.append(spy_date_map[cands[-1]] if cands else spy_date_map[sorted_spy_d[0]])

                if spy_eq_res and spy_eq_res[0] > 0:
                    base_s = spy_eq_res[0]
                    spy_equity = [round(v / base_s, 6) for v in spy_eq_res]

                spy_eq_arr  = np.array(spy_equity, dtype=np.float64) if spy_equity else None
                spy_ret_arr = np.diff(spy_eq_arr) / spy_eq_arr[:-1] if spy_eq_arr is not None else None
        except Exception as spy_err:
            _cb(f"SPY skipped: {spy_err}")

        portfolio_metrics = compute_all(
            equity=port_eq, monthly_ret=port_ret,
            bm_equity=spy_eq_arr,
            bm_monthly=spy_ret_arr,
            periods_per_year=ppy,
            label=cfg.run_label or f"Tranche-{cfg.n_tranches}×{top_per_tranche} {cfg.score_column}",
        )

        # ── 10. Annual returns + heatmap ──────────────────────────────────────
        annual_ret_by_year = _compute_annual_returns(equity_dates, blended_equity, [], spy_equity)
        monthly_heatmap    = _build_monthly_heatmap(equity_dates, port_ret.tolist(), ppy)

        # ── 11. Trade log ─────────────────────────────────────────────────────
        trade_log = []
        for t_idx, tres in tranche_results.items():
            for i, hlog in enumerate(tres["holdings_log"]):
                d_entry = hlog["date"]
                d_exit  = hlog.get("next_date", d_entry)
                for h in hlog["holdings"]:
                    sym = h["symbol"]
                    ep  = float(_get_price(d_entry, sym) or 0)
                    xp  = float(_get_price(d_exit, sym)  or 0)
                    if ep > 0 and xp > 0:
                        ret = float(np.clip((xp - ep) / ep, -0.95, 3.0))
                    else:
                        ret = None
                    trade_log.append({
                        "entry_date":   d_entry,
                        "symbol":       sym.replace(".US", ""),
                        "entry_price":  round(ep, 4) if ep else None,
                        "exit_date":    d_exit,
                        "exit_price":   round(xp, 4) if xp else None,
                        "return_pct":   round(ret * 100, 2) if ret is not None else None,
                        "sector":       h.get("sector", "Unknown"),
                        "score":        h.get("score"),
                        "weight":       h.get("weight"),
                        "market_cap_B": h.get("market_cap"),
                        "tranche":      t_idx,
                    })

        elapsed = round(time.time() - t0, 1)
        _cb(f"Tranche portfolio backtest complete in {elapsed}s")

        run_label = (
            cfg.run_label or
            f"{SCORE_COLUMNS.get(cfg.score_column, cfg.score_column)} | "
            f"Top-{cfg.total_stocks} | {cfg.n_tranches}T-Qtrly | "
            f"Equal-Wt | {cfg.cap_tier} | "
            f"{cfg.start_date[:4]}-{cfg.end_date[:4]} [PM-TRANCHE]"
        )

        return {
            "status":            "complete",
            "run_label":         run_label,
            "config": {
                "score_column":    cfg.score_column,
                "top_n":           cfg.total_stocks,
                "sector_max":      cfg.sector_max,
                "rebalance_freq":  "quarterly",          # quarterly stagger
                "hold_months":     12,
                "n_tranches":      cfg.n_tranches,
                "top_per_tranche": top_per_tranche,
                "start_date":      cfg.start_date,
                "end_date":        cfg.end_date,
                "min_price":       cfg.min_price,
                "cap_tier":        cfg.cap_tier,
                "cost_bps":        cfg.transaction_cost_bps,
                "stop_loss_pct":   cfg.stop_loss_pct,
                "weight_scheme":   cfg.weight_scheme,
            },
            "portfolio_equity":   [round(v, 6) for v in blended_equity],
            "bm_equity":          [],
            "spy_equity":         [round(v, 6) for v in spy_equity] if spy_equity else [],
            "equity_dates":       equity_dates,
            "portfolio_metrics":  portfolio_metrics,
            "bm_metrics":         {},
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
