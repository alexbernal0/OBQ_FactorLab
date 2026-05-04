"""
engine/portfolio_bank.py
========================
OBQ FactorLab — Portfolio Strategy Bank

IDs: PM-{SCORE_SHORT}-{YYYYMMDD}-{4CHAR}
e.g. PM-JCNFUL-20260430-A3F2

Stores full portfolio backtest results for the Portfolio Models tab.
"""

from __future__ import annotations

import os
import json
import math
import hashlib
import datetime
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np

BANK_DIR  = Path(os.environ.get("OBQ_BANK_DIR", r"D:\OBQ_AI\OBQ_FactorLab_Bank"))
BANK_FILE = BANK_DIR / "portfolio_strategy_bank.duckdb"


def _get_bank() -> duckdb.DuckDBPyConnection:
    BANK_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(BANK_FILE))
    _ensure_schema(con)
    return con


def _ensure_schema(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_models (
        strategy_id     VARCHAR PRIMARY KEY,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        run_label       VARCHAR,
        score_column    VARCHAR,
        top_n           INTEGER,
        sector_max      INTEGER,
        rebalance_freq  VARCHAR,
        start_date      VARCHAR,
        end_date        VARCHAR,
        cap_tier        VARCHAR,
        cost_bps        DOUBLE,
        stop_loss_pct   DOUBLE,
        weight_scheme   VARCHAR,

        -- Key performance metrics (scalar — fast sort/filter)
        cagr            DOUBLE,
        sharpe          DOUBLE,
        max_dd          DOUBLE,
        calmar          DOUBLE,
        sortino         DOUBLE,
        ann_vol         DOUBLE,
        win_rate_monthly DOUBLE,
        surefire_ratio  DOUBLE,
        equity_r2       DOUBLE,
        alpha_vs_bm     DOUBLE,
        beta_vs_bm      DOUBLE,
        n_periods       INTEGER,
        elapsed_s       DOUBLE,

        -- Full JSON blobs for tearsheet replay
        config_json             VARCHAR,
        portfolio_metrics_json  VARCHAR,
        bm_metrics_json         VARCHAR,
        spy_metrics_json        VARCHAR,
        portfolio_equity_json   VARCHAR,
        bm_equity_json          VARCHAR,
        spy_equity_json         VARCHAR,
        equity_dates_json       VARCHAR,
        period_data_json        VARCHAR,
        annual_ret_json         VARCHAR,
        monthly_heatmap_json    VARCHAR,
        holdings_log_json       VARCHAR,

        -- Status
        status      VARCHAR DEFAULT 'saved',
        notes       VARCHAR DEFAULT '',
        tags        VARCHAR DEFAULT ''
    )
    """)

    # Migrate: add any missing columns
    existing = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='portfolio_models'"
    ).fetchall()}
    new_cols = [
        ("alpha_vs_bm",       "DOUBLE"),
        ("beta_vs_bm",        "DOUBLE"),
        ("holdings_log_json", "VARCHAR"),
        ("trade_log_json",    "VARCHAR"),
    ]
    for col, dtype in new_cols:
        if col not in existing:
            try:
                con.execute(f"ALTER TABLE portfolio_models ADD COLUMN {col} {dtype}")
            except Exception:
                pass

    con.execute("""
    CREATE INDEX IF NOT EXISTS idx_pm_score
    ON portfolio_models (score_column, cagr DESC)
    """)


def _make_strategy_id(score_col: str, cfg: dict) -> str:
    score_short = score_col.replace("_", "").upper()[:6]
    date_str    = datetime.date.today().strftime("%Y%m%d")
    sig = json.dumps({
        "score":     score_col,
        "top_n":     cfg.get("top_n", 20),
        "sector_max":cfg.get("sector_max", 5),
        "rebal":     cfg.get("rebalance_freq", "quarterly"),
        "start":     cfg.get("start_date", ""),
        "end":       cfg.get("end_date", ""),
        "cap_tier":  cfg.get("cap_tier", "all"),
        "stop_loss": cfg.get("stop_loss_pct", 0.0),
    }, sort_keys=True)
    h4 = hashlib.sha256(sig.encode()).hexdigest()[:4].upper()
    return f"PM-{score_short}-{date_str}-{h4}"


def save_portfolio_model(result: dict, overwrite: bool = True) -> str:
    """Save a completed portfolio backtest. Returns strategy_id."""
    if result.get("status") != "complete":
        raise ValueError("Only 'complete' results can be saved")

    config = result.get("config", {})
    pm     = result.get("portfolio_metrics", {})
    score  = config.get("score_column", "unknown")

    strategy_id = _make_strategy_id(score, config)

    def _safe(v):
        if v is None: return None
        try:
            f = float(v)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except (TypeError, ValueError):
            return v

    def _jdump(obj):
        def _clean(o):
            if isinstance(o, dict):  return {k: _clean(v) for k, v in o.items()}
            if isinstance(o, list):  return [_clean(v) for v in o]
            if isinstance(o, float):
                return None if (math.isnan(o) or math.isinf(o)) else o
            if isinstance(o, (np.integer, np.floating)):
                v = float(o)
                return None if (math.isnan(v) or math.isinf(v)) else v
            return o
        return json.dumps(_clean(obj))

    row = {
        "strategy_id":    strategy_id,
        "run_label":      result.get("run_label", ""),
        "score_column":   score,
        "top_n":          int(config.get("top_n", 20)),
        "sector_max":     int(config.get("sector_max", 5)),
        "rebalance_freq": config.get("rebalance_freq", "quarterly"),
        "start_date":     config.get("start_date", ""),
        "end_date":       config.get("end_date", ""),
        "cap_tier":       config.get("cap_tier", "all"),
        "cost_bps":       _safe(config.get("cost_bps", 15.0)),
        "stop_loss_pct":  _safe(config.get("stop_loss_pct", 0.0)),
        "weight_scheme":  config.get("weight_scheme", "equal"),
        # Scalar metrics
        "cagr":             _safe(pm.get("cagr")),
        "sharpe":           _safe(pm.get("sharpe")),
        "max_dd":           _safe(pm.get("max_dd")),
        "calmar":           _safe(pm.get("calmar")),
        "sortino":          _safe(pm.get("sortino")),
        "ann_vol":          _safe(pm.get("ann_vol")),
        "win_rate_monthly": _safe(pm.get("win_rate_monthly")),
        "surefire_ratio":   _safe(pm.get("surefire_ratio")),
        "equity_r2":        _safe(pm.get("equity_r2")),
        "alpha_vs_bm":      _safe(pm.get("alpha")),
        "beta_vs_bm":       _safe(pm.get("beta")),
        "n_periods":        int(result.get("n_periods", 0)),
        "elapsed_s":        _safe(result.get("elapsed_s")),
        # JSON blobs
        "config_json":            _jdump(config),
        "portfolio_metrics_json": _jdump(pm),
        "bm_metrics_json":        _jdump(result.get("bm_metrics", {})),
        "spy_metrics_json":       _jdump(result.get("spy_metrics", {})),
        "portfolio_equity_json":  _jdump(result.get("portfolio_equity", [])),
        "bm_equity_json":         _jdump(result.get("bm_equity", [])),
        "spy_equity_json":        _jdump(result.get("spy_equity", [])),
        "equity_dates_json":      _jdump(result.get("equity_dates", [])),
        "period_data_json":       _jdump(result.get("period_data", [])),
        "annual_ret_json":        _jdump(result.get("annual_ret_by_year", [])),
        "monthly_heatmap_json":   _jdump(result.get("monthly_heatmap", {})),
        "holdings_log_json":      _jdump(result.get("holdings_log", [])),
        "trade_log_json":         _jdump(result.get("trade_log", [])),
        "status": "saved",
        "notes":  "",
        "tags":   "",
    }

    con = _get_bank()
    try:
        existing = con.execute(
            "SELECT strategy_id FROM portfolio_models WHERE strategy_id = ?",
            [strategy_id]
        ).fetchone()

        if existing and not overwrite:
            return strategy_id

        if existing:
            set_clause = ", ".join(f"{k} = ?" for k in row if k != "strategy_id")
            vals = [row[k] for k in row if k != "strategy_id"] + [strategy_id]
            con.execute(f"UPDATE portfolio_models SET {set_clause} WHERE strategy_id = ?", vals)
        else:
            cols = ", ".join(row.keys())
            plh  = ", ".join("?" for _ in row)
            con.execute(f"INSERT INTO portfolio_models ({cols}) VALUES ({plh})", list(row.values()))

        con.commit()

        # Also save to dedicated trade log DB
        trade_log = result.get("trade_log", [])
        if trade_log:
            try:
                from engine.trade_log_db import save_portfolio_trades
                save_portfolio_trades(strategy_id, score, trade_log)
            except Exception:
                pass  # non-fatal

        return strategy_id
    finally:
        con.close()


def get_all_portfolio_models(limit: int = 200) -> list:
    con = _get_bank()
    try:
        rows = con.execute(f"""
            SELECT strategy_id, created_at::VARCHAR, run_label, score_column,
                   top_n, sector_max, rebalance_freq, start_date, end_date, cap_tier,
                   cost_bps, stop_loss_pct, weight_scheme,
                   cagr, sharpe, max_dd, calmar, sortino, ann_vol,
                   win_rate_monthly, surefire_ratio, equity_r2,
                   alpha_vs_bm, beta_vs_bm, n_periods, elapsed_s,
                   status, notes, tags
            FROM portfolio_models
            ORDER BY created_at DESC NULLS LAST
            LIMIT {limit}
        """).fetchdf()
        import math as _math
        def _c(v):
            if isinstance(v, float) and (_math.isnan(v) or _math.isinf(v)):
                return None
            return v
        records = rows.to_dict(orient="records")
        return [{k: _c(v) for k, v in m.items()} for m in records]
    finally:
        con.close()


def get_portfolio_model(strategy_id: str) -> Optional[dict]:
    con = _get_bank()
    try:
        row = con.execute(
            "SELECT * FROM portfolio_models WHERE strategy_id = ?", [strategy_id]
        ).fetchdf()
        if row.empty: return None
        d = row.iloc[0].to_dict()
        json_cols = (
            "config_json", "portfolio_metrics_json", "bm_metrics_json",
            "spy_metrics_json", "portfolio_equity_json", "bm_equity_json",
            "spy_equity_json", "equity_dates_json", "period_data_json",
            "annual_ret_json", "monthly_heatmap_json", "holdings_log_json",
            "trade_log_json",
        )
        for col in json_cols:
            raw = d.get(col)
            if raw is None:
                d[col] = [] if "equity" in col or "period" in col or "annual" in col or "holdings" in col else {}
                continue
            try:
                d[col] = json.loads(raw)
            except Exception:
                d[col] = {} if isinstance(raw, str) else raw
        return d
    finally:
        con.close()
