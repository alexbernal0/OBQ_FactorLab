"""
engine/strategy_bank.py
=======================
OBQ FactorLab — Strategy Bank

Persists factor model backtest results to a local DuckDB file.
Each run gets a unique human-readable strategy ID:
  FM-{SCORE_SHORT}-{YYYYMMDD}-{4-CHAR-HASH}
  e.g. FM-JCN-20260430-A3F2

The bank stores:
  - Full config
  - Factor metrics (IC, ICIR, spread, monotonicity, per-bucket CAGR/Sharpe/DD)
  - Compact NAV series for each quintile
  - Run metadata (timestamp, label, status)

Promotion flow (future):
  FM-JCN-20260430-A3F2 → PS-JCN-20260430-A3F2  (portfolio strategy)
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

# ── Bank location ─────────────────────────────────────────────────────────────
BANK_DIR  = Path(os.environ.get("OBQ_BANK_DIR",
                                r"D:\OBQ_AI\OBQ_FactorLab_Bank"))
BANK_FILE = BANK_DIR / "factor_strategy_bank.duckdb"


def _get_bank() -> duckdb.DuckDBPyConnection:
    BANK_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(BANK_FILE))
    _ensure_schema(con)
    return con


def _ensure_schema(con: duckdb.DuckDBPyConnection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS factor_models (
        strategy_id     VARCHAR PRIMARY KEY,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        run_label       VARCHAR,
        score_column    VARCHAR,
        score_display   VARCHAR,
        n_buckets       INTEGER,
        hold_months     INTEGER,
        start_date      VARCHAR,
        end_date        VARCHAR,
        cap_tier        VARCHAR,
        rebalance_freq  VARCHAR,
        min_price       DOUBLE,
        min_adv_usd     DOUBLE,
        cost_bps        DOUBLE,

        -- Factor signal quality
        ic_mean             DOUBLE,
        ic_std              DOUBLE,
        icir                DOUBLE,
        ic_hit_rate         DOUBLE,
        spearman_rho        DOUBLE,
        monotonicity_score  DOUBLE,

        -- Quintile performance
        quintile_spread_cagr  DOUBLE,
        q1_cagr               DOUBLE,
        q1_sharpe             DOUBLE,
        q1_max_dd             DOUBLE,
        q1_sortino            DOUBLE,
        q1_surefire           DOUBLE,
        q1_equity_r2          DOUBLE,
        qn_cagr               DOUBLE,
        qn_sharpe             DOUBLE,
        n_obs                 INTEGER,
        n_stocks_avg          DOUBLE,
        elapsed_s             DOUBLE,

        -- Full results JSON (complete — for tearsheet replay)
        config_json              VARCHAR,
        bucket_metrics_json      VARCHAR,
        ic_data_json             VARCHAR,
        bucket_equity_json       VARCHAR,
        annual_ret_json          VARCHAR,
        dates_json               VARCHAR,
        tortoriello_json         VARCHAR,
        universe_metrics_json    VARCHAR,
        period_data_json         VARCHAR,
        sector_attribution_json  VARCHAR,
        spy_metrics_json         VARCHAR,
        fitness_json             VARCHAR,

        -- Fitness scalar columns (for fast sorting without JSON parse)
        staircase_score      DOUBLE,
        alpha_win_rate       DOUBLE,
        avg_annual_alpha     DOUBLE,
        bear_score           DOUBLE,
        bull_score           DOUBLE,
        downside_capture     DOUBLE,
        alpha_sharpe         DOUBLE,
        obq_fund_score       DOUBLE,
        q1_calmar            DOUBLE,

        -- Status / review
        status          VARCHAR DEFAULT 'saved',
        notes           VARCHAR DEFAULT '',
        promoted_to     VARCHAR DEFAULT NULL,  -- PS-xxx if promoted to portfolio
        tags            VARCHAR DEFAULT ''
    )
    """)

    # Migrate: add new JSON columns if they don't exist yet (idempotent)
    existing_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='factor_models'"
    ).fetchall()}
    new_cols = [
        ("dates_json",              "VARCHAR"),
        ("universe_equity_json",    "VARCHAR"),
        ("tortoriello_json",        "VARCHAR"),
        ("universe_metrics_json",   "VARCHAR"),
        ("period_data_json",        "VARCHAR"),
        ("sector_attribution_json", "VARCHAR"),
        ("spy_metrics_json",        "VARCHAR"),
        ("fitness_json",            "VARCHAR"),
        ("staircase_score",         "DOUBLE"),
        ("alpha_win_rate",          "DOUBLE"),
        ("avg_annual_alpha",        "DOUBLE"),
        ("bear_score",              "DOUBLE"),
        ("bull_score",              "DOUBLE"),
        ("downside_capture",        "DOUBLE"),
        ("alpha_sharpe",            "DOUBLE"),
        ("obq_fund_score",          "DOUBLE"),
        ("q1_calmar",               "DOUBLE"),
        ("trade_log_json",          "VARCHAR"),
    ]
    for col_name, dtype in new_cols:
        if col_name not in existing_cols:
            try:
                con.execute(f"ALTER TABLE factor_models ADD COLUMN {col_name} {dtype}")
                existing_cols.add(col_name)  # track so we don't double-add
            except Exception:
                pass  # already exists or other benign error

    # Index for fast queries by score
    con.execute("""
    CREATE INDEX IF NOT EXISTS idx_fm_score
    ON factor_models (score_column, icir DESC)
    """)


# ── ID generation ─────────────────────────────────────────────────────────────

def _make_strategy_id(score_column: str, config: dict) -> str:
    """
    FM-{SCORE_SHORT}-{YYYYMMDD}-{4CHAR}
    Hash is deterministic from config so re-running same params = same ID (deduplication).
    """
    score_short = score_column.replace("_", "").upper()[:6]
    date_str    = datetime.date.today().strftime("%Y%m%d")
    # Hash from key config params for deduplication
    sig = json.dumps({
        "score": score_column,
        "n_buckets":     config.get("n_buckets", 5),
        "hold_months":   config.get("hold_months", 6),
        "start_date":    config.get("start_date", ""),
        "end_date":      config.get("end_date", ""),
        "cap_tier":      config.get("cap_tier", "all"),
        "rebalance_freq":config.get("rebalance_freq", ""),
        "min_price":     config.get("min_price", 5.0),
    }, sort_keys=True)
    h4 = hashlib.sha256(sig.encode()).hexdigest()[:4].upper()
    return f"FM-{score_short}-{date_str}-{h4}"


# ── Save a factor model result ─────────────────────────────────────────────────

def save_factor_model(result: dict, overwrite: bool = True) -> str:
    """
    Save a completed factor backtest to the strategy bank.
    Returns the strategy_id.
    Skips if already exists (same ID) unless overwrite=True.
    """
    if result.get("status") != "complete":
        raise ValueError("Only 'complete' results can be saved")

    config  = result.get("config", {})
    fm      = result.get("factor_metrics", {})
    score   = config.get("score_column", result.get("score_column", "unknown"))

    strategy_id = _make_strategy_id(score, config)

    def _safe(v):
        """Convert numpy/nan/inf to Python scalar."""
        if v is None: return None
        if isinstance(v, (list, dict)): return v
        try:
            f = float(v)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except (TypeError, ValueError):
            return v

    def _jdump(obj):
        """JSON-safe dump with nan/inf cleaning."""
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

    # Extract Q1 and Qn bucket metrics
    n_buckets = int(config.get("n_buckets", 5))
    bm = result.get("bucket_metrics", {})
    q1m = bm.get("1", {}) or {}
    qnm = bm.get(str(n_buckets), {}) or {}

    con = _get_bank()
    try:
        # Check if exists
        existing = con.execute(
            "SELECT strategy_id FROM factor_models WHERE strategy_id = ?",
            [strategy_id]
        ).fetchone()

        if existing and not overwrite:
            return strategy_id  # already saved

        row = {
            "strategy_id":          strategy_id,
            "run_label":            result.get("run_label", ""),
            "score_column":         score,
            "score_display":        result.get("run_label", score),
            "n_buckets":            n_buckets,
            "hold_months":          int(config.get("hold_months", 6)),
            "start_date":           config.get("start_date", ""),
            "end_date":             config.get("end_date", ""),
            "cap_tier":             config.get("cap_tier", "all"),
            "rebalance_freq":       config.get("rebalance_freq", ""),
            "min_price":            _safe(config.get("min_price", 5.0)),
            "min_adv_usd":          _safe(config.get("min_adv_usd", 1_000_000)),
            "cost_bps":             _safe(config.get("cost_bps", 15.0)),
            # Signal quality
            "ic_mean":              _safe(fm.get("ic_mean")),
            "ic_std":               _safe(fm.get("ic_std")),
            "icir":                 _safe(fm.get("icir")),
            "ic_hit_rate":          _safe(fm.get("ic_hit_rate")),
            "spearman_rho":         _safe(fm.get("spearman_rho")),
            "monotonicity_score":   _safe(fm.get("monotonicity_score")),
            # Quintile perf
            "quintile_spread_cagr": _safe(fm.get("quintile_spread_cagr")),
            "q1_cagr":              _safe(q1m.get("cagr")),
            "q1_sharpe":            _safe(q1m.get("sharpe")),
            "q1_max_dd":            _safe(q1m.get("max_dd")),
            "q1_sortino":           _safe(q1m.get("sortino")),
            "q1_surefire":          _safe(q1m.get("surefire_ratio")),
            "q1_equity_r2":         _safe(q1m.get("equity_r2")),
            "qn_cagr":              _safe(qnm.get("cagr")),
            "qn_sharpe":            _safe(qnm.get("sharpe")),
            "n_obs":                int(result.get("n_obs", 0)),
            "n_stocks_avg":         _safe(result.get("n_stocks_avg")),
            "elapsed_s":            _safe(result.get("elapsed_s")),
            # JSON blobs — ALL buckets stored for full tearsheet replay
            "config_json":              _jdump(config),
            "bucket_metrics_json":      _jdump(bm),
            "ic_data_json":             _jdump(result.get("ic_data", [])),
            "bucket_equity_json":       _jdump(result.get("bucket_equity", {})),
            "annual_ret_json":          _jdump(result.get("annual_ret_by_bucket", {})),
            "dates_json":               _jdump(result.get("dates", [])),
            "universe_equity_json":     _jdump(result.get("universe_equity", [])),
            "tortoriello_json":         _jdump(result.get("tortoriello", {})),
            "universe_metrics_json":    _jdump(result.get("universe_metrics", {})),
            "period_data_json":         _jdump(result.get("period_data", [])),
            "sector_attribution_json":  _jdump(result.get("sector_attribution", [])),
            "spy_metrics_json":         _jdump(result.get("spy_metrics", {})),
            "fitness_json":             _jdump(result.get("fitness", {})),
            "trade_log_json":           _jdump(result.get("trade_log", [])),
            # Fitness scalars for fast DB sorting
            "staircase_score":  _safe(fm.get("staircase_score")),
            "alpha_win_rate":   _safe(fm.get("alpha_win_rate")),
            "avg_annual_alpha": _safe(fm.get("avg_annual_alpha")),
            "bear_score":       _safe(fm.get("bear_score")),
            "bull_score":       _safe(fm.get("bull_score")),
            "downside_capture": _safe(fm.get("downside_capture")),
            "alpha_sharpe":     _safe(fm.get("alpha_sharpe")),
            "obq_fund_score":   _safe(fm.get("obq_fund_score")),
            "q1_calmar":        _safe(q1m.get("calmar")),
            "q1_surefire":      _safe(q1m.get("surefire_ratio")),
            "status":               "saved",
            "notes":                "",
            "promoted_to":          None,
            "tags":                 "",
        }

        if existing:
            # Update
            set_clause = ", ".join(f"{k} = ?" for k in row if k != "strategy_id")
            vals = [row[k] for k in row if k != "strategy_id"] + [strategy_id]
            con.execute(f"UPDATE factor_models SET {set_clause} WHERE strategy_id = ?", vals)
        else:
            # Insert
            cols = ", ".join(row.keys())
            plh  = ", ".join("?" for _ in row)
            con.execute(f"INSERT INTO factor_models ({cols}) VALUES ({plh})", list(row.values()))

        con.commit()

        # Also save to dedicated trade log DB (separate table, fast queries)
        trade_log = result.get("trade_log", [])
        if trade_log:
            try:
                from engine.trade_log_db import save_factor_trades
                n_saved = save_factor_trades(strategy_id, score, trade_log)
            except Exception as _tl_err:
                pass  # non-fatal — trade log DB is supplementary

        return strategy_id

    finally:
        con.close()


# ── Query the bank ─────────────────────────────────────────────────────────────

def get_all_models(limit: int = 200) -> list[dict]:
    """Return all saved factor models sorted by ICIR desc."""
    con = _get_bank()
    try:
        rows = con.execute(f"""
            SELECT strategy_id, created_at::VARCHAR, run_label, score_column,
                   n_buckets, hold_months, start_date, end_date, cap_tier,
                   ic_mean, icir, ic_hit_rate, monotonicity_score, spearman_rho,
                   quintile_spread_cagr, q1_cagr, q1_sharpe, q1_max_dd, q1_surefire, q1_calmar,
                   qn_cagr, n_obs, n_stocks_avg, elapsed_s,
                   staircase_score, alpha_win_rate, avg_annual_alpha,
                   bear_score, bull_score, downside_capture, alpha_sharpe, obq_fund_score,
                   status, notes, promoted_to, tags
            FROM factor_models
            ORDER BY created_at DESC NULLS LAST
            LIMIT {limit}
        """).fetchdf()
        return rows.to_dict(orient="records")
    finally:
        con.close()


def get_model(strategy_id: str) -> Optional[dict]:
    """Load full model including JSON blobs."""
    con = _get_bank()
    try:
        row = con.execute(
            "SELECT * FROM factor_models WHERE strategy_id = ?", [strategy_id]
        ).fetchdf()
        if row.empty: return None
        d = row.iloc[0].to_dict()
        # Parse JSON blobs
        json_cols = (
            "config_json", "bucket_metrics_json", "ic_data_json",
            "bucket_equity_json", "annual_ret_json",
            "dates_json", "universe_equity_json", "tortoriello_json", "universe_metrics_json",
            "period_data_json", "sector_attribution_json", "spy_metrics_json", "fitness_json",
            "trade_log_json",
        )
        for col in json_cols:
            raw = d.get(col)
            if raw is None:
                d[col] = [] if col in ("dates_json","ic_data_json","annual_ret_json","period_data_json","sector_attribution_json") else {}
                continue
            try:
                d[col] = json.loads(raw)
            except Exception:
                d[col] = [] if col in ("dates_json","ic_data_json","annual_ret_json","period_data_json","sector_attribution_json") else {}
        return d
    finally:
        con.close()


def update_model_notes(strategy_id: str, notes: str = "", tags: str = "") -> bool:
    con = _get_bank()
    try:
        con.execute("UPDATE factor_models SET notes=?, tags=? WHERE strategy_id=?",
                    [notes, tags, strategy_id])
        con.commit()
        return True
    except Exception:
        return False
    finally:
        con.close()


def get_bank_summary() -> dict:
    """Quick stats for the bank."""
    con = _get_bank()
    try:
        r = con.execute("""
            SELECT COUNT(*) n_models,
                   COUNT(DISTINCT score_column) n_scores,
                   MAX(icir) best_icir,
                   MAX(quintile_spread_cagr) best_spread,
                   MAX(created_at)::VARCHAR last_run
            FROM factor_models
        """).fetchone()
        return {"n_models": r[0], "n_scores": r[1], "best_icir": r[2],
                "best_spread": r[3], "last_run": r[4]}
    finally:
        con.close()
