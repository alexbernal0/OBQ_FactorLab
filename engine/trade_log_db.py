"""
engine/trade_log_db.py
======================
Dedicated trade log database for OBQ FactorLab.
Stores every Q1 entry/exit across all factor backtests AND
every portfolio position across all portfolio backtests.

Separate from strategy_bank.py / portfolio_bank.py to:
  - Keep bank tables fast (no massive blobs)
  - Enable proper SQL queries across trades (win rate by sector, year, score)
  - Support full audit trail independently of tearsheet rendering

DB: D:/OBQ_AI/OBQ_FactorLab_Bank/trade_log.duckdb

Tables:
  factor_trades    -- one row per Q1 stock per period
  portfolio_trades -- one row per portfolio holding per period
"""
from __future__ import annotations
import os, json, math
from pathlib import Path
import duckdb
import numpy as np

BANK_DIR   = Path(os.environ.get("OBQ_BANK_DIR", r"D:\OBQ_AI\OBQ_FactorLab_Bank"))
TRADE_DB   = BANK_DIR / "trade_log.duckdb"


def _get_trade_db() -> duckdb.DuckDBPyConnection:
    BANK_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(TRADE_DB))
    _ensure_schema(con)
    return con


def _ensure_schema(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS factor_trades (
        strategy_id   VARCHAR,
        score_column  VARCHAR,
        entry_date    VARCHAR,
        exit_date     VARCHAR,
        symbol        VARCHAR,
        sector        VARCHAR,
        score         DOUBLE,
        bucket        INTEGER,
        return_pct    DOUBLE,
        market_cap_B  DOUBLE,
        entry_price   DOUBLE,
        exit_price    DOUBLE
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_trades (
        strategy_id   VARCHAR,
        score_column  VARCHAR,
        entry_date    VARCHAR,
        exit_date     VARCHAR,
        symbol        VARCHAR,
        sector        VARCHAR,
        score         DOUBLE,
        weight        DOUBLE,
        return_pct    DOUBLE,
        market_cap_B  DOUBLE,
        entry_price   DOUBLE,
        exit_price    DOUBLE,
        turnover_pct  DOUBLE
    )
    """)

    # Indexes for fast queries
    try:
        con.execute("CREATE INDEX IF NOT EXISTS idx_ft_sid ON factor_trades(strategy_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ft_sym ON factor_trades(symbol)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_pt_sid ON portfolio_trades(strategy_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_pt_sym ON portfolio_trades(symbol)")
    except Exception:
        pass


def save_factor_trades(strategy_id: str, score_column: str, trade_log: list) -> int:
    """
    Save factor Q1 trade log to dedicated table.
    Deletes existing entries for this strategy_id first (idempotent).
    Returns number of rows inserted.
    """
    if not trade_log:
        return 0

    con = _get_trade_db()
    try:
        # Delete existing entries for this strategy
        con.execute("DELETE FROM factor_trades WHERE strategy_id = ?", [strategy_id])

        # Bulk insert
        rows = []
        for t in trade_log:
            rows.append((
                strategy_id,
                score_column,
                t.get("entry_date"),
                t.get("exit_date"),
                t.get("symbol"),
                t.get("sector"),
                t.get("score"),
                t.get("bucket", 1),
                t.get("return_pct"),
                t.get("market_cap_B"),
                t.get("entry_price"),
                t.get("exit_price"),
            ))

        con.executemany("""
            INSERT INTO factor_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        con.commit()
        return len(rows)
    finally:
        con.close()


def save_portfolio_trades(strategy_id: str, score_column: str, trade_log: list) -> int:
    """
    Save portfolio trade log to dedicated table.
    Returns number of rows inserted.
    """
    if not trade_log:
        return 0

    con = _get_trade_db()
    try:
        con.execute("DELETE FROM portfolio_trades WHERE strategy_id = ?", [strategy_id])

        rows = []
        for t in trade_log:
            rows.append((
                strategy_id,
                score_column,
                t.get("entry_date"),
                t.get("exit_date"),
                t.get("symbol"),
                t.get("sector"),
                t.get("score"),
                t.get("weight"),
                t.get("return_pct"),
                t.get("market_cap_B"),
                t.get("entry_price"),
                t.get("exit_price"),
                None,  # turnover_pct not stored per-trade
            ))

        con.executemany("""
            INSERT INTO portfolio_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        con.commit()
        return len(rows)
    finally:
        con.close()


def get_factor_trades(strategy_id: str) -> list:
    """Retrieve all Q1 trades for a factor strategy."""
    con = _get_trade_db()
    try:
        rows = con.execute("""
            SELECT entry_date, exit_date, symbol, sector, score, bucket,
                   return_pct, market_cap_B, entry_price, exit_price
            FROM factor_trades
            WHERE strategy_id = ?
            ORDER BY entry_date, symbol
        """, [strategy_id]).fetchdf()
        return rows.to_dict(orient="records")
    finally:
        con.close()


def get_portfolio_trades(strategy_id: str) -> list:
    """Retrieve all trades for a portfolio strategy."""
    con = _get_trade_db()
    try:
        rows = con.execute("""
            SELECT entry_date, exit_date, symbol, sector, score, weight,
                   return_pct, market_cap_B, entry_price, exit_price
            FROM portfolio_trades
            WHERE strategy_id = ?
            ORDER BY entry_date, symbol
        """, [strategy_id]).fetchdf()
        return rows.to_dict(orient="records")
    finally:
        con.close()


def get_trade_summary(strategy_id: str, trade_type: str = "factor") -> dict:
    """
    Get aggregate trade statistics for a strategy.
    Returns: total_trades, win_rate, avg_return, best_trade, worst_trade,
             by_sector breakdown, by_year breakdown.
    """
    con = _get_trade_db()
    try:
        tbl = "factor_trades" if trade_type == "factor" else "portfolio_trades"
        r = con.execute(f"""
            SELECT
                COUNT(*) as total_trades,
                ROUND(AVG(CASE WHEN return_pct > 0 THEN 1.0 ELSE 0.0 END) * 100, 1) as win_rate_pct,
                ROUND(AVG(return_pct), 2) as avg_return_pct,
                ROUND(MAX(return_pct), 2) as best_trade_pct,
                ROUND(MIN(return_pct), 2) as worst_trade_pct,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT YEAR(entry_date::DATE)) as years_covered
            FROM {tbl}
            WHERE strategy_id = ? AND return_pct IS NOT NULL
        """, [strategy_id]).fetchone()

        if not r or r[0] == 0:
            return {"total_trades": 0}

        # By sector
        sectors = con.execute(f"""
            SELECT sector,
                   COUNT(*) as n,
                   ROUND(AVG(return_pct), 2) as avg_ret,
                   ROUND(AVG(CASE WHEN return_pct > 0 THEN 1.0 ELSE 0.0 END) * 100, 1) as win_pct
            FROM {tbl}
            WHERE strategy_id = ? AND return_pct IS NOT NULL
            GROUP BY sector ORDER BY avg_ret DESC
        """, [strategy_id]).fetchdf().to_dict(orient="records")

        # By year
        by_year = con.execute(f"""
            SELECT YEAR(entry_date::DATE) as year,
                   COUNT(*) as n,
                   ROUND(AVG(return_pct), 2) as avg_ret,
                   ROUND(AVG(CASE WHEN return_pct > 0 THEN 1.0 ELSE 0.0 END) * 100, 1) as win_pct
            FROM {tbl}
            WHERE strategy_id = ? AND return_pct IS NOT NULL
            GROUP BY year ORDER BY year
        """, [strategy_id]).fetchdf().to_dict(orient="records")

        return {
            "total_trades":     r[0],
            "win_rate_pct":     r[1],
            "avg_return_pct":   r[2],
            "best_trade_pct":   r[3],
            "worst_trade_pct":  r[4],
            "unique_symbols":   r[5],
            "years_covered":    r[6],
            "by_sector":        sectors,
            "by_year":          by_year,
        }
    finally:
        con.close()


def count_trades() -> dict:
    """Quick count of all trades in the DB."""
    con = _get_trade_db()
    try:
        f = con.execute("SELECT COUNT(*), COUNT(DISTINCT strategy_id) FROM factor_trades").fetchone()
        p = con.execute("SELECT COUNT(*), COUNT(DISTINCT strategy_id) FROM portfolio_trades").fetchone()
        return {
            "factor_trades": f[0], "factor_strategies": f[1],
            "portfolio_trades": p[0], "portfolio_strategies": p[1],
        }
    finally:
        con.close()
