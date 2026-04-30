"""Flask app for OBQ_FactorLab."""
import os, sys, json, threading, queue, time, uuid, math
from pathlib import Path
from flask import Flask, render_template, jsonify, request, Response, stream_with_context

# Window reference — set by main.py after webview.create_window()
_webview_window = None

# Pre-cached SPY result — computed once at startup, served instantly to UI
_spy_cache: dict = {}
_spy_cache_lock = threading.Lock()


def _preload_spy():
    """Run SPY backtest in background at startup so it's ready when UI loads."""
    try:
        from engine.spy_backtest import run_spy_backtest
        result = run_spy_backtest()
        with _spy_cache_lock:
            _spy_cache.update(result)
    except Exception as e:
        with _spy_cache_lock:
            _spy_cache["error"] = str(e)

threading.Thread(target=_preload_spy, name="spy-preload", daemon=True).start()

# Ensure project root on path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.data import (
    UniverseConfig, get_available_factors, get_score_table_range,
    FACTOR_MAP, SECTOR_CHOICES, INDEX_CHOICES,
)
from engine.backtest import run_backtest
from engine.spy_backtest import run_spy_backtest
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

app = Flask(__name__,
            static_folder=str(ROOT / "gui" / "static"),
            static_url_path="/static",
            template_folder=str(ROOT / "gui"))

# ── Active run tracking ───────────────────────────────────────────────────
_runs: dict = {}         # run_id -> {status, result, log_q}
_run_lock = threading.Lock()


@app.route("/")
def index():
    return app.send_static_file("index.html")


# ── Config endpoints ────────────────────────────────────────────────────────

@app.route("/api/config/factors")
def config_factors():
    return jsonify(get_available_factors())


@app.route("/api/config/options")
def config_options():
    return jsonify({
        "factors":           list(FACTOR_MAP.keys()),
        "indices":           INDEX_CHOICES,
        "sectors":           SECTOR_CHOICES,
        "rebalance_freqs":   ["Monthly", "Quarterly", "Semi-Annual", "Annual"],
        "na_handling":       ["Exclude", "Worst", "Neutral"],
        "position_sizing":   ["Equal", "Vol-Parity"],
        "direction":         ["Long Only", "Long-Short"],
        "model_types":       ["quintile", "topn"],
    })


@app.route("/api/config/factor_range")
def config_factor_range():
    factor = request.args.get("factor", "value")
    return jsonify(get_score_table_range(factor))


# ── Backtest run ────────────────────────────────────────────────────────────

@app.route("/api/backtest/run", methods=["POST"])
def backtest_run():
    data = request.get_json(force=True) or {}
    run_id = str(uuid.uuid4())
    cfg = UniverseConfig(**{k: v for k, v in data.items()
                            if k in UniverseConfig.__dataclass_fields__})

    log_q: queue.Queue = queue.Queue(maxsize=500)
    with _run_lock:
        _runs[run_id] = {"status": "running", "result": None, "log_q": log_q}

    def _cb(level, msg):
        try: log_q.put_nowait({"level": level, "msg": msg, "ts": time.time()})
        except: pass

    def _worker():
        try:
            result = run_backtest(cfg, cb=_cb)
            with _run_lock:
                _runs[run_id]["result"] = result
                _runs[run_id]["status"] = "complete"
            _cb("ok", f"DONE  run_id={run_id}")
        except Exception as e:
            import traceback
            _cb("error", f"FAILED: {e}")
            with _run_lock:
                _runs[run_id]["status"] = "error"
                _runs[run_id]["result"] = {"status": "error", "error": str(e)}

    threading.Thread(target=_worker, name=f"run-{run_id[:8]}", daemon=True).start()
    return jsonify({"run_id": run_id, "status": "running"})


@app.route("/api/backtest/status/<run_id>")
def backtest_status(run_id):
    info = _runs.get(run_id)
    if not info:
        return jsonify({"error": "unknown run"}), 404
    return jsonify({"run_id": run_id, "status": info["status"],
                    "has_result": info["result"] is not None})


@app.route("/api/backtest/result/<run_id>")
def backtest_result(run_id):
    info = _runs.get(run_id)
    if not info:
        return jsonify({"error": "unknown run"}), 404
    if info["status"] == "running":
        return jsonify({"status": "running"}), 202
    # Deep-clean result to remove any Inf/NaN before serialization
    def _clean(obj):
        if isinstance(obj, dict): return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list): return [_clean(v) for v in obj]
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj): return None
            return obj
        return obj
    return jsonify(_clean(info["result"]))


@app.route("/api/backtest/stream/<run_id>")
def backtest_stream(run_id):
    """SSE log stream for a running backtest."""
    def gen():
        info = _runs.get(run_id)
        if not info:
            yield "data: {\"error\":\"unknown run\"}\n\n"
            return
        q = info["log_q"]
        yield f"data: {json.dumps({'level':'info','msg':'stream open'})}\n\n"
        while True:
            try:
                entry = q.get(timeout=15)
                yield f"data: {json.dumps(entry)}\n\n"
                if info.get("status") in ("complete","error") and q.empty():
                    break
            except queue.Empty:
                yield ": heartbeat\n\n"
                if info.get("status") in ("complete","error"):
                    break
    return Response(stream_with_context(gen()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.route("/api/backtest/spy_preloaded")
def backtest_spy_preloaded():
    """Return pre-cached SPY result. Returns 202 if still computing."""
    with _spy_cache_lock:
        if not _spy_cache:
            return jsonify({"status": "loading"}), 202
        if "error" in _spy_cache and len(_spy_cache) == 1:
            return jsonify({"status": "error", "error": _spy_cache["error"]}), 500
        return jsonify(dict(_spy_cache))


@app.route("/api/backtest/spy", methods=["POST"])
def backtest_spy():
    """Fast SPY long-only benchmark — no factor engine, just prices."""
    data = request.get_json(force=True) or {}
    run_id = "spy-" + str(uuid.uuid4())[:8]
    log_q: queue.Queue = queue.Queue(maxsize=200)
    with _run_lock:
        _runs[run_id] = {"status": "running", "result": None, "log_q": log_q}

    def _cb(level, msg):
        try: log_q.put_nowait({"level": level, "msg": msg, "ts": time.time()})
        except: pass

    def _worker():
        try:
            result = run_spy_backtest(
                start_date=data.get("start_date", "2010-01-31"),
                end_date=data.get("end_date") or None,
                rf_annual=float(data.get("rf_annual", 0.04)),
                cb=_cb,
            )
            with _run_lock:
                _runs[run_id]["result"] = result
                _runs[run_id]["status"] = "complete" if result.get("status") != "error" else "error"
            _cb("ok", f"Done — {result.get('elapsed_s')}s")
        except Exception as e:
            import traceback; traceback.print_exc()
            _cb("error", str(e))
            with _run_lock:
                _runs[run_id]["status"] = "error"
                _runs[run_id]["result"] = {"status": "error", "error": str(e)}

    threading.Thread(target=_worker, name=f"spy-{run_id}", daemon=True).start()
    return jsonify({"run_id": run_id, "status": "running"})


@app.route("/api/backtest/export_pdf", methods=["POST"])
def export_pdf():
    import datetime
    from gui.app_pdf import generate as gen_pdf
    data = request.get_json(force=True) or {}
    run_id = data.get("run_id")
    info = _runs.get(run_id)
    if not info or not info.get("result"):
        return jsonify({"error": "run not found"}), 404
    result = info["result"]
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"OBQ_FactorLab_{result.get('factor','factor')}_{result.get('mode','run')}_{ts}.pdf"
    pdf_path = Path.home() / "Downloads" / fname
    try:
        gen_pdf(result, pdf_path)
        return jsonify({"path": str(pdf_path), "filename": fname})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/evaljs", methods=["POST"])
def evaljs():
    """Dev tool: evaluate JS in webview and return result."""
    js = request.form.get("js") or (request.get_json(force=True) or {}).get("js", "")
    if not js:
        return jsonify({"error": "no js"}), 400
    try:
        import gui.app as _self
        win = _self._webview_window
        if win is None:
            return jsonify({"error": "no webview window ref — app not ready"}), 503
        result = win.evaluate_js(js)
        return jsonify({"result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/image", methods=["POST"])
def export_image():
    """Legacy endpoint — kept for compatibility. Use /api/export/image_data instead."""
    return jsonify({"error": "Use IMG button in the app — it renders charts directly from Plotly"}), 400


@app.route("/api/export/image_data", methods=["POST"])
def export_image_data():
    """Receive pre-rendered PNG base64 from JS canvas compositing and save to Downloads."""
    import datetime as _dt, pathlib as _pl, base64 as _b64
    data = request.get_json(force=True) or {}
    run_label  = data.get("run_label", "tearsheet")
    image_b64  = data.get("image_b64", "")

    if not image_b64:
        return jsonify({"error": "No image data received"}), 400

    ts   = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in run_label)[:40]
    fname = f"OBQ_FactorLab_{safe}_{ts}.png"
    path  = _pl.Path.home() / "Downloads" / fname

    try:
        img_bytes = _b64.b64decode(image_b64)
        path.write_bytes(img_bytes)
        return jsonify({"path": str(path), "filename": fname})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/csv", methods=["POST"])
def export_csv():
    """Export tearsheet metrics to CSV in Downloads folder."""
    import csv, datetime as _dt, pathlib as _pl, math as _math
    data = request.get_json(force=True) or {}
    run_label = data.get("run_label", "backtest")
    metrics   = data.get("metrics") or {}
    result    = data.get("result") or {}

    ts   = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in run_label)[:40]
    fname = f"OBQ_FactorLab_{safe}_{ts}.csv"
    path  = _pl.Path.home() / "Downloads" / fname

    # Flatten metrics + key result fields
    def _fmt(v):
        if v is None: return ""
        if isinstance(v, (list, dict)): return str(v)
        try:
            f = float(v)
            return "" if (_math.isnan(f) or _math.isinf(f)) else str(round(f, 6))
        except Exception:
            return str(v)

    rows = [
        ("Run Label",   run_label),
        ("Mode",        result.get("mode", "")),
        ("Start Date",  result.get("start_date", "")),
        ("End Date",    result.get("end_date", "")),
        ("N Periods",   result.get("n_periods", "")),
        ("Export Date", _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        ("", ""),
        ("METRIC", "VALUE"),
    ]
    for k, v in sorted(metrics.items()):
        rows.append((k, _fmt(v)))

    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerows(rows)
        return jsonify({"path": str(path), "filename": fname})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/pdf", methods=["POST"])
def export_pdf_report():
    """Export comprehensive tearsheet PDF to Downloads — equity chart + full metrics table."""
    import datetime as _dt, pathlib as _pl, math as _math
    data = request.get_json(force=True) or {}
    run_label = data.get("run_label", "backtest")
    metrics   = data.get("metrics") or {}
    result    = data.get("result") or {}

    ts   = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in run_label)[:40]
    fname = f"OBQ_FactorLab_{safe}_{ts}.pdf"
    path  = _pl.Path.home() / "Downloads" / fname

    PCT_KEYS = {"cagr","total_return","expected_monthly","expected_annual","ann_vol",
                "max_dd","avg_dd","var_95","var_99","cvar_95","cvar_99",
                "win_rate_monthly","win_rate_yearly","win_rate_quarterly",
                "avg_up_month","avg_down_month","best_month","worst_month",
                "best_year","worst_year","exposure","alpha","tracking_error",
                "up_capture","down_capture","m_squared","psr","gain_pain_ratio",
                "lake_ratio","mar_ratio"}

    def _fmt(k, v):
        if v is None: return "—"
        if isinstance(v, list): return str([round(x,3) if isinstance(x,float) else x for x in v])
        try:
            f = float(v)
            if _math.isnan(f) or _math.isinf(f): return "—"
            if k in PCT_KEYS: return f"{f*100:+.2f}%" if f < 0 or k in {"cagr","total_return","expected_monthly","expected_annual","alpha"} else f"{f*100:.2f}%"
            if isinstance(v, bool): return str(v)
            if abs(f) > 999: return f"{f:,.0f}"
            return f"{f:.4f}"
        except Exception:
            return str(v)

    SECTIONS = [
        ("PERIOD", ["n_years","n_periods"]),
        ("RETURNS", ["cagr","total_return","expected_monthly","expected_annual","ann_vol"]),
        ("RISK-ADJUSTED", ["sharpe","smart_sharpe","sortino","smart_sortino","calmar",
                           "omega","gain_pain_ratio","serenity","pain_ratio",
                           "recovery_factor","mar_ratio","system_score","k_ratio","psr"]),
        ("OBQ SUREFIRE SUITE", ["iudr","surefire_ratio","integrated_dd","integrated_up"]),
        ("RISK", ["max_dd","avg_dd","ulcer_index","pain_index","lake_ratio",
                  "var_95","var_99","cvar_95","cvar_99"]),
        ("DISTRIBUTION", ["skewness","kurtosis","tail_ratio","best_month","worst_month",
                          "best_year","worst_year"]),
        ("WIN / LOSS", ["win_rate_monthly","win_rate_quarterly","win_rate_yearly",
                        "avg_up_month","avg_down_month","payoff_ratio","profit_factor",
                        "common_sense_ratio","cpc_index","max_consec_wins","max_consec_losses","exposure"]),
        ("STATISTICAL", ["sharpe_tstat","haircut_sharpe","sharpe_ci_95","jarque_bera_p"]),
        ("VS BENCHMARK", ["alpha","beta","info_ratio","tracking_error","r_squared",
                          "up_capture","down_capture","treynor_ratio","m_squared"]),
    ]

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        from matplotlib.backends.backend_pdf import PdfPages
        import numpy as np

        equity = result.get("portfolio_equity", [])
        dates  = result.get("equity_dates", [])

        with PdfPages(str(path)) as pdf:
            # ── PAGE 1: Header + KPIs + Equity + Drawdown ─────────────────
            fig = plt.figure(figsize=(11, 8.5))
            fig.patch.set_facecolor("white")

            # Dark header band
            ax_h = fig.add_axes([0, 0.93, 1, 0.07])
            ax_h.axis("off"); ax_h.set_facecolor("#0d1b2a")
            ax_h.add_patch(plt.Rectangle((0,0),1,1,transform=ax_h.transAxes,facecolor="#0d1b2a"))
            ax_h.text(0.02, 0.62, run_label, transform=ax_h.transAxes,
                      fontsize=13, fontweight="bold", color="#c9a84c", va="center")
            ax_h.text(0.02, 0.22, f"OBQ Factor Lab  |  {result.get('start_date','')} — {result.get('end_date','')}  |  {result.get('n_periods','')} periods",
                      transform=ax_h.transAxes, fontsize=8, color="#8fa3ba", va="center")

            # KPI boxes
            kpis = [
                ("CAGR",     f"{(metrics.get('cagr',0) or 0)*100:+.1f}%", "#2e7d32"),
                ("SHARPE",   f"{metrics.get('sharpe',0) or 0:.2f}",       "#1565c0"),
                ("MAX DD",   f"{(metrics.get('max_dd',0) or 0)*100:.1f}%","#c62828"),
                ("SORTINO",  f"{metrics.get('sortino',0) or 0:.2f}",      "#1565c0"),
                ("CALMAR",   f"{metrics.get('calmar',0) or 0:.2f}",       "#1565c0"),
                ("WIN RATE", f"{(metrics.get('win_rate_monthly',0) or 0)*100:.1f}%","#2e7d32"),
                ("SUREFIRE", f"{metrics.get('surefire_ratio',0) or 0:.1f}","#7b1fa2"),
                ("IUDR",     f"{metrics.get('iudr',0) or 0:.1f}",         "#7b1fa2"),
            ]
            n_kpi = len(kpis)
            for i, (lbl, val, col) in enumerate(kpis):
                x = 0.01 + i * (0.98/n_kpi)
                w = 0.97/n_kpi - 0.005
                ax_k = fig.add_axes([x, 0.86, w, 0.065])
                ax_k.axis("off")
                ax_k.add_patch(plt.Rectangle((0,0),1,1,transform=ax_k.transAxes,
                                              facecolor=col, alpha=0.88, linewidth=0))
                ax_k.text(0.5, 0.72, lbl, transform=ax_k.transAxes,
                          fontsize=5.5, fontweight="bold", color="#c9a84c", ha="center", va="center")
                ax_k.text(0.5, 0.28, val, transform=ax_k.transAxes,
                          fontsize=10, fontweight="bold", color="white", ha="center", va="center")

            # Equity curve
            if equity and dates:
                ax1 = fig.add_axes([0.07, 0.48, 0.90, 0.36])
                ax1.plot(range(len(dates)), equity, color="#0066cc", lw=1.3, label="Portfolio")
                ax1.fill_between(range(len(dates)), equity, 1.0, alpha=0.07, color="#0066cc")
                ax1.axhline(1.0, color="#9ca3af", lw=0.7, linestyle="--")
                step = max(1, len(dates)//8)
                ax1.set_xticks(range(0, len(dates), step))
                ax1.set_xticklabels([str(dates[i])[:7] for i in range(0, len(dates), step)], fontsize=6, rotation=30)
                ax1.set_ylabel("Growth (×)", fontsize=8); ax1.grid(True, alpha=0.25)
                ax1.set_title("Equity Curve", fontsize=9, fontweight="bold", loc="left")
                ax1.legend(fontsize=7, loc="upper left")

                # Drawdown
                ax2 = fig.add_axes([0.07, 0.10, 0.90, 0.35])
                eq_arr = np.array(equity, dtype=float)
                peak = np.maximum.accumulate(eq_arr)
                dd = (eq_arr / peak - 1) * 100
                ax2.fill_between(range(len(dates)), dd, 0, color="#dc2626", alpha=0.45, label="Strategy DD")
                ax2.plot(range(len(dates)), dd, color="#dc2626", lw=0.8)
                ax2.axhline(0, color="#374151", lw=0.6)
                ax2.set_xticks(range(0, len(dates), step))
                ax2.set_xticklabels([str(dates[i])[:7] for i in range(0, len(dates), step)], fontsize=6, rotation=30)
                ax2.set_ylabel("Drawdown (%)", fontsize=8); ax2.grid(True, alpha=0.25)
                ax2.set_title("Drawdown Underwater", fontsize=9, fontweight="bold", loc="left")
                ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))

            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

            # ── PAGE 2: Complete Metrics Table ─────────────────────────────
            fig2 = plt.figure(figsize=(11, 8.5))
            fig2.patch.set_facecolor("white")

            # Header
            ax_h2 = fig2.add_axes([0, 0.96, 1, 0.04])
            ax_h2.axis("off")
            ax_h2.add_patch(plt.Rectangle((0,0),1,1,transform=ax_h2.transAxes,facecolor="#0d1b2a"))
            ax_h2.text(0.02, 0.5, f"{run_label}  |  PERFORMANCE METRICS",
                       transform=ax_h2.transAxes, fontsize=10, fontweight="bold",
                       color="#c9a84c", va="center")

            ax2m = fig2.add_axes([0.02, 0.01, 0.96, 0.94])
            ax2m.axis("off")

            # Build two-column layout of all metrics
            C_DARK, C_GOLD, C_GREEN, C_RED, C_PURPLE = "#0d1b2a","#c9a84c","#2e7d32","#c62828","#6a1b9a"
            C_LGREY = "#f8f9fa"

            all_rows = []
            for sec_name, keys in SECTIONS:
                all_rows.append(("__SEC__", sec_name))
                for k in keys:
                    v = metrics.get(k)
                    all_rows.append((k.replace("_"," ").title(), _fmt(k, v)))

            n_rows = len(all_rows)
            row_h = min(0.96 / n_rows, 0.032)
            y = 0.98

            for lbl, val in all_rows:
                if lbl == "__SEC__":
                    ax2m.add_patch(plt.Rectangle((0, y - row_h), 1, row_h,
                                                  transform=ax2m.transAxes,
                                                  facecolor=C_DARK, zorder=0))
                    ax2m.text(0.01, y - row_h/2, val.upper(),
                              transform=ax2m.transAxes, fontsize=7.5, fontweight="bold",
                              color=C_GOLD, va="center")
                else:
                    bg = C_LGREY if (all_rows.index((lbl,val)) % 2 == 0) else "white"
                    ax2m.add_patch(plt.Rectangle((0, y - row_h), 1, row_h,
                                                  transform=ax2m.transAxes,
                                                  facecolor=bg, zorder=0))
                    ax2m.text(0.01, y - row_h/2, lbl,
                              transform=ax2m.transAxes, fontsize=7, color=C_DARK, va="center")
                    # Color value
                    try:
                        fv = float(val.replace("%","").replace("+",""))
                        vc = C_RED if fv < 0 else (C_GREEN if fv > 0 else C_DARK)
                    except Exception:
                        vc = C_DARK
                    if any(k in lbl.lower() for k in ["iudr","surefire"]):
                        vc = C_PURPLE
                    ax2m.text(0.55, y - row_h/2, val,
                              transform=ax2m.transAxes, fontsize=7, fontweight="bold",
                              color=vc, va="center")
                y -= row_h

            pdf.savefig(fig2, bbox_inches="tight")
            plt.close(fig2)

        return jsonify({"path": str(path), "filename": fname})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/snap")
def snap():
    """Dev tool: capture screen, return base64 PNG. No file saved."""
    import base64, io
    try:
        import PIL.ImageGrab
        img = PIL.ImageGrab.grab()
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()
        return jsonify({"img": b64})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# FACTOR BACKTEST API  —  /api/factor/*
# All routes prefixed /api/factor/ to avoid any conflict with existing routes
# ═══════════════════════════════════════════════════════════════════════════════

# In-memory store of factor backtest runs (keyed by run_id)
_factor_runs: dict = {}
_factor_lock = threading.Lock()


@app.route("/api/factor/scores")
def factor_scores():
    """Return all available score columns with metadata."""
    try:
        from engine.factor_backtest import get_available_scores
        scores = get_available_scores()
        return jsonify({"scores": scores})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/factor/score_range")
def factor_score_range():
    """Return date range and symbol count for a given score column."""
    score_col = request.args.get("score", "jcn_full_composite")
    try:
        from engine.factor_backtest import get_score_date_range
        return jsonify(get_score_date_range(score_col))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/factor/run", methods=["POST"])
def factor_run():
    """Launch a factor backtest in background. Returns run_id immediately."""
    data = request.get_json(force=True) or {}
    run_id = "fac-" + str(uuid.uuid4())[:8]

    try:
        from engine.factor_backtest import FactorBacktestConfig, run_factor_backtest

        cfg = FactorBacktestConfig(
            score_column     = data.get("score_column",     "jcn_full_composite"),
            score_direction  = data.get("score_direction",  "higher_better"),
            n_buckets        = int(data.get("n_buckets",    5)),
            start_date       = data.get("start_date",       "2005-01-31"),
            end_date         = data.get("end_date",         "2024-12-31"),
            hold_months      = int(data.get("hold_months",  6)),
            min_price        = float(data.get("min_price",  5.0)),
            max_price        = float(data.get("max_price",  10000.0)),
            min_adv_usd      = float(data.get("min_adv_usd", 1_000_000)),
            cap_tier         = data.get("cap_tier",         "all"),
            exclude_sectors  = data.get("exclude_sectors",  []),
            rebalance_freq   = data.get("rebalance_freq",   "semi-annual"),
            transaction_cost_bps = float(data.get("cost_bps", 15.0)),
            run_label        = data.get("run_label",        ""),
        )
        if not cfg.run_label:
            cfg.run_label = f"{cfg.score_column} | {cfg.n_buckets}Q | {cfg.hold_months}mo | {cfg.cap_tier}"

        log_q: queue.Queue = queue.Queue(maxsize=200)
        with _factor_lock:
            _factor_runs[run_id] = {"status": "running", "result": None, "log_q": log_q, "cfg": data}

        def _cb(level, msg):
            try: log_q.put_nowait({"level": level, "msg": msg, "ts": time.time()})
            except: pass

        def _worker():
            try:
                result = run_factor_backtest(cfg, cb=_cb)
                with _factor_lock:
                    _factor_runs[run_id]["result"] = result
                    _factor_runs[run_id]["status"] = "complete" if result.get("status") != "error" else "error"

                # ── Auto-save to strategy bank ─────────────────────────────
                if result.get("status") == "complete":
                    try:
                        from engine.strategy_bank import save_factor_model
                        sid = save_factor_model(result, overwrite=True)
                        with _factor_lock:
                            _factor_runs[run_id]["strategy_id"] = sid
                        _cb("ok", f"Saved to bank: {sid}  |  elapsed: {result.get('elapsed_s')}s")
                    except Exception as save_err:
                        _cb("ok", f"Done {result.get('elapsed_s')}s (bank save failed: {save_err})")
                else:
                    _cb("ok", f"Done — {result.get('elapsed_s')}s")
            except Exception as e:
                import traceback; traceback.print_exc()
                _cb("error", str(e))
                with _factor_lock:
                    _factor_runs[run_id]["status"] = "error"
                    _factor_runs[run_id]["result"] = {"status": "error", "error": str(e)}

        threading.Thread(target=_worker, name=f"factor-{run_id}", daemon=True).start()
        return jsonify({"run_id": run_id, "status": "running"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/factor/status/<run_id>")
def factor_status(run_id):
    info = _factor_runs.get(run_id)
    if not info:
        return jsonify({"error": "unknown run"}), 404
    return jsonify({"run_id": run_id, "status": info["status"],
                    "has_result": info["result"] is not None})


@app.route("/api/factor/stream/<run_id>")
def factor_stream(run_id):
    """SSE log stream for a running factor backtest."""
    def gen():
        info = _factor_runs.get(run_id)
        if not info:
            yield "data: {\"error\":\"unknown run\"}\n\n"; return
        q = info["log_q"]
        yield f"data: {json.dumps({'level':'info','msg':'stream open'})}\n\n"
        while True:
            try:
                entry = q.get(timeout=15)
                yield f"data: {json.dumps(entry)}\n\n"
                if info.get("status") in ("complete","error") and q.empty():
                    break
            except queue.Empty:
                yield ": heartbeat\n\n"
                if info.get("status") in ("complete","error"):
                    break
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.route("/api/factor/result/<run_id>")
def factor_result(run_id):
    info = _factor_runs.get(run_id)
    if not info:
        return jsonify({"error": "unknown run"}), 404
    if info["status"] == "running":
        return jsonify({"status": "running"}), 202
    result = info["result"] or {}

    # Attach strategy_id if saved
    if info.get("strategy_id"):
        result = dict(result, strategy_id=info["strategy_id"])

    # Clean NaN/Inf before JSON serialization
    def _clean(obj):
        if isinstance(obj, dict): return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list): return [_clean(v) for v in obj]
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj): return None
            return obj
        return obj
    return jsonify(_clean(result))


@app.route("/api/factor/bank")
def factor_bank():
    """Return all saved factor models from the strategy bank."""
    try:
        from engine.strategy_bank import get_all_models, get_bank_summary
        models = get_all_models(limit=500)
        summary = get_bank_summary()
        # Clean NaN
        import math as _math
        def _c(v):
            if isinstance(v, float) and (_math.isnan(v) or _math.isinf(v)): return None
            return v
        models = [{k: _c(v) for k, v in m.items()} for m in models]
        return jsonify({"models": models, "summary": summary})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/factor/bank/<strategy_id>")
def factor_bank_model(strategy_id):
    """Return full model from bank by strategy_id."""
    try:
        from engine.strategy_bank import get_model
        m = get_model(strategy_id)
        if not m: return jsonify({"error": "not found"}), 404
        return jsonify(m)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/factor/bank/<strategy_id>/notes", methods=["POST"])
def factor_bank_notes(strategy_id):
    """Update notes/tags for a model."""
    data = request.get_json(force=True) or {}
    try:
        from engine.strategy_bank import update_model_notes
        ok = update_model_notes(strategy_id, data.get("notes",""), data.get("tags",""))
        return jsonify({"ok": ok})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/findings", methods=["GET","POST"])
def findings_api():
    import datetime as _dt
    import pathlib as _pl
    import json as _j
    bank_dir = _pl.Path(os.environ.get("OBQ_BANK_DIR", r"D:\OBQ_AI\OBQ_FactorLab_Bank"))
    bank_dir.mkdir(parents=True, exist_ok=True)
    findings_file = bank_dir / "findings.json"

    if request.method == "GET":
        try:
            data = _j.loads(findings_file.read_text(encoding="utf-8")) if findings_file.exists() else []
            return jsonify({"findings": data})
        except Exception as e:
            return jsonify({"findings": [], "error": str(e)})

    # POST — save new finding
    try:
        data = request.get_json(force=True) or {}
        existing = _j.loads(findings_file.read_text(encoding="utf-8")) if findings_file.exists() else []
        finding = {
            "id":          len(existing) + 1,
            "title":       data.get("title", ""),
            "body":        data.get("body", ""),
            "strategy_id": data.get("strategy_id"),
            "tag":         data.get("tag", "factor"),
            "actions":     data.get("actions", ""),
            "created_at":  data.get("created_at", _dt.datetime.now().isoformat()),
        }
        existing.insert(0, finding)
        findings_file.write_text(_j.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        return jsonify({"ok": True, "id": finding["id"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/findings/<int:finding_id>", methods=["DELETE"])
def delete_finding(finding_id):
    import pathlib as _pl
    import json as _j
    bank_dir = _pl.Path(os.environ.get("OBQ_BANK_DIR", r"D:\OBQ_AI\OBQ_FactorLab_Bank"))
    findings_file = bank_dir / "findings.json"
    try:
        data = _j.loads(findings_file.read_text(encoding="utf-8")) if findings_file.exists() else []
        data = [f for f in data if f.get("id") != finding_id]
        findings_file.write_text(_j.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5744, debug=False, use_reloader=False, threaded=True)
