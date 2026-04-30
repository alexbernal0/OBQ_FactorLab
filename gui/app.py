"""Flask app for OBQ_FactorLab."""
import os, sys, json, threading, queue, time, uuid, math
from pathlib import Path
from flask import Flask, render_template, jsonify, request, Response, stream_with_context

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
        # Access the global webview window reference
        import __main__ as _main
        win = getattr(_main, "_webview_window", None)
        if win is None:
            return jsonify({"error": "no webview window ref"}), 503
        result = win.evaluate_js(js)
        return jsonify({"result": result})
    except Exception as e:
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


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5744, debug=False, use_reloader=False, threaded=True)
