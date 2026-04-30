"""OBQ FactorLab — launcher."""
import sys
import time
import threading
import multiprocessing
from pathlib import Path

# Windows multiprocessing guard (same pattern as Options Scanner)
multiprocessing.freeze_support()

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gui.app import app
import webview

PORT = 5744
HOST = "127.0.0.1"


def _start_flask():
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)

    t = threading.Thread(target=_start_flask, name="flask", daemon=True)
    t.start()
    time.sleep(1.5)  # let Flask bind

    window = webview.create_window(
        "OBQ Factor Lab",
        f"http://{HOST}:{PORT}/",
        width=1600, height=960,
        min_size=(1100, 700),
        background_color="#ffffff",
        resizable=True,
        confirm_close=False,
    )

    # Make window accessible to Flask's /api/evaljs and /api/snap endpoints
    import __main__ as _mm
    _mm._webview_window = window

    webview.start()
