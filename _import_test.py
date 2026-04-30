import sys, os
sys.path.insert(0, r"C:\Users\admin\Desktop\OBQ_FactorLab")
os.chdir(r"C:\Users\admin\Desktop\OBQ_FactorLab")

print("testing imports...")
try:
    print("  dotenv...", end=" ")
    from dotenv import load_dotenv; load_dotenv(); print("OK")
    print("  flask...", end=" ")
    from flask import Flask; print("OK")
    print("  engine.data...", end=" ")
    from engine.data import UniverseConfig; print("OK")
    print("  engine.backtest...", end=" ")
    from engine.backtest import run_backtest; print("OK")
    print("  engine.spy_backtest...", end=" ")
    from engine.spy_backtest import run_spy_backtest; print("OK")
    print("  gui.app...", end=" ")
    from gui.app import app; print("OK")
    print("\nAll imports OK!")
except Exception as e:
    import traceback; traceback.print_exc()
