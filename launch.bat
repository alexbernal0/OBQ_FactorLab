@echo off
:: OBQ Factor Lab — launcher using project-local venv
:: Double-click this file (or run from terminal) to start the app.
:: Uses .venv inside this folder — completely isolated from other OBQ Python apps.

setlocal

cd /d "%~dp0"

:: Use the project venv
set PYTHON="%~dp0.venv\Scripts\python.exe"

:: Sanity check
if not exist %PYTHON% (
    echo ERROR: .venv not found. Run setup_venv.bat first.
    pause
    exit /b 1
)

echo Starting OBQ Factor Lab...
echo Python: %PYTHON%
echo Port:   5744

%PYTHON% main.py

endlocal
