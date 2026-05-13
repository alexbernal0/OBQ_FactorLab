@echo off
:: OBQ Factor Lab — first-time venv setup
:: Run this once to create/rebuild the .venv.
:: Uses the system Python 3.12 to create an isolated environment.

setlocal

cd /d "%~dp0"

echo ============================================
echo  OBQ Factor Lab — venv setup
echo ============================================

:: Create venv if it doesn't exist
if not exist ".venv\Scripts\python.exe" (
    echo Creating .venv...
    python -m venv .venv
    if errorlevel 1 (
        echo ERROR: Failed to create venv. Make sure Python 3.12 is on PATH.
        pause
        exit /b 1
    )
    echo .venv created.
) else (
    echo .venv already exists, skipping creation.
)

echo.
echo Installing requirements...
.venv\Scripts\pip install -r requirements.txt

echo.
echo ============================================
echo  Setup complete. Run launch.bat to start.
echo ============================================
pause

endlocal
