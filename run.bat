@echo off
title PMV Flasher

:: Check for Python
python --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON=python
    goto :found
)

python3 --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON=python3
    goto :found
)

:: Python not found — offer to install
echo.
echo  Python is not installed or not in PATH.
echo.
choice /C YN /M "Would you like to install Python automatically using winget"
if %errorlevel% equ 1 (
    echo.
    echo Installing Python via winget...
    winget install Python.Python.3.12 --accept-package-agreements --accept-source-agreements
    if %errorlevel% neq 0 (
        echo.
        echo  winget install failed. Opening the Python download page instead...
        start https://www.python.org/downloads/
        echo.
        echo  Please install Python, then run this script again.
        pause
        exit /b 1
    )
    :: Refresh PATH so the new python is found
    set "PATH=%LOCALAPPDATA%\Programs\Python\Python312;%LOCALAPPDATA%\Programs\Python\Python312\Scripts;%PATH%"
    set PYTHON=python
    goto :found
) else (
    echo.
    echo  Opening the Python download page...
    start https://www.python.org/downloads/
    echo.
    echo  Please install Python, then run this script again.
    pause
    exit /b 1
)

:found
echo Using: %PYTHON%
%PYTHON% --version

:: Install dependencies
echo.
echo Checking dependencies...
%PYTHON% -m pip install --quiet --upgrade pip >nul 2>&1
%PYTHON% -m pip install --quiet -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo  Failed to install dependencies. Please run manually:
    echo    %PYTHON% -m pip install -r requirements.txt
    pause
    exit /b 1
)

:: Launch the GUI
echo.
echo Starting PMV Flasher...
%PYTHON% pmv_gui.py
if %errorlevel% neq 0 (
    echo.
    echo  PMV Flasher exited with an error.
    pause
)
