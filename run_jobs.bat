@echo off
cd /d %~dp0
set "PYTHONUTF8=1"

set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=jobs"

set "PY_CMD="
where py >nul 2>&1
if not errorlevel 1 (
  set "PY_CMD=py -3.13"
) else (
  where python >nul 2>&1
  if not errorlevel 1 (
    set "PY_CMD=python"
  )
)

if "%PY_CMD%"=="" (
  echo [ERROR] Python is not installed or not available on PATH.
  echo [ERROR] Install Python 3.13 or newer, then run this file again.
  pause
  exit /b 1
)

set "LOCAL_DEPS=%CD%\.deps"
if not exist "%LOCAL_DEPS%" (
  mkdir "%LOCAL_DEPS%"
)

set "PYTHONPATH=%LOCAL_DEPS%;%PYTHONPATH%"

if exist "secrets.local.bat" (
  call secrets.local.bat
)

call %PY_CMD% -c "import requests, pandas, openpyxl" >nul 2>&1
if errorlevel 1 (
  echo [INFO] Installing required Python packages...
  call %PY_CMD% -m pip install --disable-pip-version-check --target "%LOCAL_DEPS%" -r requirements.txt
  if errorlevel 1 (
    echo [ERROR] Failed to install required Python packages.
    echo [ERROR] Check your internet, firewall, or proxy settings and run again.
    pause
    exit /b 1
  )
  call %PY_CMD% -c "import requests, pandas, openpyxl" >nul 2>&1
  if errorlevel 1 (
    echo [ERROR] Package installation finished but imports still fail.
    pause
    exit /b 1
  )
)

if "%KOSIS_API_KEY%"=="" (
  echo [ERROR] KOSIS_API_KEY is not set.
  echo [ERROR] Create secrets.local.bat from secrets.local.bat.example and set your keys.
  pause
  exit /b 1
)

call %PY_CMD% runner.py "%TARGET%"
pause
