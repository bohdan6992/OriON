@echo off
rem -------------------------------------------------
rem start_orion_daily.bat (place inside OriON folder)
rem Uses:
rem   - OriON\ops\config.json  (repos, branches, layout)
rem   - OriON\ops\access_token.json (GitHub PAT)  [read by python code]
rem -------------------------------------------------

setlocal EnableExtensions EnableDelayedExpansion

rem ORION_HOME = folder where this .bat lives
set "ORION_HOME=%~dp0"
if "%ORION_HOME:~-1%"=="\" set "ORION_HOME=%ORION_HOME:~0,-1%"

rem repo root is one level above OriON
set "ORION_ROOT=%ORION_HOME%\.."

rem --- Python executable (EDIT THIS) ---
rem Keep your current Python version here (no upgrades needed).
set "PYTHON_EXE=C:\Python39\python.exe"

rem Optional: add Git to PATH if git not found
rem set "PATH=C:\Program Files\Git\bin;%PATH%"

rem --- Logging ---
set "LOG_DIR=%ORION_HOME%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem Make timestamp safe (handles spaces in %TIME%)
set "TS_DATE=%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%"
set "TS_TIME=%TIME:~0,2%-%TIME:~3,2%-%TIME:~6,2%"
set "TS_TIME=%TS_TIME: =0%"
set "LOG_FILE=%LOG_DIR%\run_orion_%TS_DATE%_%TS_TIME%.log"

echo Starting OriON runner at %DATE% %TIME% > "%LOG_FILE%"
echo ORION_HOME=%ORION_HOME% >> "%LOG_FILE%"
echo ORION_ROOT=%ORION_ROOT% >> "%LOG_FILE%"
echo PYTHON_EXE=%PYTHON_EXE% >> "%LOG_FILE%"

rem Ensure we run from OriON folder (important for relative paths)
pushd "%ORION_HOME%"

rem Export ORION_HOME for the runner (it will also self-detect, but this is explicit)
set "ORION_HOME=%ORION_HOME%"

rem --- Run ---
"%PYTHON_EXE%" "%ORION_HOME%\run_orion_daily.py" >> "%LOG_FILE%" 2>&1

set "RC=%ERRORLEVEL%"
echo Runner exit code: %RC% >> "%LOG_FILE%"
echo Finished at %DATE% %TIME% >> "%LOG_FILE%"

popd
exit /b %RC%
