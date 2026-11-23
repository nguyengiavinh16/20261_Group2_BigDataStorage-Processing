@echo off
REM Double-click hoac chay: setup_env.bat [-DownloadData] [-RunNotebook]
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_env.ps1" %*
pause
