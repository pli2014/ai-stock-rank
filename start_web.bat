@echo off
cd /d "%~dp0"
set PYTHONPATH=%~dp0\src;%PYTHONPATH%
python -m src.main
pause