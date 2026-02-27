@echo off
cd /d %~dp0
call .venv\Scripts\activate

set "KOSIS_API_KEY=M2M0ZjY5MmRkMTA0YjQ0MzBhM2MwNjgzMjY5N2RmNWY="

python runner.py
pause