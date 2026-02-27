@echo off
cd /d %~dp0
call .venv\Scripts\activate

set "KOSIS_API_KEY=M2M0ZjY5MmRkMTA0YjQ0MzBhM2MwNjgzMjY5N2RmNWY="
set "DATA_GO_KR_SERVICE_KEY=c5c716565082b067fc48f2983e876159b967f25e3b14c189f23cf2e6b7e1fb33"

python runner.py
pause