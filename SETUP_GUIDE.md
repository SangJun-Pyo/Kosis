# Kosis-main Setup Guide

## 목적
`run_all.bat` 한 번으로 필요한 Python 환경을 준비하고, 선택한 `jobs` 범위를 실행해 `output/` 아래에 엑셀 파일을 생성합니다.
이 프로젝트는 이제 `.venv` 없이도 실행되며, 필요한 패키지는 프로젝트 내부 `.deps/`에 자동 설치됩니다.

## 필요한 파일
- `run_jobs.bat`: 공통 실행기. 설치 확인 후 지정한 jobs 경로만 실행
- `run_all.bat`: 전체 jobs 실행
- `run_population.bat`: 인구 파트만 실행
- `run_economy_industry.bat`: 경제/산업 파트만 실행
- `run_employment_labor.bat`: 고용/노동 파트만 실행
- `run_tourism_culture.bat`: 관광/문화 파트만 실행
- `runner.py`: KOSIS / data.go.kr API 호출과 피벗, 엑셀 저장 로직
- `requirements.txt`: 필수 Python 패키지 목록
- `jobs/`: 파트별 job 정의 폴더
- `.deps/`: 자동 생성되는 로컬 Python 패키지 폴더
- `secrets.local.bat`: 로컬 API 키 설정 파일 (`Git 제외`)

## 실행 전 준비
1. Windows에 Python 3.13 이상이 설치되어 있어야 합니다.
2. `py` 또는 `python` 명령이 동작해야 합니다.
3. 인터넷 연결이 가능해야 합니다.
4. `secrets.local.bat`에 API 키가 설정되어 있어야 합니다.

## 첫 실행 방법
1. 프로젝트 폴더에서 원하는 배치 파일을 실행합니다.
2. 배치 파일이 자동으로 아래 작업을 수행합니다.
   - 사용 가능한 Python 실행 파일 탐색
   - `.deps/` 폴더 생성
   - `secrets.local.bat` 로드
   - `requests`, `pandas`, `openpyxl` 설치
   - `PYTHONPATH`에 `.deps/` 추가
   - 지정한 job 폴더 또는 파일만 실행

## 로컬 키 파일
프로젝트 루트에 `secrets.local.bat`를 만들고 아래처럼 사용합니다.

```bat
@echo off
set "KOSIS_API_KEY=YOUR_KOSIS_API_KEY"
set "DATA_GO_KR_SERVICE_KEY=YOUR_DATA_GO_KR_SERVICE_KEY"
```

## 파트별 실행 파일
- `run_all.bat`: 전체 실행
- `run_population.bat`: `jobs/population`
- `run_economy_industry.bat`: `jobs/economy_industry`
- `run_employment_labor.bat`: `jobs/employment_labor`
- `run_tourism_culture.bat`: `jobs/tourism_culture`

## 현재 권장 폴더 구조
```text
jobs/
  population/
  economy_industry/
  employment_labor/
  tourism_culture/
```

현재 분류 기준:
- `population/`: 기존 `1-*` job
- `economy_industry/`: 기존 `2-*` job
- `employment_labor/`: 기존 `4-*` job
- `tourism_culture/`: 추후 추가 예정

## 자동 설치되는 패키지
- `requests`: API 호출
- `pandas`: 데이터 가공과 피벗
- `openpyxl`: 엑셀 파일 저장

## 생성되는 폴더
- `.deps/`: 프로젝트 로컬 Python 패키지
- `output/`: 실행 결과 엑셀 파일

## 새 Job 추가 방법
1. 해당 파트 폴더 아래에 JSON 파일을 추가합니다.
2. 기본 KOSIS 작업은 아래 형태를 따릅니다.

```json
{
  "job_name": "예시_작업명",
  "provider": "kosis",
  "orgId": "101",
  "tblId": "TABLE_ID",
  "prdSe": "Y",
  "newEstPrdCnt": 5,
  "itmId": "T1",
  "objL1": "ALL",
  "output_subdir": "인구",
  "output_prefix": "예시_출력명",
  "pivot": {
    "index": ["C1_NM"],
    "columns": ["PRD_DE"],
    "values": "DT",
    "sheet_name": "TABLE_VIEW"
  }
}
```

## 문제 해결
### `No module named 'openpyxl'`
배치 파일이 다시 실행되면 자동 설치됩니다. 수동 설치가 필요하면 아래를 실행합니다.

```powershell
python -m pip install --target .deps -r requirements.txt
```

### 다른 PC에서 `.venv` 경로 문제로 실행이 안 됨
현재 배치 파일은 `.venv`를 사용하지 않으므로 해당 문제는 피할 수 있습니다.

### `Python is not installed or not available on PATH`
PC에 Python이 없거나 PATH에 등록되지 않은 상태입니다. Python 3.13 이상을 설치한 뒤 다시 실행해야 합니다.

### `KOSIS_API_KEY is not set`
`secrets.local.bat`가 있는지, 그리고 `KOSIS_API_KEY` 값이 들어있는지 확인합니다.

### `Unable to connect to the remote server`
사내망, 방화벽, 백신, 프록시 설정 때문에 KOSIS 접속이 차단될 수 있습니다.

### 엑셀은 저장되는데 내용이 기대와 다름
해당 `jobs/*.json`의 `pivot.index`, `pivot.columns`, `itmId`, `objL1~objL8` 값을 확인해야 합니다.

### 특정 파트만 실행하고 싶음
해당 파트용 배치 파일을 실행하면 됩니다. 또는 직접 아래처럼 실행할 수 있습니다.

```powershell
python runner.py jobs/population
python runner.py jobs/economy_industry
python runner.py jobs/employment_labor
python runner.py jobs/tourism_culture
```

## 권장 운영 방식
- 패키지는 항상 프로젝트의 `.deps/`에 설치
- 프로젝트는 하나만 유지하고, 파트는 `jobs` 하위 폴더와 배치 파일로 분리
- 새 통계표를 붙일 때는 먼저 KOSIS API 응답의 컬럼 구조를 확인
- `RAW` 시트와 `TABLE_VIEW` 시트를 함께 확인해 피벗 기준이 맞는지 검증
