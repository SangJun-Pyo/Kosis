# Kosis-main

KOSIS Open API 데이터를 내려받아 `jobs/*.json` 설정에 따라 엑셀 파일로 저장하는 프로젝트입니다.

필요한 패키지는 프로젝트 내부 `.deps` 폴더에 자동 설치됩니다.

## 빠른 시작

### 1. Python 확인

Python 3.13 이상이 설치되어 있어야 합니다.

```powershell
py -3.13 --version
```

또는

```powershell
python --version
```

### 2. 실행

- 전체 실행: `run_all.bat`
- 인구 파트만 실행: `run_population.bat`
- 경제/산업 파트만 실행: `run_economy_industry.bat`
- 고용/노동 파트만 실행: `run_employment_labor.bat`
- 관광/문화 파트만 실행: `run_tourism_culture.bat`

배치 파일을 실행하면 자동으로:

- Python 확인
- `.deps` 생성
- `requests`, `pandas`, `openpyxl` 설치
- KOSIS job 실행
- `output` 폴더에 엑셀 저장

## 결과 파일 위치

- 결과 파일은 `output/` 폴더 아래에 저장됩니다.
- 예: `output\인구\...xlsx`

## 주요 파일

- `run_jobs.bat`
  공통 실행기
- `run_all.bat`
  전체 job 실행
- `run_population.bat`
  인구 파트 실행
- `runner.py`
  API 호출, 전처리, 피벗, 엑셀 저장 담당
- `jobs/`
  job JSON 모음

## 폴더 구조

```text
jobs/
  population/
  economy_industry/
  employment_labor/
  tourism_culture/

output/
.deps/
runner.py
run_all.bat
run_jobs.bat
```

## 참고

더 자세한 설치/운영 가이드는 [SETUP_GUIDE.md](C:/Users/sangj/Kosis-main/Kosis-main/SETUP_GUIDE.md)를 보면 됩니다.
