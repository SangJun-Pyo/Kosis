# Setup Guide

## 1. 프로젝트 개요

이 프로젝트는 KOSIS Open API 데이터를 수집해서 `jobs/*.json` 설정에 맞는 엑셀 파일을 생성합니다.

현재 실행 구조는 다음과 같습니다.

- 프로젝트 내부 `.deps` 폴더에 패키지 설치
- 배치 파일로 파트별 또는 전체 실행

## 2. 실행 전 준비물

필수:

- Windows
- Python 3.13 이상
- 인터넷 연결
- KOSIS API 키

## 3. 꼭 알아야 하는 파일

- [run_jobs.bat](C:/Users/sangj/Kosis-main/Kosis-main/run_jobs.bat)
  공통 실행기. 실제 설치/실행 로직이 모두 들어 있습니다.

- [run_all.bat](C:/Users/sangj/Kosis-main/Kosis-main/run_all.bat)
  전체 파트를 한 번에 실행합니다.

- [run_population.bat](C:/Users/sangj/Kosis-main/Kosis-main/run_population.bat)
  인구 파트만 실행합니다.

- [runner.py](C:/Users/sangj/Kosis-main/Kosis-main/runner.py)
  API 호출, 전처리, 피벗, 엑셀 저장을 담당합니다.

- [requirements.txt](C:/Users/sangj/Kosis-main/Kosis-main/requirements.txt)
  필요한 파이썬 패키지 목록입니다.

- [secrets.local.bat.example](C:/Users/sangj/Kosis-main/Kosis-main/secrets.local.bat.example)
  API 키 설정 예시 파일입니다.

- [jobs](C:/Users/sangj/Kosis-main/Kosis-main/jobs)
  각 작업 JSON이 들어 있는 폴더입니다.

## 4. 처음 설치하는 방법

### 4-1. Python 설치

Python 3.13 이상을 설치합니다.

설치 후 PowerShell 또는 cmd에서 아래 둘 중 하나가 동작하면 됩니다.

```powershell
py -3.13 --version
python --version
```

### 4-2. API 키 파일 만들기

[secrets.local.bat.example](C:/Users/sangj/Kosis-main/Kosis-main/secrets.local.bat.example)을 복사해서  
`secrets.local.bat` 파일을 만듭니다.

예시:

```bat
@echo off
set "KOSIS_API_KEY=여기에_KOSIS_API_KEY"
```

## 5. 실행 방법

### 전체 실행

```bat
run_all.bat
```

### 인구 파트만 실행

```bat
run_population.bat
```

### 직접 특정 경로 실행

```powershell
python runner.py jobs/population
python runner.py jobs/economy_industry
python runner.py jobs/employment_labor
python runner.py jobs/tourism_culture
python runner.py jobs/population/1-20_elderly_single_leisure_facility_compare.json
```

## 6. 실행 시 내부적으로 일어나는 일

[run_jobs.bat](C:/Users/sangj/Kosis-main/Kosis-main/run_jobs.bat)은 아래 순서로 동작합니다.

1. Python 실행 파일 찾기
2. `.deps` 폴더 생성
3. `secrets.local.bat` 로드
4. `requests`, `pandas`, `openpyxl` import 확인
5. 없으면 `requirements.txt` 기준으로 `.deps`에 설치
6. `runner.py` 실행
7. 결과 엑셀 저장

## 7. 자동 생성되는 폴더

- `.deps/`
  파이썬 패키지가 설치되는 폴더입니다.

- `output/`
  결과 엑셀 파일이 저장되는 폴더입니다.

- `__pycache__/`
  파이썬이 자동으로 만드는 캐시 폴더입니다.

## 8. job 파일 추가 방법

새 통계표를 추가하려면 보통 다음 순서로 작업합니다.

1. KOSIS API URL 확인
2. 어떤 시트를 만들지 결정
3. `jobs/<파트>/` 아래 새 JSON 추가
4. 배치 또는 `runner.py`로 단독 실행
5. `RAW` 시트와 summary 시트 확인

기본 예시는 아래 형태입니다.

```json
{
  "job_name": "예시_job",
  "provider": "kosis_sources",
  "sources": [
    {
      "name": "source1",
      "orgId": "101",
      "tblId": "TABLE_ID",
      "prdSe": "Y",
      "newEstPrdCnt": 5,
      "format": "json",
      "jsonVD": "Y",
      "itmId": "T01",
      "objL1": "ALL"
    }
  ],
  "output_subdir": "인구",
  "output_prefix": "예시_출력명",
  "views": [
    {
      "kind": "pivot",
      "source": "source1",
      "sheet_name": "TABLE_VIEW",
      "index": ["C1_NM"],
      "columns": ["PRD_DE"],
      "values": "DT"
    }
  ]
}
```

## 9. 자주 나는 오류와 해결 방법

### `KOSIS_API_KEY 환경변수가 없습니다`

원인:

- `secrets.local.bat`가 없거나
- 파일 안에 `KOSIS_API_KEY`가 설정되지 않음

해결:

- `secrets.local.bat.example`을 복사해서 `secrets.local.bat` 생성
- API 키 입력 후 다시 실행

### `No module named 'openpyxl'`

원인:

- `.deps` 설치가 덜 되었거나
- 설치가 실패했음

해결:

```powershell
python -m pip install --target .deps -r requirements.txt
```

또는 `run_jobs.bat`를 다시 실행합니다.

### `ConnectionResetError(10054)` 또는 원격 호스트 끊김

원인:

- KOSIS 서버 일시 오류
- 사내망/방화벽/백신/프록시 간섭

해결:

- 잠시 후 다시 실행
- 네트워크 정책 확인
- 실패 job만 다시 실행

### 저장하려는 엑셀 파일이 열려 있음

현재 러너는 이런 경우 경고를 보여주고, 필요하면 다른 이름으로 저장합니다.

### 삭제하지 않는 것이 좋은 것

- `jobs/`
- `runner.py`
- `run_*.bat`
- `requirements.txt`
- `README.md`
- `SETUP_GUIDE.md`
- `secrets.local.bat`
- `.git/`

### 특히 주의

- `secrets.local.bat`
  삭제하면 API 키 설정이 사라집니다.