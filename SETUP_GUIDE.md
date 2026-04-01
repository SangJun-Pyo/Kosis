# SETUP GUIDE

이 문서는 현재 코드(`run_jobs.bat`, `runner.py`) 기준으로 실행/운영 방법을 정리한 문서입니다.

## 1. 요구 사항

- Windows
- Python 3.13 이상 (`py -3.13` 또는 `python` 명령 가능)
- 인터넷 연결 (KOSIS / pip)

## 2. 설치 및 첫 실행

프로젝트 루트에서 아래 배치를 실행합니다.

- 전체: `run_jobs.bat`

`run_jobs.bat` 동작:
1. Python 확인
2. `.deps` 생성
3. `requirements.txt` 설치 (`pip install --target .deps -r requirements.txt`)
4. `PYTHONPATH=.deps;...` 설정
5. `runner.py` 실행

## 3. API 키 설정

### 우선순위
1. `secrets.local.bat`
2. 기존 환경변수
3. `run_jobs.bat` 내부 기본값

### 권장 방식

루트에 `secrets.local.bat` 파일 생성:
```bat
@echo off
set "KOSIS_API_KEY=여기에_키"
```

보안상 배치 파일 하드코딩보다는 `secrets.local.bat` 또는 시스템 환경변수 사용을 권장합니다.

## 4. Job 구조

현재 주요 provider:
- `kosis`
- `kosis_multi`
- `kosis_sources`

실무에서 가장 자주 쓰는 것은 `kosis_sources`입니다.

### kosis_sources 핵심 필드
- `sources`: 여러 API 소스를 이름으로 정의
- `views`: 시트 생성 규칙
- `include_source_raw`: `true`면 source raw 시트 저장, `false`면 생략
- `raw_sheets`: raw 시트명을 직접 제어할 때 사용

### views에서 자주 쓰는 kind
- `pivot`
- `sum_pivot`
- `metric_summary`
- `single_metric_share_summary`
- `age_gender_share_compare`
- `stack_blocks`

## 5. 출력 규칙

- 경로: `output\{output_subdir}\{output_prefix}_YYYYMMDD.xlsx`
- 같은 파일이 열려 있으면 자동으로 `_01`, `_02` suffix를 붙여 저장
- MultiIndex 컬럼은 병합 헤더(`merge_cells=True`)로 저장

## 6. 실행 중 로그

- 진행률: `[RUN i/n xx%]`
- 성공: `[OK ...]`
- 실패: `[ERROR ...]`
- 재시도: `[WARN] 요청 실패 ...` + `[INFO] N초 후 재시도`

요청 실패는 기본적으로 재시도 후 계속 진행합니다.

## 7. 자주 발생하는 문제

### 1) `No module named openpyxl` 등 모듈 에러
- `run_jobs.bat`가 아니라 `python runner.py`를 직접 실행했을 가능성
- 또는 pip 설치 실패

해결:
1. 배치 파일로 실행
2. `.deps` 삭제 후 재실행
3. 네트워크/프록시 점검

### 2) `ConnectionResetError(10054)`
- KOSIS 또는 네트워크에서 연결이 끊긴 경우
- 재실행하면 통과하는 경우가 많음

### 3) `Permission denied` (엑셀 저장)
- 같은 결과 파일이 열려 있는 상태
- 자동으로 `_01` 파일로 저장되지만, 원래 파일명으로 저장하려면 엑셀을 닫고 재실행

## 8. 파일 정리 가이드

삭제 가능:
- `.deps/` (재실행 시 재생성)
- `output/` (산출물 백업 후)
- `__pycache__/`

삭제 금지(핵심):
- `jobs/`
- `runner.py`
- `run_jobs.bat` 및 `run_*.bat`
- `requirements.txt`