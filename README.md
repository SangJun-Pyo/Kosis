# Kosis-main

KOSIS / data.go.kr OpenAPI 데이터를 `jobs/*.json` 설정으로 수집하고, 엑셀(`.xlsx`)로 내보내는 프로젝트입니다.

## 빠른 시작

### 실행 (권장)
- 전체 실행: `run_jobs.bat`

처음 실행 시 `run_jobs.bat`가:
- `.deps` 폴더 생성
- `requirements.txt` 패키지 설치
- `runner.py` 실행
을 자동으로 처리합니다.

## 프로젝트 구조

- `runner.py`: 수집/가공/엑셀 저장 메인 실행기
- `run_jobs.bat`: 공통 실행 배치
- `jobs/`: job JSON
- `output/`: 결과 엑셀 출력
- `requirements.txt`: 필수 패키지

## API 키

`run_jobs.bat`는 아래 순서로 API 키를 결정합니다.
1. `secrets.local.bat` 값
2. 이미 설정된 시스템 환경변수
3. 배치 내부 기본값

## 실행 결과

결과 파일은 `output\{output_subdir}\{output_prefix}_YYYYMMDD.xlsx` 형태로 저장됩니다.

같은 파일이 열려 있으면 자동으로 `_01`, `_02` suffix를 붙여 저장합니다.

## 주의

- `python runner.py ...` 직접 실행 시 패키지 자동 설치가 동작하지 않을 수 있습니다. 배치 실행을 권장합니다.
- 네트워크/방화벽 상태에 따라 KOSIS 호출이 일시 실패할 수 있으며, 기본 재시도 로직이 적용됩니다.
