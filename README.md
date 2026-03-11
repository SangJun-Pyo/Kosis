# Kosis

KOSIS / data.go.kr 통계 데이터를 내려받아 `jobs/*.json` 정의에 따라 엑셀 파일로 저장하는 프로젝트입니다.

## 요구사항

- Windows
- Python 3.13 이상
- 인터넷 연결
- `py` 또는 `python` 명령 사용 가능

## 빠른 시작

1. 저장소를 클론합니다.
2. `secrets.local.bat.example`을 참고해 `secrets.local.bat`를 만듭니다.
3. 원하는 배치 파일을 실행합니다.

## 실행 파일

- `run_all.bat`: 전체 job 실행
- `run_population.bat`: 인구 파트 실행
- `run_economy_industry.bat`: 경제/산업 파트 실행
- `run_employment_labor.bat`: 고용/노동 파트 실행
- `run_tourism_culture.bat`: 관광/문화 파트 실행

## 구조

```text
jobs/
  population/
  economy_industry/
  employment_labor/
  tourism_culture/
```

## 비밀키 설정

실제 API 키는 Git에 올리지 않습니다.

`secrets.local.bat` 예시:

```bat
@echo off
set "KOSIS_API_KEY=YOUR_KOSIS_API_KEY"
set "DATA_GO_KR_SERVICE_KEY=YOUR_DATA_GO_KR_SERVICE_KEY"
```

## 참고

더 자세한 실행/운영 설명은 [SETUP_GUIDE.md](SETUP_GUIDE.md)를 참고하세요.

