import os
import json
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd

BASE_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
JOBS_DIR = Path("jobs")
OUTPUT_ROOT = Path("output")

api_key = os.getenv("KOSIS_API_KEY", "").strip()
if not api_key:
    raise RuntimeError("환경변수 KOSIS_API_KEY가 설정되어 있지 않습니다.")


def make_default_pivot(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    기본 피벗: 지역(C1_NM) x 시점(PRD_DE)
    """
    if "DT" not in df.columns or "PRD_DE" not in df.columns:
        return None
    if "C1_NM" not in df.columns:
        return None

    df = df.copy()
    df["DT"] = pd.to_numeric(df["DT"], errors="coerce")
    df["PRD_DE"] = df["PRD_DE"].astype(str)

    pivot = (
        df.pivot_table(
            index="C1_NM",
            columns="PRD_DE",
            values="DT",
            aggfunc="first"
        )
        .sort_index()
    )

    # 202511 -> 2025.11 (월일 때만) / 연도(Y)면 그냥 2023 형태로 유지됨
    def fmt_prd(x: str) -> str:
        x = str(x)
        return f"{x[:4]}.{x[4:]}" if len(x) >= 6 else x

    pivot.columns = [fmt_prd(c) for c in pivot.columns]
    return pivot.reset_index()


def make_custom_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    """
    job JSON의 pivot 설정 기반으로 피벗 생성
    pivot_cfg 예시:
    {
      "index": ["C1_NM","C2_NM","C3_NM"],
      "columns": ["PRD_DE"],
      "values": "DT",
      "sheet_name": "TABLE_VIEW",
      "flatten_columns_year": true
    }
    """
    idx = pivot_cfg.get("index", [])
    cols = pivot_cfg.get("columns", [])
    val = pivot_cfg.get("values", "DT")

    if not isinstance(idx, list) or not isinstance(cols, list):
        raise RuntimeError("pivot.index / pivot.columns 는 반드시 리스트여야 합니다.")

    need = set(idx + cols + [val])
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"Pivot columns missing: {missing}")

    df = df.copy()
    if val in df.columns:
        df[val] = pd.to_numeric(df[val], errors="coerce")
    if "PRD_DE" in df.columns:
        df["PRD_DE"] = df["PRD_DE"].astype(str)

    pv = df.pivot_table(
        index=idx,
        columns=cols,
        values=val,
        aggfunc="first"
    )

    # 컬럼 평탄화
    if isinstance(pv.columns, pd.MultiIndex):
        pv.columns = ["_".join(map(str, c)).strip() for c in pv.columns.values]
    else:
        pv.columns = [str(c) for c in pv.columns]

    # (옵션) 연/월 보기 좋게: PRD_DE만 쓰는 케이스에 한해 포맷
    # flatten_columns_year=True면 연도는 그대로 두고,
    # 월이면 202511 -> 2025.11로 바꿈
    if pivot_cfg.get("flatten_columns_year", False):
        def fmt_prd2(x: str) -> str:
            x = str(x)
            return f"{x[:4]}.{x[4:]}" if len(x) >= 6 and x.isdigit() else x
        pv.columns = [fmt_prd2(c) for c in pv.columns]

    pv = pv.reset_index()
    return pv


def run_job(job_path: Path):
    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)

    job_name = job.get("job_name", job_path.stem)
    print(f"\n▶ 실행: {job_name}")

    # ---- params 구성 ----
    params = {
        "method": "getList",
        "apiKey": api_key,
        "orgId": str(job["orgId"]).strip(),
        "tblId": str(job["tblId"]).strip(),
        "prdSe": job.get("prdSe", "M"),
        "format": job.get("format", "json"),
        "jsonVD": job.get("jsonVD", "Y"),
    }

    if job.get("newEstPrdCnt") is not None and str(job.get("newEstPrdCnt")).strip() != "":
        params["newEstPrdCnt"] = str(job["newEstPrdCnt"]).strip()

    if job.get("itmId"):
        params["itmId"] = str(job["itmId"]).strip()

    for i in range(1, 9):
        key = f"objL{i}"
        if job.get(key):
            params[key] = str(job[key]).strip()

    # ---- API 호출 ----
    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()

    data = r.json()
    if not isinstance(data, list):
        print("❌ API 오류(리스트 아님):", data)
        return

    df = pd.DataFrame(data)
    if df.empty:
        print("⚠ 데이터 0건 (파라미터 확인 필요)")
        return

    # ---- TABLE_VIEW 만들기 (custom pivot 우선) ----
    pivot_cfg = job.get("pivot")
    pivot_df = None

    try:
        if pivot_cfg:
            pivot_df = make_custom_pivot(df, pivot_cfg)
        else:
            pivot_df = make_default_pivot(df)
    except Exception as e:
        print("⚠ TABLE_VIEW 생성 실패:", e)
        pivot_df = None

    # ---- 저장 경로/파일명 ----
    today = datetime.today().strftime("%Y%m%d")

    subdir = job.get("output_subdir", "")
    out_dir = OUTPUT_ROOT / subdir if subdir else OUTPUT_ROOT
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix = job.get("output_prefix", "kosis")
    file_name = f"{prefix}_{today}.xlsx"
    out_path = out_dir / file_name

    sheet_name = "TABLE_VIEW"
    if pivot_cfg and pivot_cfg.get("sheet_name"):
        sheet_name = pivot_cfg["sheet_name"]

    # ---- 엑셀 저장 ----
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="RAW", index=False)
        if pivot_df is not None:
            pivot_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ 저장 완료: {out_path}")


def main():
    jobs = sorted(JOBS_DIR.glob("*.json"))
    print(f"총 {len(jobs)}개 job 실행")

    for job_file in jobs:
        run_job(job_file)

    print("\n모든 작업 완료")


if __name__ == "__main__":
    main()