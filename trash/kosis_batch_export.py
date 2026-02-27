import os
from datetime import datetime
import requests
import pandas as pd

BASE_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
CONFIG_PATH = "config.xlsx"
OUTPUT_ROOT = "output"


def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def build_params(row, api_key):
    """
    config.xlsx 한 행(row)에서 KOSIS 요청 params 생성
    """
    params = {
        "method": "getList",
        "apiKey": api_key,
        "orgId": safe_str(row.get("orgId")),
        "tblId": safe_str(row.get("tblId")),
        "prdSe": safe_str(row.get("prdSe")) or "M",
        "format": safe_str(row.get("format")) or "json",
        "jsonVD": safe_str(row.get("jsonVD")) or "Y",
    }

    # itmId / objL1..objL8
    for k in ["itmId", "objL1", "objL2", "objL3", "objL4", "objL5", "objL6", "objL7", "objL8"]:
        v = safe_str(row.get(k))
        if v != "":
            params[k] = v

    # 기간 지정: newEstPrdCnt 우선, 없으면 start/end 사용
    new_cnt = safe_str(row.get("newEstPrdCnt"))
    start_prd = safe_str(row.get("startPrdDe"))
    end_prd = safe_str(row.get("endPrdDe"))

    if new_cnt != "":
        params["newEstPrdCnt"] = new_cnt
    else:
        # start/end를 쓸 경우(선택) - KOSIS API가 지원하는 경우에만 사용
        if start_prd != "":
            params["startPrdDe"] = start_prd
        if end_prd != "":
            params["endPrdDe"] = end_prd

    return params


def make_output_path(row):
    """
    output_subdir / output_file_prefix 기반으로 저장 경로 생성
    """
    today_str = datetime.today().strftime("%Y%m%d")

    subdir = safe_str(row.get("output_subdir"))
    out_dir = os.path.join(OUTPUT_ROOT, subdir) if subdir else OUTPUT_ROOT
    os.makedirs(out_dir, exist_ok=True)

    prefix = safe_str(row.get("output_file_prefix")) or f"kosis_export_{today_str}"
    file_name = f"{prefix}.xlsx"
    return os.path.join(out_dir, file_name)


def pivot_table_view(df):
    """
    홈페이지처럼 보이게: (지역명) x (시점) 형태로 피벗
    기본은 C1_NM(지역명)이 있으면 지역명 기준.
    """
    # 값/시점 정리
    if "DT" in df.columns:
        df["DT"] = pd.to_numeric(df["DT"], errors="coerce")
    if "PRD_DE" in df.columns:
        df["PRD_DE"] = df["PRD_DE"].astype(str)

    index_col = "C1_NM" if "C1_NM" in df.columns else ("C1" if "C1" in df.columns else None)
    if index_col is None:
        return None, None  # 피벗 불가

    # 기본: 시점(PRD_DE)을 열로 펼치기
    pivot = (
        df.pivot_table(
            index=index_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
        )
        .sort_index()
    )

    # 열 이름 2025.11 형태로
    def fmt_prd(prd):
        prd = str(prd)
        return f"{prd[:4]}.{prd[4:]}" if len(prd) >= 6 else prd

    pivot.columns = [fmt_prd(c) for c in pivot.columns]
    pivot = pivot.reset_index()

    # 월별 변화량
    pivot_change = pivot.set_index(index_col).diff(axis=1).reset_index()

    return pivot, pivot_change

def main():
    # 1) API KEY
    api_key = os.getenv("KOSIS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("환경변수 KOSIS_API_KEY가 비어있습니다. 또는 코드에서 apiKey를 직접 넣어주세요.")

    # 2) config.xlsx 읽기
    cfg = pd.read_excel(CONFIG_PATH, sheet_name="config")

    # enabled=Y만
    cfg["enabled"] = cfg["enabled"].astype(str).str.upper().str.strip()
    cfg_run = cfg[cfg["enabled"] == "Y"].copy()

    if cfg_run.empty:
        print("enabled=Y 인 행이 없습니다. config.xlsx를 확인하세요.")
        return

    print(f"총 {len(cfg_run)}개 작업을 실행합니다.")

    # 3) 행별 실행
    for idx, row in cfg_run.iterrows():
        seq = safe_str(row.get("seq")) or str(idx + 1)
        title = safe_str(row.get("지표명(소분류)")) or safe_str(row.get("output_file_prefix")) or f"job_{seq}"

        try:
            params = build_params(row, api_key)
            out_path = make_output_path(row)

            print(f"\n[{seq}] {title}")
            print(" - request:", params.get("orgId"), params.get("tblId"), params.get("prdSe"))

            r = requests.get(BASE_URL, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            df = pd.DataFrame(data)

            if df.empty:
                print(" - WARNING: 응답이 비어있습니다. (필터/파라미터 확인)")
                continue

            pivot, pivot_change = pivot_table_view(df)

            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="RAW", index=False)
                if pivot is not None:
                    pivot.to_excel(writer, sheet_name="TABLE_VIEW", index=False)
                if pivot_change is not None:
                    pivot_change.to_excel(writer, sheet_name="CHANGE", index=False)

            print(f" - saved: {out_path}")

        except Exception as e:
            print(f" - ERROR: {e}")

    print("\n완료.")


if __name__ == "__main__":
    main()