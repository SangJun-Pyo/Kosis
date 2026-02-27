import os
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd

# -----------------------------
# 공통 설정
# -----------------------------
KOSIS_BASE_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
JOBS_DIR = Path("jobs")
OUTPUT_ROOT = Path("output")

# KOSIS 키는 기존 그대로 유지 (provider 없는 job은 kosis로 처리)
KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "").strip()


# -----------------------------
# 유틸
# -----------------------------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def deep_get(obj: Any, path: str) -> Any:
    """
    점(.) 경로로 딕셔너리/리스트 내부 접근
    예) "response.body.items.item"
    - dict: key로 접근
    - list: 숫자 인덱스 접근 가능 (예: "items.0.name")
    """
    cur = obj
    if not path:
        return cur
    for part in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            if part.isdigit():
                idx = int(part)
                cur = cur[idx] if 0 <= idx < len(cur) else None
            else:
                # 리스트에 part가 오면 의미 없으니 None 처리
                return None
        else:
            return None
    return cur


def normalize_to_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def sanitize_filename(name: str) -> str:
    # 윈도우 금지문자 치환
    for ch in '\\/:*?"<>|':
        name = name.replace(ch, "_")
    return name.strip()[:150]


# -----------------------------
# Pivot (기존 그대로 + 커스텀)
# -----------------------------
def make_default_pivot(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    기본 피벗: 지역(C1_NM) x 시점(PRD_DE)
    """
    if "DT" not in df.columns or "PRD_DE" not in df.columns:
        return None
    if "C1_NM" not in df.columns:
        return None

    d = df.copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d["PRD_DE"] = d["PRD_DE"].astype(str)

    pv = (
        d.pivot_table(
            index="C1_NM",
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
        )
        .sort_index()
    )

    # 202511 -> 2025.11 (월일 때만) / 연도(Y)면 2023 유지
    def fmt_prd(x: str) -> str:
        x = str(x)
        return f"{x[:4]}.{x[4:]}" if len(x) >= 6 else x

    pv.columns = [fmt_prd(c) for c in pv.columns]
    return pv.reset_index()


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

    d = df.copy()
    if val in d.columns:
        d[val] = pd.to_numeric(d[val], errors="coerce")
    if "PRD_DE" in d.columns:
        d["PRD_DE"] = d["PRD_DE"].astype(str)

    pv = d.pivot_table(index=idx, columns=cols, values=val, aggfunc="first")

    # 컬럼 평탄화
    if isinstance(pv.columns, pd.MultiIndex):
        pv.columns = ["_".join(map(str, c)).strip() for c in pv.columns.values]
    else:
        pv.columns = [str(c) for c in pv.columns]

    # (옵션) 월 포맷
    if pivot_cfg.get("flatten_columns_year", False):
        def fmt_prd2(x: str) -> str:
            x = str(x)
            return f"{x[:4]}.{x[4:]}" if len(x) >= 6 and x.isdigit() else x
        pv.columns = [fmt_prd2(c) for c in pv.columns]

    return pv.reset_index()


def build_table_view(df: pd.DataFrame, job: dict) -> Tuple[Optional[pd.DataFrame], str]:
    """
    job['pivot'] 있으면 custom, 없으면 default.
    반환: (pivot_df or None, sheet_name)
    """
    pivot_cfg = job.get("pivot")
    sheet_name = "TABLE_VIEW"
    if pivot_cfg and pivot_cfg.get("sheet_name"):
        sheet_name = pivot_cfg["sheet_name"]

    try:
        if pivot_cfg:
            return make_custom_pivot(df, pivot_cfg), sheet_name
        return make_default_pivot(df), sheet_name
    except Exception as e:
        print("⚠ TABLE_VIEW 생성 실패:", e)
        return None, sheet_name


# -----------------------------
# Provider: KOSIS
# -----------------------------
def run_kosis_job(job: dict) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], str]:
    if not KOSIS_API_KEY:
        raise RuntimeError("KOSIS_API_KEY 환경변수가 없습니다.")

    params = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
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

    r = requests.get(KOSIS_BASE_URL, params=params, timeout=60)
    r.raise_for_status()

    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"KOSIS API 오류(리스트 아님): {data}")

    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("KOSIS 데이터 0건 (파라미터 확인 필요)")

    pivot_df, sheet_name = build_table_view(df, job)
    return df, pivot_df, sheet_name


# -----------------------------
# Provider: data.go.kr (일반화)
# -----------------------------
def run_data_go_kr_job(job: dict) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], str]:
    """
    job 예시:
    {
      "provider":"data_go_kr",
      "base_url":"https://apis.data.go.kr/....",
      "params": {"serviceKey":"{{DATA_GO_KR_SERVICE_KEY}}", "type":"json", ...},
      "item_path":"response.body.items.item",   # 필수(권장)
      "output_prefix":"...",
      "pivot": {...}
    }
    """
    base_url = job.get("base_url")
    if not base_url:
        raise RuntimeError("data_go_kr job에는 base_url이 필요합니다.")

    params = job.get("params", {})
    if not isinstance(params, dict):
        raise RuntimeError("data_go_kr job의 params는 dict여야 합니다.")

    # serviceKey 치환 지원
    # - job JSON에 "{{DATA_GO_KR_SERVICE_KEY}}"로 넣어두면 환경변수 DATA_GO_KR_SERVICE_KEY에서 읽어줌
    svc_env = os.getenv("DATA_GO_KR_SERVICE_KEY", "").strip()
    for k, v in list(params.items()):
        if isinstance(v, str) and v.strip() == "{{DATA_GO_KR_SERVICE_KEY}}":
            if not svc_env:
                raise RuntimeError("환경변수 DATA_GO_KR_SERVICE_KEY가 없습니다.")
            params[k] = svc_env

    r = requests.get(base_url, params=params, timeout=60)
    r.raise_for_status()

    print("STATUS:", r.status_code)
    print("CONTENT-TYPE:", r.headers.get("Content-Type"))
    print("BODY_HEAD:", r.text[:500])

    # 응답 JSON 시도
    try:
        data = r.json()
    except Exception:
        # XML 등일 수 있음
        raise RuntimeError("data.go.kr 응답이 JSON이 아닙니다. (job params에 type/json 옵션 확인 필요)")

    item_path = job.get("item_path", "")
    if not item_path:
        # item_path 없으면 최상단이 리스트인지 확인
        if isinstance(data, list):
            items = data
        else:
            raise RuntimeError("data_go_kr job에는 item_path가 필요합니다. (예: response.body.items.item)")
    else:
        items = deep_get(data, item_path)

    items = normalize_to_list(items)
    if not items:
        raise RuntimeError(f"data.go.kr items 0건 (item_path={item_path})")

    # items가 dict가 아니라 문자열/숫자일 수도 있으니 안전 처리
    # dict list가 아니면 DataFrame이 이상해질 수 있음
    if isinstance(items[0], dict):
        df = pd.DataFrame(items)
    else:
        df = pd.DataFrame({"value": items})

    pivot_df, sheet_name = build_table_view(df, job)
    return df, pivot_df, sheet_name


# -----------------------------
# 저장
# -----------------------------
def save_excel(job: dict, raw_df: pd.DataFrame, pivot_df: Optional[pd.DataFrame], sheet_name: str) -> Path:
    today = datetime.today().strftime("%Y%m%d")

    subdir = job.get("output_subdir", "")
    out_dir = OUTPUT_ROOT / subdir if subdir else OUTPUT_ROOT
    ensure_dir(out_dir)

    prefix = job.get("output_prefix", "export")
    prefix = sanitize_filename(prefix)

    file_name = f"{prefix}_{today}.xlsx"
    out_path = out_dir / file_name

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        raw_df.to_excel(writer, sheet_name="RAW", index=False)
        if pivot_df is not None:
            # 엑셀 시트명 제한(31자)
            sheet = sheet_name[:31]
            pivot_df.to_excel(writer, sheet_name=sheet, index=False)

    return out_path


# -----------------------------
# 실행
# -----------------------------
def run_job(job_path: Path) -> None:
    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)

    job_name = job.get("job_name", job_path.stem)
    provider = job.get("provider", "kosis")  # ★ provider 없으면 kosis로 처리

    print(f"\n▶ 실행: {job_name}  (provider={provider})")

    if provider == "kosis":
        raw_df, pivot_df, sheet_name = run_kosis_job(job)
    elif provider == "data_go_kr":
        raw_df, pivot_df, sheet_name = run_data_go_kr_job(job)
    else:
        raise RuntimeError(f"Unknown provider: {provider}")

    out_path = save_excel(job, raw_df, pivot_df, sheet_name)
    print(f"✅ 저장 완료: {out_path}")


def main():
    jobs = sorted(JOBS_DIR.glob("*.json"))
    print(f"총 {len(jobs)}개 job 실행")

    for job_file in jobs:
        try:
            run_job(job_file)
        except Exception as e:
            print(f"❌ 실패: {job_file.name} -> {e}")

    print("\n모든 작업 완료")


if __name__ == "__main__":
    main()