import os
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd

# -----------------------------
KOSIS_BASE_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
JOBS_DIR = Path("jobs")
OUTPUT_ROOT = Path("output")

KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "").strip()

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def deep_get(obj: Any, path: str) -> Any:
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
    for ch in '\\/:*?"<>|':
        name = name.replace(ch, "_")
    return name.strip()[:150]


def map_age_to_10y_bucket(label: Any) -> Optional[str]:
    s = str(label).strip()
    if not s:
        return None
    if s in ("계", "총계", "합계"):
        return "계"

    nums = [int(x) for x in re.findall(r"\d+", s)]
    if "이하" in s:
        if nums and nums[0] <= 9:
            return "9세 이하"
    if "이상" in s or "+" in s:
        if nums and nums[0] >= 100:
            return "100세 이상"
        if nums:
            start = (nums[0] // 10) * 10
            if start == 0:
                return "9세 이하"
            if start >= 100:
                return "100세 이상"
            return f"{start}-{start+9}세"

    if len(nums) >= 2:
        start = (nums[0] // 10) * 10
        if start == 0:
            return "9세 이하"
        if start >= 100:
            return "100세 이상"
        return f"{start}-{start+9}세"

    if len(nums) == 1:
        n = nums[0]
        if n <= 9:
            return "9세 이하"
        if n >= 100:
            return "100세 이상"
        start = (n // 10) * 10
        return f"{start}-{start+9}세"

    return None


def apply_preprocess(df: pd.DataFrame, job: dict) -> pd.DataFrame:
    cfg = job.get("preprocess", {})
    if not isinstance(cfg, dict) or not cfg:
        return df

    d = df.copy()

    age_cfg = cfg.get("age_bucket_10y")
    if age_cfg:
        if not isinstance(age_cfg, dict):
            raise RuntimeError("preprocess.age_bucket_10y must be a dict")
        src = age_cfg.get("source", "C2_NM")
        if src not in d.columns:
            raise RuntimeError(f"preprocess source column missing: {src}")

        d[src] = d[src].map(map_age_to_10y_bucket)
        if age_cfg.get("drop_unknown", True):
            d = d[d[src].notna()].copy()

        order = [
            "계",
            "9세 이하",
            "10-19세",
            "20-29세",
            "30-39세",
            "40-49세",
            "50-59세",
            "60-69세",
            "70-79세",
            "80-89세",
            "90-99세",
            "100세 이상",
        ]
        d[src] = pd.Categorical(d[src], categories=order, ordered=True)

        if "DT" in d.columns:
            d["DT"] = pd.to_numeric(d["DT"], errors="coerce").fillna(0)
            group_cols = [c for c in d.columns if c != "DT"]
            d = d.groupby(group_cols, as_index=False, dropna=False, sort=False)["DT"].sum()
            d[src] = pd.Categorical(d[src], categories=order, ordered=True)

            sort_cols = [c for c in ["C1_NM", src, "PRD_DE", "ITM_NM"] if c in d.columns]
            if sort_cols:
                d = d.sort_values(sort_cols, kind="stable")

    return d

def make_default_pivot(df: pd.DataFrame) -> Optional[pd.DataFrame]:
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

    def fmt_prd(x: str) -> str:
        x = str(x)
        return f"{x[:4]}.{x[4:]}" if len(x) >= 6 else x

    pv.columns = [fmt_prd(c) for c in pv.columns]
    return pv.reset_index()

def make_custom_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
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

    # Optional row filters before pivot, e.g. {"PRD_DE": ["2015", "2025"]}
    filters = pivot_cfg.get("filters", {})
    if filters:
        if not isinstance(filters, dict):
            raise RuntimeError("pivot.filters must be a dict")
        for col, allowed in filters.items():
            if col not in d.columns:
                raise RuntimeError(f"pivot.filters column missing: {col}")
            allowed_vals = allowed if isinstance(allowed, list) else [allowed]
            allowed_vals = [str(v) for v in allowed_vals]
            d = d[d[col].astype(str).isin(allowed_vals)]

    if val in d.columns:
        d[val] = pd.to_numeric(d[val], errors="coerce")
    if "PRD_DE" in d.columns:
        d["PRD_DE"] = d["PRD_DE"].astype(str)

    sort_opt = bool(pivot_cfg.get("sort", True))
    pv = d.pivot_table(index=idx, columns=cols, values=val, aggfunc="first", sort=sort_opt)

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
    pivot_cfg = job.get("pivot")
    sheet_name = "TABLE_VIEW"
    if pivot_cfg and pivot_cfg.get("sheet_name"):
        sheet_name = pivot_cfg["sheet_name"]

    try:
        pivot_src = apply_preprocess(df, job)
        if pivot_cfg:
            return make_custom_pivot(pivot_src, pivot_cfg), sheet_name
        return make_default_pivot(pivot_src), sheet_name
    except Exception as e:
        print("⚠ TABLE_VIEW 생성 실패:", e)
        return None, sheet_name

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


def _fetch_kosis_df(job_like: dict) -> pd.DataFrame:
    if not KOSIS_API_KEY:
        raise RuntimeError("KOSIS_API_KEY 환경변수가 없습니다.")

    params = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "orgId": str(job_like["orgId"]).strip(),
        "tblId": str(job_like["tblId"]).strip(),
        "prdSe": job_like.get("prdSe", "M"),
        "format": job_like.get("format", "json"),
        "jsonVD": job_like.get("jsonVD", "Y"),
    }

    if job_like.get("newEstPrdCnt") is not None and str(job_like.get("newEstPrdCnt")).strip() != "":
        params["newEstPrdCnt"] = str(job_like["newEstPrdCnt"]).strip()

    if job_like.get("itmId"):
        params["itmId"] = str(job_like["itmId"]).strip()

    for i in range(1, 9):
        key = f"objL{i}"
        if job_like.get(key):
            params[key] = str(job_like[key]).strip()

    r = requests.get(KOSIS_BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"KOSIS API returned non-list: {data}")
    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("KOSIS returned 0 rows")
    return df


def run_kosis_multi_job(job: dict) -> Tuple[Any, Optional[pd.DataFrame], str]:
    sources = job.get("sources", [])
    if not isinstance(sources, list) or not sources:
        raise RuntimeError("kosis_multi requires non-empty sources list")

    merge_keys = job.get("merge_keys", ["C1", "C1_NM", "PRD_DE"])
    if not isinstance(merge_keys, list) or not merge_keys:
        raise RuntimeError("kosis_multi.merge_keys must be a non-empty list")

    metrics = job.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("kosis_multi.metrics must be a non-empty list")

    src_raw_frames: Dict[str, pd.DataFrame] = {}
    for src in sources:
        if not isinstance(src, dict):
            raise RuntimeError("Each source must be a dict")
        src_name = str(src.get("name", "")).strip()
        if not src_name:
            raise RuntimeError("Each source needs a non-empty name")
        df = _fetch_kosis_df(src)
        need = [c for c in merge_keys if c not in df.columns]
        if need:
            raise RuntimeError(f"source '{src_name}' missing merge keys: {need}")
        if "DT" not in df.columns:
            raise RuntimeError(f"source '{src_name}' missing DT column")
        src_raw_frames[src_name] = df.copy()

    merged: Optional[pd.DataFrame] = None
    for metric in metrics:
        if not isinstance(metric, dict):
            raise RuntimeError("Each metric must be a dict")
        mid = str(metric.get("id", "")).strip()
        if not mid:
            raise RuntimeError("Each metric needs id")
        src_name = str(metric.get("source", "")).strip()
        if not src_name:
            continue
        if src_name not in src_raw_frames:
            raise RuntimeError(f"Unknown source in metrics: {src_name}")

        src_df = src_raw_frames[src_name].copy()
        src_filter = metric.get("source_filter", {})
        if src_filter:
            if not isinstance(src_filter, dict):
                raise RuntimeError(f"metric '{mid}' source_filter must be a dict")
            for col, val in src_filter.items():
                if col not in src_df.columns:
                    raise RuntimeError(f"metric '{mid}' source_filter column missing: {col}")
                src_df = src_df[src_df[col].astype(str) == str(val)]

        pick = list(dict.fromkeys(merge_keys + ["DT"]))
        d = src_df[pick].copy()
        d["DT"] = pd.to_numeric(d["DT"], errors="coerce")

        agg = str(metric.get("agg", "first")).strip().lower()
        if agg == "sum":
            d = d.groupby(merge_keys, as_index=False, dropna=False)["DT"].sum()
        else:
            d = d.groupby(merge_keys, as_index=False, dropna=False)["DT"].first()

        d = d.rename(columns={"DT": mid})
        if merged is None:
            merged = d
        else:
            merged = merged.merge(d, on=merge_keys, how="outer")

    if merged is None:
        raise RuntimeError("No source-based metrics found")

    for metric in metrics:
        mid = str(metric.get("id", "")).strip()
        formula = metric.get("formula")
        if not mid or not formula:
            continue
        try:
            merged[mid] = merged.eval(str(formula))
        except Exception as e:
            raise RuntimeError(f"Failed to evaluate formula for metric '{mid}': {e}") from e
        if metric.get("round") is not None:
            merged[mid] = merged[mid].round(int(metric["round"]))

    for metric in metrics:
        mid = str(metric.get("id", "")).strip()
        if mid and mid in merged.columns:
            merged[mid] = pd.to_numeric(merged[mid], errors="coerce")

    long_parts: List[pd.DataFrame] = []
    for metric in metrics:
        mid = str(metric.get("id", "")).strip()
        if not mid or mid not in merged.columns:
            continue
        label = str(metric.get("label", mid))
        part = merged[merge_keys + [mid]].copy()
        part["METRIC"] = label
        part = part.rename(columns={mid: "VALUE"})
        long_parts.append(part)

    if not long_parts:
        raise RuntimeError("No metric columns available after merge/formula")

    raw_df = pd.concat(long_parts, ignore_index=True)
    pivot_df, sheet_name = build_table_view(raw_df, job)

    raw_out: Any = raw_df
    raw_sheets = job.get("raw_sheets", [])
    if isinstance(raw_sheets, list) and raw_sheets:
        sheet_map: Dict[str, pd.DataFrame] = {}
        for i, spec in enumerate(raw_sheets, start=1):
            if not isinstance(spec, dict):
                continue
            src_name = str(spec.get("source", "")).strip()
            if not src_name or src_name not in src_raw_frames:
                continue
            name = str(spec.get("sheet_name", f"RAW_{i}")).strip() or f"RAW_{i}"
            d = src_raw_frames[src_name].copy()

            flt = spec.get("filters", {})
            if isinstance(flt, dict):
                for col, val in flt.items():
                    if col in d.columns:
                        vals = val if isinstance(val, list) else [val]
                        d = d[d[col].astype(str).isin([str(v) for v in vals])]

            cols = spec.get("columns")
            if isinstance(cols, list) and cols:
                keep = [c for c in cols if c in d.columns]
                if keep:
                    d = d[keep].copy()

            sheet_map[name] = d

        if sheet_map:
            raw_out = sheet_map

    return raw_out, pivot_df, sheet_name

# -----------------------------
def run_data_go_kr_job(job: dict) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], str]:
    base_url = job.get("base_url")
    if not base_url:
        raise RuntimeError("data_go_kr job에는 base_url이 필요합니다.")

    params = job.get("params", {})
    if not isinstance(params, dict):
        raise RuntimeError("data_go_kr job의 params는 dict여야 합니다.")

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

    try:
        data = r.json()
    except Exception:
        raise RuntimeError("data.go.kr 응답이 JSON이 아닙니다. (job params에 type/json 옵션 확인 필요)")

    item_path = job.get("item_path", "")
    if not item_path:
        if isinstance(data, list):
            items = data
        else:
            raise RuntimeError("data_go_kr job에는 item_path가 필요합니다. (예: response.body.items.item)")
    else:
        items = deep_get(data, item_path)

    items = normalize_to_list(items)
    if not items:
        raise RuntimeError(f"data.go.kr items 0건 (item_path={item_path})")

    if isinstance(items[0], dict):
        df = pd.DataFrame(items)
    else:
        df = pd.DataFrame({"value": items})

    pivot_df, sheet_name = build_table_view(df, job)
    return df, pivot_df, sheet_name

# -----------------------------
def save_excel(job: dict, raw_df: Any, pivot_df: Optional[pd.DataFrame], sheet_name: str) -> Path:
    today = datetime.today().strftime("%Y%m%d")

    subdir = job.get("output_subdir", "")
    out_dir = OUTPUT_ROOT / subdir if subdir else OUTPUT_ROOT
    ensure_dir(out_dir)

    prefix = job.get("output_prefix", "export")
    prefix = sanitize_filename(prefix)

    file_name = f"{prefix}_{today}.xlsx"
    out_path = out_dir / file_name

    def normalize_sheet_name(name: str) -> str:
        n = str(name)
        for ch in "[]:*?/\\":  # Excel sheet forbidden characters
            n = n.replace(ch, "_")
        n = n.strip() or "RAW"
        return n[:31]

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        if isinstance(raw_df, dict):
            wrote = False
            for k, v in raw_df.items():
                if isinstance(v, pd.DataFrame):
                    v.to_excel(writer, sheet_name=normalize_sheet_name(k), index=False)
                    wrote = True
            if not wrote:
                pd.DataFrame().to_excel(writer, sheet_name="RAW", index=False)
        elif isinstance(raw_df, pd.DataFrame):
            raw_df.to_excel(writer, sheet_name="RAW", index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name="RAW", index=False)
        if pivot_df is not None:
            sheet = sheet_name[:31]
            pivot_df.to_excel(writer, sheet_name=sheet, index=False)

    return out_path

# -----------------------------
def run_job(job_path: Path) -> None:
    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)

    job_name = job.get("job_name", job_path.stem)
    provider = job.get("provider", "kosis")

    print(f"\n▶ 실행: {job_name}  (provider={provider})")

    if provider == "kosis":
        raw_df, pivot_df, sheet_name = run_kosis_job(job)
    elif provider == "kosis_multi":
        raw_df, pivot_df, sheet_name = run_kosis_multi_job(job)
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
