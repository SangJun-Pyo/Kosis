import os
import json
import re
import sys
import time
import ctypes
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd

# -----------------------------
KOSIS_BASE_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
JOBS_DIR = Path("jobs")
OUTPUT_ROOT = Path("output")
DEFAULT_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_WAIT_SEC = 2.0

KOSIS_API_KEY = os.getenv("KOSIS_API_KEY", "").strip()
ANSI_RESET = "\033[0m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_CYAN = "\033[36m"
_ANSI_READY: Optional[bool] = None

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def supports_ansi() -> bool:
    global _ANSI_READY

    if _ANSI_READY is not None:
        return _ANSI_READY

    if os.getenv("NO_COLOR"):
        _ANSI_READY = False
        return False

    if os.name != "nt":
        _ANSI_READY = sys.stdout.isatty()
        return _ANSI_READY

    if not sys.stdout.isatty():
        _ANSI_READY = False
        return False

    try:
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            enabled = mode.value | 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
            if kernel32.SetConsoleMode(handle, enabled):
                _ANSI_READY = True
                return True
    except Exception:
        pass

    _ANSI_READY = bool(os.getenv("WT_SESSION") or os.getenv("TERM") or os.getenv("ANSICON"))
    return _ANSI_READY


def colorize(text: str, color: str) -> str:
    if not supports_ansi():
        return text
    return f"{color}{text}{ANSI_RESET}"


def request_json_with_retry(url: str, params: dict, timeout: int = DEFAULT_TIMEOUT) -> Any:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_error = e
            if attempt >= MAX_RETRIES:
                break
            print(colorize(f"[WARN] 요청 실패 {attempt}/{MAX_RETRIES}: {e}", ANSI_YELLOW))
            print(f"[INFO] {RETRY_WAIT_SEC:.0f}초 후 재시도합니다.")
            time.sleep(RETRY_WAIT_SEC)

    raise RuntimeError(f"API 요청 실패 after {MAX_RETRIES} attempts: {last_error}")


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
            d = d.groupby(group_cols, as_index=False, dropna=False, sort=False, observed=True)["DT"].sum()
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
            observed=True,
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
    pv = d.pivot_table(index=idx, columns=cols, values=val, aggfunc="first", sort=sort_opt, observed=True)

    preserve_multi = bool(pivot_cfg.get("preserve_multiindex_columns", False))

    # 컬럼 평탄화
    if isinstance(pv.columns, pd.MultiIndex):
        if not preserve_multi:
            pv.columns = ["_".join(map(str, c)).strip() for c in pv.columns.values]
    else:
        pv.columns = [str(c) for c in pv.columns]

    label_map = pivot_cfg.get("column_label_map", {})
    if isinstance(label_map, dict) and label_map:
        if isinstance(pv.columns, pd.MultiIndex):
            pass
        else:
            pv.columns = [str(label_map.get(str(c), c)) for c in pv.columns]

    # (옵션) 월 포맷
    if pivot_cfg.get("flatten_columns_year", False):
        def fmt_prd2(x: str) -> str:
            x = str(x)
            return f"{x[:4]}.{x[4:]}" if len(x) >= 6 and x.isdigit() else x
        pv.columns = [fmt_prd2(c) for c in pv.columns]

    if preserve_multi:
        return pv

    return pv.reset_index()

def make_metric_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["C1_NM", "ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"summary pivot columns missing: {missing}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("summary pivot requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    item_ids = pivot_cfg.get("item_ids", [])
    if item_ids:
        d = d[d["ITM_ID"].astype(str).isin([str(x) for x in item_ids])].copy()

    item_order = [str(x) for x in item_ids] if item_ids else list(dict.fromkeys(d["ITM_ID"].astype(str)))
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d["C1_NM"].astype(str)))

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    pv = d.pivot_table(
        index=["ITM_ID", "ITM_NM", "C1_NM"],
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    ).reset_index()

    for y in years:
        if y not in pv.columns:
            pv[y] = pd.NA

    cagr_label = pivot_cfg.get("cagr_label")
    start_year = years[0]
    end_year = years[-1]
    if not cagr_label:
        cagr_label = f"CAGR('{start_year[2:]}~'{end_year[2:]})"

    periods = max(int(end_year) - int(start_year), 1)
    share_year = str(pivot_cfg.get("share_year", end_year))
    share_item_ids = {str(x) for x in pivot_cfg.get("share_item_ids", [])}
    if not share_item_ids:
        share_item_ids = {str(x) for x in pv["ITM_ID"].astype(str) if "(명)" in str(pv.loc[pv["ITM_ID"].astype(str) == str(x), "ITM_NM"].iloc[0])}

    rows: List[Dict[str, Any]] = []
    for item_id in item_order:
        block = pv[pv["ITM_ID"].astype(str) == item_id].copy()
        if block.empty:
            continue

        item_name = str(block["ITM_NM"].iloc[0])
        block["__region_order"] = block["C1_NM"].astype(str).map({name: i for i, name in enumerate(region_order)})
        block["__region_order"] = block["__region_order"].fillna(9999)
        block = block.sort_values("__region_order", kind="stable")

        national_series = block[block["C1_NM"].astype(str) == "전국"]
        national_val = None
        if not national_series.empty and share_year in national_series.columns:
            national_val = pd.to_numeric(national_series.iloc[0][share_year], errors="coerce")

        first_row = True
        for _, rec in block.iterrows():
            row: Dict[str, Any] = {
                "구분": item_name if first_row else "",
                "지역": "계" if str(rec["C1_NM"]) == "전국" else str(rec["C1_NM"]),
            }
            for y in years:
                row[f"{y}년"] = rec.get(y)

            start_val = pd.to_numeric(rec.get(start_year), errors="coerce")
            end_val = pd.to_numeric(rec.get(end_year), errors="coerce")
            if (
                pd.notna(start_val)
                and pd.notna(end_val)
                and start_val not in (0, 0.0)
                and start_val > 0
                and end_val > 0
            ):
                row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            else:
                row[cagr_label] = pd.NA

            if str(item_id) in share_item_ids and pd.notna(national_val) and national_val not in (0, 0.0):
                row["비중"] = round((pd.to_numeric(rec.get(share_year), errors="coerce") / national_val) * 100, 1)
            else:
                row["비중"] = pd.NA

            rows.append(row)
            first_row = False

    cols = ["구분", "지역"] + [f"{y}년" for y in years] + ["비중", cagr_label]
    return pd.DataFrame(rows, columns=cols)


def make_latest_rank_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["C1_NM", "ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest rank pivot columns missing: {missing}")

    year = str(pivot_cfg.get("year", "")).strip()
    if not year:
        raise RuntimeError("latest rank pivot requires year")

    item_ids = [str(x) for x in pivot_cfg.get("item_ids", [])]
    if not item_ids:
        raise RuntimeError("latest rank pivot requires item_ids")

    rank_item_id = str(pivot_cfg.get("rank_item_id", "")).strip()
    if not rank_item_id:
        raise RuntimeError("latest rank pivot requires rank_item_id")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[(d["PRD_DE"] == year) & (d["ITM_ID"].astype(str).isin(item_ids))].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")

    pv = d.pivot_table(
        index="C1_NM",
        columns="ITM_ID",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    ).reset_index()

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d["C1_NM"].astype(str)))

    for item_id in item_ids:
        if item_id not in pv.columns:
            pv[item_id] = pd.NA

    label_map = pivot_cfg.get("column_labels", {})
    rank_label = str(pivot_cfg.get("rank_label", "순위"))
    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))

    rank_series = pv.loc[pv["C1_NM"].astype(str) != national_name, ["C1_NM", rank_item_id]].copy()
    rank_series["__rank"] = pd.to_numeric(rank_series[rank_item_id], errors="coerce").rank(method="min", ascending=False)
    rank_map = dict(zip(rank_series["C1_NM"].astype(str), rank_series["__rank"]))

    pv["__order"] = pv["C1_NM"].astype(str).map({name: i for i, name in enumerate(region_order)})
    pv["__order"] = pv["__order"].fillna(9999)
    pv = pv.sort_values("__order", kind="stable")

    rows: List[Dict[str, Any]] = []
    for _, rec in pv.iterrows():
        row: Dict[str, Any] = {
            area_label: national_alias if str(rec["C1_NM"]) == national_name else str(rec["C1_NM"])
        }
        for item_id in item_ids:
            row[str(label_map.get(item_id, item_id))] = rec.get(item_id)
        row[rank_label] = pd.NA if str(rec["C1_NM"]) == national_name else int(rank_map.get(str(rec["C1_NM"]), 0))
        rows.append(row)

    cols = [area_label] + [str(label_map.get(item_id, item_id)) for item_id in item_ids] + [rank_label]
    return pd.DataFrame(rows, columns=cols)


def apply_row_filters(df: pd.DataFrame, filters: Any) -> pd.DataFrame:
    if not isinstance(filters, dict) or not filters:
        return df

    d = df.copy()
    for col, allowed in filters.items():
        if col not in d.columns:
            raise RuntimeError(f"filter column missing: {col}")
        allowed_vals = allowed if isinstance(allowed, list) else [allowed]
        allowed_vals = [str(v) for v in allowed_vals]
        d = d[d[col].astype(str).isin(allowed_vals)]
    return d


def make_rank_timeseries_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"rank timeseries columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if region_col not in df.columns:
        raise RuntimeError(f"rank timeseries region column missing: {region_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("rank timeseries requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin(years)].copy()

    pv = d.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    ).reset_index()

    for y in years:
        if y not in pv.columns:
            pv[y] = pd.NA

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))
    rank_label = str(pivot_cfg.get("rank_label", "순위"))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    rank_year = str(pivot_cfg.get("rank_year", years[-1]))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(d[region_col].astype(str)))

    pv["__order"] = pv[region_col].astype(str).map({name: i for i, name in enumerate(region_order)})
    pv["__order"] = pv["__order"].fillna(9999)
    pv = pv.sort_values("__order", kind="stable")

    rank_series = pv.loc[pv[region_col].astype(str) != national_name, [region_col, rank_year]].copy()
    rank_series["__rank"] = pd.to_numeric(rank_series[rank_year], errors="coerce").rank(method="min", ascending=False)
    rank_map = dict(zip(rank_series[region_col].astype(str), rank_series["__rank"]))

    periods = max(int(years[-1]) - int(years[0]), 1)
    rows: List[Dict[str, Any]] = []
    for _, rec in pv.iterrows():
        row: Dict[str, Any] = {
            area_label: national_alias if str(rec[region_col]) == national_name else str(rec[region_col])
        }
        for y in years:
            row[f"{y}년"] = rec.get(y)

        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
            row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
        else:
            row[cagr_label] = pd.NA

        row[rank_label] = pd.NA if str(rec[region_col]) == national_name else int(rank_map.get(str(rec[region_col]), 0))
        rows.append(row)

    cols = [area_label] + [f"{y}년" for y in years] + [rank_label, cagr_label]
    return pd.DataFrame(rows, columns=cols)


def build_source_views(source_frames: Dict[str, pd.DataFrame], job: dict) -> Dict[str, pd.DataFrame]:
    views: Dict[str, pd.DataFrame] = {}
    specs = job.get("views", [])
    if not isinstance(specs, list) or not specs:
        return views

    for i, spec in enumerate(specs, start=1):
        if not isinstance(spec, dict):
            continue
        src_name = str(spec.get("source", "")).strip()
        if not src_name or src_name not in source_frames:
            print(f"[WARN] view source missing: {src_name}")
            continue

        d = source_frames[src_name].copy()
        d = apply_row_filters(d, spec.get("filters", {}))
        sheet_name = str(spec.get("sheet_name", f"TABLE_VIEW_{i}")).strip() or f"TABLE_VIEW_{i}"
        kind = str(spec.get("kind", "pivot")).strip().lower()

        try:
            if kind == "pivot":
                views[sheet_name] = make_custom_pivot(d, spec)
            elif kind == "sum_pivot":
                group_cols = spec.get("groupby", [])
                if not isinstance(group_cols, list) or not group_cols:
                    raise RuntimeError("sum_pivot requires non-empty groupby")
                work = d.copy()
                work["DT"] = pd.to_numeric(work["DT"], errors="coerce").fillna(0)
                work = work.groupby(group_cols, as_index=False, dropna=False, observed=True)["DT"].sum()
                pivot_spec = dict(spec)
                pivot_spec.pop("filters", None)
                pivot_spec.pop("groupby", None)
                views[sheet_name] = make_custom_pivot(work, pivot_spec)
            elif kind == "rank_timeseries":
                views[sheet_name] = make_rank_timeseries_pivot(d, spec)
            elif kind == "metric_summary":
                views[sheet_name] = make_metric_summary_pivot(d, spec)
            elif kind == "latest_rank":
                views[sheet_name] = make_latest_rank_pivot(d, spec)
            else:
                raise RuntimeError(f"unknown view kind: {kind}")
        except Exception as e:
            print(f"[WARN] source view 생성 실패 ({sheet_name}): {e}")

    return views


def build_table_views(df: pd.DataFrame, job: dict) -> Dict[str, pd.DataFrame]:
    views: Dict[str, pd.DataFrame] = {}
    pivot_src = apply_preprocess(df, job)

    pivot_cfg = job.get("pivot")
    primary_sheet_name = "TABLE_VIEW"
    if pivot_cfg and pivot_cfg.get("sheet_name"):
        primary_sheet_name = pivot_cfg["sheet_name"]

    try:
        if pivot_cfg:
            primary_df = make_custom_pivot(pivot_src, pivot_cfg)
        else:
            primary_df = make_default_pivot(pivot_src)
        if primary_df is not None:
            views[primary_sheet_name] = primary_df
    except Exception as e:
        print("[WARN] TABLE_VIEW 생성 실패:", e)

    extra_pivots = job.get("extra_pivots", [])
    if isinstance(extra_pivots, list):
        for i, cfg in enumerate(extra_pivots, start=1):
            if not isinstance(cfg, dict):
                continue
            sheet_name = str(cfg.get("sheet_name", f"TABLE_VIEW_{i}")).strip() or f"TABLE_VIEW_{i}"
            kind = str(cfg.get("kind", "pivot")).strip().lower()
            try:
                if kind == "metric_summary":
                    views[sheet_name] = make_metric_summary_pivot(df, cfg)
                elif kind == "latest_rank":
                    views[sheet_name] = make_latest_rank_pivot(df, cfg)
                else:
                    views[sheet_name] = make_custom_pivot(pivot_src, cfg)
            except Exception as e:
                print(f"[WARN] 추가 피벗 생성 실패 ({sheet_name}): {e}")

    return views


def build_table_view(df: pd.DataFrame, job: dict) -> Tuple[Optional[pd.DataFrame], str]:
    views = build_table_views(df, job)
    if not views:
        return None, "TABLE_VIEW"
    first_sheet = next(iter(views))
    return views[first_sheet], first_sheet

# -----------------------------
def run_kosis_job(job: dict) -> Tuple[pd.DataFrame, Any, str]:
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

    data = request_json_with_retry(KOSIS_BASE_URL, params=params)
    if not isinstance(data, list):
        raise RuntimeError(f"KOSIS API 오류(리스트 아님): {data}")

    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("KOSIS 데이터 0건 (파라미터 확인 필요)")

    pivot_views = build_table_views(df, job)
    first_sheet = next(iter(pivot_views), "TABLE_VIEW")
    return df, pivot_views if pivot_views else None, first_sheet


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

    data = request_json_with_retry(KOSIS_BASE_URL, params=params)
    if not isinstance(data, list):
        raise RuntimeError(f"KOSIS API returned non-list: {data}")
    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("KOSIS returned 0 rows")
    return df


def run_kosis_multi_job(job: dict) -> Tuple[Any, Any, str]:
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
            d = d.groupby(merge_keys, as_index=False, dropna=False, observed=True)["DT"].sum()
        else:
            d = d.groupby(merge_keys, as_index=False, dropna=False, observed=True)["DT"].first()

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
    pivot_views = build_table_views(raw_df, job)
    sheet_name = next(iter(pivot_views), "TABLE_VIEW")

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

    return raw_out, pivot_views if pivot_views else None, sheet_name


def run_kosis_sources_job(job: dict) -> Tuple[Any, Any, str]:
    sources = job.get("sources", [])
    if not isinstance(sources, list) or not sources:
        raise RuntimeError("kosis_sources requires non-empty sources list")

    src_raw_frames: Dict[str, pd.DataFrame] = {}
    for src in sources:
        if not isinstance(src, dict):
            raise RuntimeError("Each source must be a dict")
        src_name = str(src.get("name", "")).strip()
        if not src_name:
            raise RuntimeError("Each source needs a non-empty name")
        src_raw_frames[src_name] = _fetch_kosis_df(src)

    pivot_views = build_source_views(src_raw_frames, job)
    sheet_name = next(iter(pivot_views), "TABLE_VIEW")

    raw_out: Any = src_raw_frames
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
            d = apply_row_filters(d, spec.get("filters", {}))

            cols = spec.get("columns")
            if isinstance(cols, list) and cols:
                keep = [c for c in cols if c in d.columns]
                if keep:
                    d = d[keep].copy()
            sheet_map[name] = d
        if sheet_map:
            raw_out = sheet_map

    return raw_out, pivot_views if pivot_views else None, sheet_name

# -----------------------------
def run_data_go_kr_job(job: dict) -> Tuple[pd.DataFrame, Any, str]:
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

    data = request_json_with_retry(base_url, params=params)

    if not isinstance(data, (dict, list)):
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

    pivot_views = build_table_views(df, job)
    first_sheet = next(iter(pivot_views), "TABLE_VIEW")
    return df, pivot_views if pivot_views else None, first_sheet

# -----------------------------
def save_excel(job: dict, raw_df: Any, pivot_df: Any, sheet_name: str) -> Path:
    today = datetime.today().strftime("%Y%m%d")

    subdir = job.get("output_subdir", "")
    out_dir = OUTPUT_ROOT / subdir if subdir else OUTPUT_ROOT
    ensure_dir(out_dir)

    prefix = job.get("output_prefix", "export")
    prefix = sanitize_filename(prefix)

    def normalize_sheet_name(name: str) -> str:
        n = str(name)
        for ch in "[]:*?/\\":  # Excel sheet forbidden characters
            n = n.replace(ch, "_")
        n = n.strip() or "RAW"
        return n[:31]

    def write_df(writer: pd.ExcelWriter, df: pd.DataFrame, target_sheet: str) -> None:
        use_index = isinstance(df.columns, pd.MultiIndex)
        df.to_excel(writer, sheet_name=normalize_sheet_name(target_sheet), index=use_index)

    def candidate_paths() -> List[Path]:
        base = out_dir / f"{prefix}_{today}.xlsx"
        paths = [base]
        for i in range(1, 100):
            paths.append(out_dir / f"{prefix}_{today}_{i:02d}.xlsx")
        return paths

    last_error: Optional[Exception] = None
    for out_path in candidate_paths():
        try:
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                if isinstance(raw_df, dict):
                    wrote = False
                    for k, v in raw_df.items():
                        if isinstance(v, pd.DataFrame):
                            write_df(writer, v, k)
                            wrote = True
                    if not wrote:
                        pd.DataFrame().to_excel(writer, sheet_name="RAW", index=False)
                elif isinstance(raw_df, pd.DataFrame):
                    write_df(writer, raw_df, "RAW")
                else:
                    pd.DataFrame().to_excel(writer, sheet_name="RAW", index=False)

                if isinstance(pivot_df, dict):
                    for k, v in pivot_df.items():
                        if isinstance(v, pd.DataFrame):
                            write_df(writer, v, k)
                elif isinstance(pivot_df, pd.DataFrame):
                    write_df(writer, pivot_df, sheet_name[:31])
            if last_error is not None:
                print(colorize(f"[WARN] 기존 파일이 열려 있어 다른 이름으로 저장했습니다: {out_path.name}", ANSI_YELLOW))
            return out_path
        except PermissionError as e:
            last_error = e
            print(colorize(f"[WARN] 파일이 열려 있습니다. 해당 파일을 닫아주세요: {out_path}", ANSI_YELLOW))
            continue

    raise last_error if last_error else RuntimeError("Failed to save Excel output")

# -----------------------------
def run_job(job_path: Path, idx: Optional[int] = None, total: Optional[int] = None) -> None:
    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)

    job_name = job.get("job_name", job_path.stem)
    provider = job.get("provider", "kosis")
    started = time.time()
    prefix = "[RUN]"

    if idx is not None and total:
        pct = int((idx / total) * 100)
        prefix = f"[RUN {idx}/{total} {pct}%]"

    print(f"\n{colorize(prefix, ANSI_CYAN)} {job_name}  (provider={provider})")

    if provider == "kosis":
        raw_df, pivot_df, sheet_name = run_kosis_job(job)
    elif provider == "kosis_multi":
        raw_df, pivot_df, sheet_name = run_kosis_multi_job(job)
    elif provider == "kosis_sources":
        raw_df, pivot_df, sheet_name = run_kosis_sources_job(job)
    elif provider == "data_go_kr":
        raw_df, pivot_df, sheet_name = run_data_go_kr_job(job)
    else:
        raise RuntimeError(f"Unknown provider: {provider}")

    out_path = save_excel(job, raw_df, pivot_df, sheet_name)
    elapsed = time.time() - started
    ok_prefix = "[OK]"
    if idx is not None and total:
        pct = int((idx / total) * 100)
        ok_prefix = f"[OK  {idx}/{total} {pct}%]"
    print(f"{colorize(ok_prefix, ANSI_GREEN)} 저장 완료: {out_path} ({elapsed:.1f}s)")

def resolve_job_files(args: List[str]) -> List[Path]:
    targets = [Path(a) for a in args] if args else [JOBS_DIR]
    jobs: List[Path] = []

    for target in targets:
        if target.is_file():
            if target.suffix.lower() == ".json":
                jobs.append(target)
            continue

        if target.is_dir():
            jobs.extend(sorted(target.rglob("*.json")))
            continue

        print(colorize(f"[WARN] 경로를 찾을 수 없어 건너뜀: {target}", ANSI_YELLOW))

    uniq: List[Path] = []
    seen = set()
    for job in jobs:
        key = str(job.resolve())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(job)
    return uniq

def main():
    jobs = resolve_job_files(sys.argv[1:])
    print(f"총 {len(jobs)}개 job 실행")

    success = 0
    failed = 0
    all_started = time.time()

    for i, job_file in enumerate(jobs, start=1):
        try:
            run_job(job_file, i, len(jobs))
            success += 1
        except KeyboardInterrupt:
            total_elapsed = time.time() - all_started
            print()
            print(colorize("[CANCEL] 사용자 중단으로 실행을 종료합니다.", ANSI_YELLOW))
            print(f"중단 시점 요약: 성공 {success}, 실패 {failed}, 총 {total_elapsed:.1f}s")
            return
        except Exception as e:
            failed += 1
            print(colorize(f"[ERROR] 실패: {job_file.name} -> {e}", ANSI_RED))

    total_elapsed = time.time() - all_started
    print(f"\n모든 작업 완료 (성공 {success}, 실패 {failed}, 총 {total_elapsed:.1f}s)")


if __name__ == "__main__":
    main()
