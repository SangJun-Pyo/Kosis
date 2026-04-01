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

    region_cfg = cfg.get("dedupe_region_names")
    if region_cfg:
        if not isinstance(region_cfg, dict):
            raise RuntimeError("preprocess.dedupe_region_names must be a dict")
        code_col = str(region_cfg.get("code_col", "C1"))
        name_col = str(region_cfg.get("name_col", "C1_NM"))
        label_col = str(region_cfg.get("label_col", f"{name_col}_LABEL"))
        if code_col not in d.columns or name_col not in d.columns:
            raise RuntimeError(f"dedupe_region_names columns missing: {code_col}, {name_col}")

        def normalize_region_name(v: Any) -> str:
            s = str(v).strip()
            s = re.sub(r"\s{2,}", "", s)
            return s

        def shorten_region_name(v: str) -> str:
            s = normalize_region_name(v)
            for suffix in ["특별자치시", "특별자치도", "특별시", "광역시", "자치시", "자치도", "도"]:
                if s.endswith(suffix):
                    return s[: -len(suffix)]
            return s

        d[label_col] = d[name_col].map(normalize_region_name)
        parent_map: Dict[str, str] = {}
        for _, rec in d[[code_col, name_col]].drop_duplicates().iterrows():
            code = str(rec[code_col]).strip().replace("'", "")
            name = normalize_region_name(rec[name_col])
            if len(code) == 2 and code != "00":
                parent_map[code] = name

        counts = d[label_col].value_counts(dropna=False)

        def build_label(rec: pd.Series) -> str:
            code = str(rec[code_col]).strip().replace("'", "")
            name = str(rec[label_col])
            if code in ("", "00") or len(code) <= 2 or counts.get(name, 0) <= 1:
                return name
            parent_name = parent_map.get(code[:2], "")
            if not parent_name:
                return name
            return f"{shorten_region_name(parent_name)}_{name}"

        d[label_col] = d.apply(build_label, axis=1)

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

    quarter_cfg = cfg.get("quarter_pick_latest_or_q4")
    if quarter_cfg:
        if not isinstance(quarter_cfg, dict):
            raise RuntimeError("preprocess.quarter_pick_latest_or_q4 must be a dict")
        src_col = str(quarter_cfg.get("source", "PRD_DE"))
        if src_col not in d.columns:
            raise RuntimeError(f"quarter_pick_latest_or_q4 source column missing: {src_col}")

        def _parse_quarter(v: Any) -> Tuple[int, int]:
            s = str(v).strip()
            m = re.match(r"^(\d{4})\.(\d)/4$", s)
            if m:
                return int(m.group(1)), int(m.group(2))
            if re.match(r"^\d{4}$", s):
                return int(s), 4
            return -1, -1

        picked: Dict[int, Tuple[int, str]] = {}
        for raw in d[src_col].astype(str):
            y, q = _parse_quarter(raw)
            if y < 0:
                continue
            prev = picked.get(y)
            if prev is None or q > prev[0]:
                picked[y] = (q, raw)
        keep = {v for _, v in picked.values()}
        if keep:
            d = d[d[src_col].astype(str).isin(keep)].copy()

    hierarchy_cfg = cfg.get("hierarchy_map")
    if hierarchy_cfg:
        if not isinstance(hierarchy_cfg, dict):
            raise RuntimeError("preprocess.hierarchy_map must be a dict")
        code_col = str(hierarchy_cfg.get("code_col", "C2"))
        group_col = str(hierarchy_cfg.get("group_col", "GROUP_NM"))
        detail_col = str(hierarchy_cfg.get("detail_col", "DETAIL_NM"))
        order_col = str(hierarchy_cfg.get("order_col", "DISPLAY_ORDER"))
        mapping = hierarchy_cfg.get("mapping", {})
        if code_col not in d.columns:
            raise RuntimeError(f"hierarchy_map code column missing: {code_col}")
        if not isinstance(mapping, dict):
            raise RuntimeError("hierarchy_map.mapping must be a dict")

        map_norm = {str(k): v for k, v in mapping.items() if isinstance(v, dict)}
        d[group_col] = pd.NA
        d[detail_col] = pd.NA
        d[order_col] = pd.NA

        def _apply_map(rec: pd.Series) -> pd.Series:
            k = str(rec[code_col])
            m = map_norm.get(k)
            if not m:
                return rec
            rec[group_col] = m.get("group", pd.NA)
            rec[detail_col] = m.get("detail", pd.NA)
            rec[order_col] = m.get("order", pd.NA)
            return rec

        d = d.apply(_apply_map, axis=1)
        d = d[d[group_col].notna()].copy()

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
    include_share = bool(pivot_cfg.get("include_share", True))
    if include_share and not share_item_ids:
        share_item_ids = {str(x) for x in pv["ITM_ID"].astype(str) if "(명)" in str(pv.loc[pv["ITM_ID"].astype(str) == str(x), "ITM_NM"].iloc[0])}
    annual_change_label = str(pivot_cfg.get("annual_change_label", "")).strip()
    annual_change_item_ids = {str(x) for x in pivot_cfg.get("annual_change_item_ids", [])}

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

            if include_share and str(item_id) in share_item_ids and pd.notna(national_val) and national_val not in (0, 0.0):
                row["비중"] = round((pd.to_numeric(rec.get(share_year), errors="coerce") / national_val) * 100, 1)
            else:
                row["비중"] = pd.NA

            if annual_change_label:
                if str(item_id) in annual_change_item_ids and pd.notna(start_val) and pd.notna(end_val):
                    row[annual_change_label] = round((end_val - start_val) / periods, 2)
                else:
                    row[annual_change_label] = pd.NA

            rows.append(row)
            first_row = False

    cols = ["구분", "지역"] + [f"{y}년" for y in years]
    if include_share:
        cols.append("비중")
    if annual_change_label:
        cols.append(annual_change_label)
    cols.append(cagr_label)
    return pd.DataFrame(rows, columns=cols)


def make_metric_block_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"metric block summary columns missing: {missing}")

    row_col = str(pivot_cfg.get("row_col", "C2_NM"))
    if row_col not in df.columns:
        raise RuntimeError(f"metric block summary row column missing: {row_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("metric block summary requires non-empty years")

    item_ids = [str(x) for x in pivot_cfg.get("item_ids", [])]
    if not item_ids:
        raise RuntimeError("metric block summary requires item_ids")

    item_labels = {str(k): str(v) for k, v in pivot_cfg.get("item_labels", {}).items()}
    row_order = [str(x) for x in pivot_cfg.get("row_order", [])]
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(years[-1]) - int(years[0]), 1)

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()
    d = d[d["ITM_ID"].astype(str).isin(item_ids)].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")

    pv = d.pivot_table(
        index=row_col,
        columns=["ITM_ID", "PRD_DE"],
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    if not row_order:
        row_order = list(dict.fromkeys(d[row_col].astype(str)))

    tuples = []
    for item_id in item_ids:
        top = item_labels.get(item_id, item_id)
        for year in years:
            tuples.append((top, f"{year}년"))
        tuples.append((top, cagr_label))

    out = pd.DataFrame(index=row_order, columns=pd.MultiIndex.from_tuples(tuples), dtype="object")
    for row_name in row_order:
        for item_id in item_ids:
            top = item_labels.get(item_id, item_id)
            for year in years:
                out.loc[row_name, (top, f"{year}년")] = pv.loc[row_name, (item_id, year)] if row_name in pv.index and (item_id, year) in pv.columns else pd.NA
            start_val = pd.to_numeric(out.loc[row_name, (top, f"{years[0]}년")], errors="coerce")
            end_val = pd.to_numeric(out.loc[row_name, (top, f"{years[-1]}년")], errors="coerce")
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
                out.loc[row_name, (top, cagr_label)] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            else:
                out.loc[row_name, (top, cagr_label)] = pd.NA

    out.index.name = str(pivot_cfg.get("row_label", "구분"))
    return out


def make_year_gender_mix_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"year gender mix columns missing: {missing}")

    index_col = str(pivot_cfg.get("index_col", "ITM_NM"))
    sex_col = str(pivot_cfg.get("sex_col", "C2_NM"))
    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if index_col not in df.columns or sex_col not in df.columns or region_col not in df.columns:
        raise RuntimeError("year gender mix required columns missing")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    detail_year = str(pivot_cfg.get("detail_year", years[-1] if years else ""))
    total_label = str(pivot_cfg.get("total_label", "계"))
    detail_labels = [str(x) for x in pivot_cfg.get("detail_labels", ["남자", "여자"])]
    item_order = [str(x) for x in pivot_cfg.get("item_order", [])]
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    keep_years = list(dict.fromkeys(years + [detail_year]))
    d = d[d["PRD_DE"].isin(keep_years)].copy()

    rows = item_order or list(dict.fromkeys(d[index_col].astype(str)))
    regions = region_order or list(dict.fromkeys(d[region_col].astype(str)))

    col_tuples: List[Tuple[str, str]] = []
    for region in regions:
        for year in years:
            col_tuples.append((region, year))
        for label in detail_labels:
            col_tuples.append((region, f"{detail_year} {label}"))

    out = pd.DataFrame(index=rows, columns=pd.MultiIndex.from_tuples(col_tuples), dtype="object")
    for region in regions:
        block = d[d[region_col].astype(str) == region].copy()
        for item in rows:
            item_df = block[block[index_col].astype(str) == item].copy()
            for year in years:
                val = item_df[(item_df["PRD_DE"] == year) & (item_df[sex_col].astype(str) == total_label)]["DT"]
                out.loc[item, (region, year)] = val.iloc[0] if not val.empty else pd.NA
            for label in detail_labels:
                val = item_df[(item_df["PRD_DE"] == detail_year) & (item_df[sex_col].astype(str) == label)]["DT"]
                out.loc[item, (region, f"{detail_year} {label}")] = val.iloc[0] if not val.empty else pd.NA

    out.index.name = str(pivot_cfg.get("row_label", "항목"))
    return out


def make_latest_profile_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"latest profile summary columns missing: {missing}")

    year = str(pivot_cfg.get("year", "")).strip()
    if not year:
        raise RuntimeError("latest profile summary requires year")

    sex_col = str(pivot_cfg.get("sex_col", "C2_NM"))
    total_label = str(pivot_cfg.get("total_label", "계"))
    male_label = str(pivot_cfg.get("male_label", "남자"))
    female_label = str(pivot_cfg.get("female_label", "여자"))
    item_order = [str(x) for x in pivot_cfg.get("item_order", [])]
    total_item_id = str(pivot_cfg.get("total_item_id", "")).strip()
    if not total_item_id:
        raise RuntimeError("latest profile summary requires total_item_id")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"] == year].copy()

    rows: List[Dict[str, Any]] = []
    total_series = d[(d["ITM_ID"].astype(str) == total_item_id) & (d[sex_col].astype(str) == total_label)]["DT"]
    grand_total = pd.to_numeric(total_series.iloc[0], errors="coerce") if not total_series.empty else pd.NA

    for item_id in item_order:
        block = d[d["ITM_ID"].astype(str) == item_id].copy()
        if block.empty:
            continue
        item_name = str(block["ITM_NM"].iloc[0])
        total_val = pd.to_numeric(block[block[sex_col].astype(str) == total_label]["DT"].iloc[0], errors="coerce") if not block[block[sex_col].astype(str) == total_label].empty else pd.NA
        male_val = pd.to_numeric(block[block[sex_col].astype(str) == male_label]["DT"].iloc[0], errors="coerce") if not block[block[sex_col].astype(str) == male_label].empty else pd.NA
        female_val = pd.to_numeric(block[block[sex_col].astype(str) == female_label]["DT"].iloc[0], errors="coerce") if not block[block[sex_col].astype(str) == female_label].empty else pd.NA

        row = {"항목": item_name, "계": total_val}
        row["비중"] = round((total_val / grand_total) * 100, 1) if pd.notna(total_val) and pd.notna(grand_total) and grand_total not in (0, 0.0) else pd.NA
        row["남자 인구수"] = male_val
        row["여자 인구수"] = female_val
        row["성비"] = round((male_val / female_val) * 100, 1) if pd.notna(male_val) and pd.notna(female_val) and female_val not in (0, 0.0) else pd.NA
        rows.append(row)

    return pd.DataFrame(rows, columns=["항목", "계", "비중", "남자 인구수", "여자 인구수", "성비"])


def make_timeseries_profile_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["ITM_ID", "ITM_NM", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"timeseries profile summary columns missing: {missing}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("timeseries profile summary requires years")

    sex_col = str(pivot_cfg.get("sex_col", "C2_NM"))
    total_label = str(pivot_cfg.get("total_label", "계"))
    male_label = str(pivot_cfg.get("male_label", "남자"))
    female_label = str(pivot_cfg.get("female_label", "여자"))
    item_order = [str(x) for x in pivot_cfg.get("item_order", [])]
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(years[-1]) - int(years[0]), 1)

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin(years)].copy()

    rows: List[Dict[str, Any]] = []
    latest_year = years[-1]
    for item_id in item_order:
        block = d[d["ITM_ID"].astype(str) == item_id].copy()
        if block.empty:
            continue
        item_name = str(block["ITM_NM"].iloc[0])
        row: Dict[str, Any] = {"구분": item_name}
        for year in years:
            val = block[(block["PRD_DE"] == year) & (block[sex_col].astype(str) == total_label)]["DT"]
            row[f"{year}년 총인구수"] = val.iloc[0] if not val.empty else pd.NA
        male_val = block[(block["PRD_DE"] == latest_year) & (block[sex_col].astype(str) == male_label)]["DT"]
        female_val = block[(block["PRD_DE"] == latest_year) & (block[sex_col].astype(str) == female_label)]["DT"]
        male_num = pd.to_numeric(male_val.iloc[0], errors="coerce") if not male_val.empty else pd.NA
        female_num = pd.to_numeric(female_val.iloc[0], errors="coerce") if not female_val.empty else pd.NA
        row["성비"] = round((male_num / female_num) * 100, 1) if pd.notna(male_num) and pd.notna(female_num) and female_num not in (0, 0.0) else pd.NA
        start_val = pd.to_numeric(row.get(f"{years[0]}년 총인구수"), errors="coerce")
        end_val = pd.to_numeric(row.get(f"{years[-1]}년 총인구수"), errors="coerce")
        row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
        rows.append(row)

    cols = ["구분"] + [f"{y}년 총인구수" for y in years] + ["성비", cagr_label]
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


def make_ratio_timeseries_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"ratio timeseries columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if region_col not in df.columns:
        raise RuntimeError(f"ratio timeseries region column missing: {region_col}")

    groupby = pivot_cfg.get("groupby", [region_col, "PRD_DE"])
    if not isinstance(groupby, list) or not groupby:
        raise RuntimeError("ratio timeseries requires non-empty groupby")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("ratio timeseries requires non-empty years")

    numerator_filters = pivot_cfg.get("numerator_filters", {})
    denominator_filters = pivot_cfg.get("denominator_filters", {})

    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce").fillna(0)
    work = work[work["PRD_DE"].isin(years)].copy()

    num = apply_row_filters(work, numerator_filters)
    den = apply_row_filters(work, denominator_filters)

    num = num.groupby(groupby, as_index=False, dropna=False, observed=True)["DT"].sum().rename(columns={"DT": "NUM"})
    den = den.groupby(groupby, as_index=False, dropna=False, observed=True)["DT"].sum().rename(columns={"DT": "DEN"})
    merged = num.merge(den, on=groupby, how="outer")
    merged["VALUE"] = merged["NUM"] / merged["DEN"]

    pv = merged.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="VALUE",
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
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    stage_label = str(pivot_cfg.get("stage_label", "소멸위험 5단계"))
    latest_year = str(pivot_cfg.get("latest_year", years[-1]))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(work[region_col].astype(str)))

    pv["__order"] = pv[region_col].astype(str).map({name: i for i, name in enumerate(region_order)})
    pv["__order"] = pv["__order"].fillna(9999)
    pv = pv.sort_values("__order", kind="stable")

    rows: List[Dict[str, Any]] = []
    for _, rec in pv.iterrows():
        row_name = national_alias if str(rec[region_col]) == national_name else str(rec[region_col])
        row: Dict[str, Any] = {area_label: row_name}
        for y in years:
            val = pd.to_numeric(rec.get(y), errors="coerce")
            row[f"{y}년"] = round(val, 2) if pd.notna(val) else pd.NA

        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
            row[cagr_label] = round((((end_val / start_val) ** (1 / max(int(years[-1]) - int(years[0]), 1))) - 1) * 100, 1)
        else:
            row[cagr_label] = pd.NA

        latest_val = pd.to_numeric(rec.get(latest_year), errors="coerce")
        if pd.isna(latest_val):
            row[stage_label] = pd.NA
        elif latest_val < 0.2:
            row[stage_label] = "고위험"
        elif latest_val < 0.5:
            row[stage_label] = "위험진입"
        elif latest_val < 1.0:
            row[stage_label] = "주의단계"
        elif latest_val < 1.5:
            row[stage_label] = "보통"
        else:
            row[stage_label] = "저위험"

        rows.append(row)

    subtotal = pivot_cfg.get("subtotal", {})
    if isinstance(subtotal, dict):
        members = [str(x) for x in subtotal.get("members", [])]
        label = str(subtotal.get("label", "")).strip()
        if members and label:
            sub = pv[pv[region_col].astype(str).isin(members)].copy()
            if not sub.empty:
                row = {area_label: label}
                for y in years:
                    vals = pd.to_numeric(sub[y], errors="coerce")
                    row[f"{y}년"] = round(vals.mean(), 2) if vals.notna().any() else pd.NA
                start_val = pd.to_numeric(row.get(f"{years[0]}년"), errors="coerce")
                end_val = pd.to_numeric(row.get(f"{years[-1]}년"), errors="coerce")
                if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0:
                    row[cagr_label] = round((((end_val / start_val) ** (1 / max(int(years[-1]) - int(years[0]), 1))) - 1) * 100, 1)
                else:
                    row[cagr_label] = pd.NA
                latest_val = pd.to_numeric(row.get(f"{latest_year}년"), errors="coerce")
                if pd.isna(latest_val):
                    row[stage_label] = pd.NA
                elif latest_val < 0.2:
                    row[stage_label] = "고위험"
                elif latest_val < 0.5:
                    row[stage_label] = "위험진입"
                elif latest_val < 1.0:
                    row[stage_label] = "주의단계"
                elif latest_val < 1.5:
                    row[stage_label] = "보통"
                else:
                    row[stage_label] = "저위험"
                rows.append(row)

    cols = [area_label] + [f"{y}년" for y in years] + [cagr_label, stage_label]
    return pd.DataFrame(rows, columns=cols)


def apply_value_maps(df: pd.DataFrame, spec: dict) -> pd.DataFrame:
    d = df.copy()
    replace_values = spec.get("replace_values", {})
    if isinstance(replace_values, dict):
        for col, mapping in replace_values.items():
            if col in d.columns and isinstance(mapping, dict):
                d[col] = d[col].astype(str).replace({str(k): v for k, v in mapping.items()})
    return d


def make_paired_metric_timeseries_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"paired metric timeseries columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM")).strip()
    if region_col not in df.columns:
        raise RuntimeError(f"paired metric timeseries region column missing: {region_col}")

    metrics = pivot_cfg.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("paired metric timeseries requires non-empty metrics")

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(df[region_col].astype(str)))

    result = pd.DataFrame(index=pd.Index(region_order, name=area_label))
    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")

    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        label = str(metric.get("label", "")).strip()
        years = [str(y) for y in metric.get("years", [])]
        if not label or not years:
            continue

        cagr_label = str(metric.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
        periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)
        block = apply_row_filters(work, metric.get("filters", {}))
        block = block[block["PRD_DE"].isin(years)].copy()

        pv = block.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in pv.columns:
                pv[year] = pd.NA
        pv = pv.reindex(region_order)

        for year in years:
            result[(label, f"{year}년")] = pv[year]

        start_vals = pd.to_numeric(pv[years[0]], errors="coerce")
        end_vals = pd.to_numeric(pv[years[-1]], errors="coerce")
        cagr_vals = pd.Series(pd.NA, index=pv.index, dtype="object")
        mask = start_vals.notna() & end_vals.notna() & (start_vals > 0) & (end_vals > 0)
        cagr_vals.loc[mask] = ((((end_vals.loc[mask] / start_vals.loc[mask]) ** (1 / periods)) - 1) * 100).round(1)
        result[(label, cagr_label)] = cagr_vals

    result.index = [national_alias if str(x) == national_name else str(x) for x in result.index]
    result.columns = pd.MultiIndex.from_tuples(result.columns)
    return result


def make_paired_metric_latest_compare_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"paired metric latest compare columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM")).strip()
    if region_col not in df.columns:
        raise RuntimeError(f"paired metric latest compare region column missing: {region_col}")

    metrics = pivot_cfg.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise RuntimeError("paired metric latest compare requires non-empty metrics")

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))

    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(df[region_col].astype(str)))

    result = pd.DataFrame(index=pd.Index(region_order, name=area_label))
    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")

    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        label = str(metric.get("label", "")).strip()
        years = [str(y) for y in metric.get("years", [])]
        if not label or len(years) != 2:
            continue

        change_label = str(metric.get("change_label", "전년대비"))
        pct_label = str(metric.get("pct_change_label", "증감률"))
        include_pct = bool(metric.get("include_pct_change", False))

        block = apply_row_filters(work, metric.get("filters", {}))
        block = block[block["PRD_DE"].isin(years)].copy()
        pv = block.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in pv.columns:
                pv[year] = pd.NA
        pv = pv.reindex(region_order)

        start_vals = pd.to_numeric(pv[years[0]], errors="coerce")
        end_vals = pd.to_numeric(pv[years[1]], errors="coerce")
        result[(label, f"{years[0]}년")] = start_vals
        result[(label, f"{years[1]}년")] = end_vals
        result[(label, change_label)] = (end_vals - start_vals).round(1)

        if include_pct:
            pct_vals = pd.Series(pd.NA, index=pv.index, dtype="object")
            mask = start_vals.notna() & end_vals.notna() & (start_vals != 0)
            pct_vals.loc[mask] = (((end_vals.loc[mask] / start_vals.loc[mask]) - 1) * 100).round(1)
            result[(label, pct_label)] = pct_vals

    result.index = [national_alias if str(x) == national_name else str(x) for x in result.index]
    result.columns = pd.MultiIndex.from_tuples(result.columns)
    return result


def make_rank_and_metric_block_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"rank_and_metric_block summary columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C2_NM")).strip()
    if region_col not in df.columns:
        raise RuntimeError(f"rank_and_metric_block region column missing: {region_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("rank_and_metric_block requires non-empty years")

    rank_metric = pivot_cfg.get("rank_metric", {})
    metric_blocks = pivot_cfg.get("metric_blocks", [])
    if not isinstance(rank_metric, dict) or not isinstance(metric_blocks, list) or not metric_blocks:
        raise RuntimeError("rank_and_metric_block requires rank_metric and metric_blocks")

    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(dict.fromkeys(df[region_col].astype(str)))

    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")
    work = work[work["PRD_DE"].isin(years)].copy()

    out = pd.DataFrame(index=pd.Index(region_order, name=area_label))

    rank_label = str(rank_metric.get("rank_label", "순위"))
    rank_cagr_label = str(rank_metric.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    rank_title = str(rank_metric.get("label", "지표"))
    rank_year = str(rank_metric.get("rank_year", years[-1]))
    rank_block = apply_row_filters(work, rank_metric.get("filters", {}))
    rank_pv = rank_block.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in rank_pv.columns:
            rank_pv[year] = pd.NA
    rank_pv = rank_pv.reindex(region_order)

    periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)
    rank_series = rank_pv.loc[rank_pv.index != national_name, rank_year]
    rank_map = rank_series.rank(method="min", ascending=False)

    for year in years:
        out[(rank_title, f"{year}년")] = rank_pv[year]
    out[(rank_title, rank_label)] = [pd.NA if idx == national_name else int(rank_map.get(idx, 0)) for idx in out.index]

    start_vals = pd.to_numeric(rank_pv[years[0]], errors="coerce")
    end_vals = pd.to_numeric(rank_pv[years[-1]], errors="coerce")
    cagr_vals = pd.Series(pd.NA, index=rank_pv.index, dtype="object")
    mask = start_vals.notna() & end_vals.notna() & (start_vals > 0) & (end_vals > 0)
    cagr_vals.loc[mask] = ((((end_vals.loc[mask] / start_vals.loc[mask]) ** (1 / periods)) - 1) * 100).round(1)
    out[(rank_title, rank_cagr_label)] = cagr_vals.reindex(out.index)

    for block in metric_blocks:
        if not isinstance(block, dict):
            continue
        title = str(block.get("label", "")).strip()
        if not title:
            continue
        cagr_label = str(block.get("cagr_label", rank_cagr_label))
        block_df = apply_row_filters(work, block.get("filters", {}))
        pv = block_df.pivot_table(
            index=region_col,
            columns="PRD_DE",
            values="DT",
            aggfunc="first",
            sort=False,
            observed=True,
        )
        for year in years:
            if year not in pv.columns:
                pv[year] = pd.NA
        pv = pv.reindex(region_order)
        for year in years:
            out[(title, f"{year}년")] = pv[year]
        start_vals = pd.to_numeric(pv[years[0]], errors="coerce")
        end_vals = pd.to_numeric(pv[years[-1]], errors="coerce")
        cagr_vals = pd.Series(pd.NA, index=pv.index, dtype="object")
        mask = start_vals.notna() & end_vals.notna() & (start_vals > 0) & (end_vals > 0)
        cagr_vals.loc[mask] = ((((end_vals.loc[mask] / start_vals.loc[mask]) ** (1 / periods)) - 1) * 100).round(1)
        out[(title, cagr_label)] = cagr_vals.reindex(out.index)

    out.index = [national_alias if str(x) == national_name else str(x) for x in out.index]
    out.columns = pd.MultiIndex.from_tuples(out.columns)
    return out


def make_age_distribution_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"age distribution summary columns missing: {missing}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("age distribution summary requires years")

    age_col = str(pivot_cfg.get("age_col", "C3_NM")).strip()
    age_code_col = str(pivot_cfg.get("age_code_col", "C3")).strip()
    if age_col not in df.columns or age_code_col not in df.columns:
        raise RuntimeError("age distribution summary requires valid age columns")

    work = df.copy()
    work["PRD_DE"] = work["PRD_DE"].astype(str)
    work["DT"] = pd.to_numeric(work["DT"], errors="coerce")
    work = work[work["PRD_DE"].isin(years)].copy()

    detail_filters = pivot_cfg.get("detail_filters", {})
    detail_order = [str(x) for x in pivot_cfg.get("detail_order", [])]
    bucket_defs = pivot_cfg.get("bucket_defs", [])
    if not detail_order or not isinstance(bucket_defs, list) or not bucket_defs:
        raise RuntimeError("age distribution summary requires detail_order and bucket_defs")

    detail_df = apply_row_filters(work, detail_filters)
    detail_pv = detail_df.pivot_table(
        index=age_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )

    left_rows: List[Dict[str, Any]] = []
    for label in detail_order:
        row: Dict[str, Any] = {"구분": label}
        for year in years:
            row[f"{year}년"] = detail_pv.loc[label, year] if label in detail_pv.index and year in detail_pv.columns else pd.NA
        left_rows.append(row)
    left = pd.DataFrame(left_rows, columns=["구분"] + [f"{y}년" for y in years])

    summary_filters = pivot_cfg.get("summary_filters", {})
    summary_df = apply_row_filters(work, summary_filters)
    total_codes = [str(x) for x in pivot_cfg.get("total_codes", [])]
    total_label = str(pivot_cfg.get("total_label", "계"))
    share_year = str(pivot_cfg.get("share_year", years[-1]))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)

    total_block = summary_df[summary_df[age_code_col].astype(str).isin(total_codes)].copy()
    total_pv = total_block.groupby(["PRD_DE"], as_index=False, dropna=False, observed=True)["DT"].sum()
    total_map = {str(r["PRD_DE"]): r["DT"] for _, r in total_pv.iterrows()}

    right_rows: List[Dict[str, Any]] = []
    total_row: Dict[str, Any] = {"요약 구분": total_label}
    for year in years:
        total_row[f"{year}년 인구수"] = total_map.get(year, pd.NA)
    total_row["비중"] = 100.0 if pd.notna(total_map.get(share_year)) else pd.NA
    start_val = pd.to_numeric(total_row.get(f"{years[0]}년 인구수"), errors="coerce")
    end_val = pd.to_numeric(total_row.get(f"{years[-1]}년 인구수"), errors="coerce")
    total_row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
    right_rows.append(total_row)

    total_share_base = pd.to_numeric(total_row.get(f"{share_year}년 인구수"), errors="coerce")
    for bucket in bucket_defs:
        if not isinstance(bucket, dict):
            continue
        label = str(bucket.get("label", "")).strip()
        codes = [str(x) for x in bucket.get("codes", [])]
        if not label or not codes:
            continue
        block = summary_df[summary_df[age_code_col].astype(str).isin(codes)].copy()
        pv = block.groupby(["PRD_DE"], as_index=False, dropna=False, observed=True)["DT"].sum()
        val_map = {str(r["PRD_DE"]): r["DT"] for _, r in pv.iterrows()}
        row: Dict[str, Any] = {"요약 구분": label}
        for year in years:
            row[f"{year}년 인구수"] = val_map.get(year, pd.NA)
        latest_val = pd.to_numeric(row.get(f"{share_year}년 인구수"), errors="coerce")
        row["비중"] = round((latest_val / total_share_base) * 100, 1) if pd.notna(latest_val) and pd.notna(total_share_base) and total_share_base not in (0, 0.0) else pd.NA
        start_val = pd.to_numeric(row.get(f"{years[0]}년 인구수"), errors="coerce")
        end_val = pd.to_numeric(row.get(f"{years[-1]}년 인구수"), errors="coerce")
        row[cagr_label] = round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1) if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0 else pd.NA
        right_rows.append(row)

    right = pd.DataFrame(
        right_rows,
        columns=["요약 구분"] + [f"{y}년 인구수" for y in years] + ["비중", cagr_label],
    )

    max_len = max(len(left), len(right))
    left = left.reindex(range(max_len))
    right = right.reindex(range(max_len))
    spacer = pd.DataFrame({"": [pd.NA] * max_len})
    return pd.concat([left, spacer, right], axis=1)


def make_single_metric_share_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"single_metric_share_summary columns missing: {missing}")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    if region_col not in df.columns:
        raise RuntimeError(f"single_metric_share_summary region column missing: {region_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("single_metric_share_summary requires non-empty years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()

    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[region_col] = d[region_col].astype(str)

    pv = d.pivot_table(
        index=region_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for year in years:
        if year not in pv.columns:
            pv[year] = pd.NA
    pv = pv[years]

    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))
    area_label = str(pivot_cfg.get("area_label", "구분"))
    share_year = str(pivot_cfg.get("share_year", years[-1]))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(years[-1]) - int(years[0]), 1)
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])]
    if not region_order:
        region_order = list(pv.index.astype(str))

    national_val = pd.to_numeric(pv.loc[national_name, share_year], errors="coerce") if national_name in pv.index else pd.NA

    rows: List[Dict[str, Any]] = []
    for region in region_order:
        if region not in pv.index:
            continue
        rec = pv.loc[region]
        row: Dict[str, Any] = {area_label: national_alias if region == national_name else region}
        for year in years:
            row[f"{year}년"] = rec.get(year)

        latest_val = pd.to_numeric(rec.get(share_year), errors="coerce")
        start_val = pd.to_numeric(rec.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(rec.get(years[-1]), errors="coerce")
        row["비중"] = (
            round((latest_val / national_val) * 100, 1)
            if pd.notna(latest_val) and pd.notna(national_val) and national_val not in (0, 0.0)
            else pd.NA
        )
        row[cagr_label] = (
            round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
            else pd.NA
        )
        rows.append(row)

    subtotals = pivot_cfg.get("subtotals", [])
    if not subtotals and pivot_cfg.get("subtotal"):
        subtotals = [pivot_cfg["subtotal"]]
    for subtotal in subtotals:
        members = [str(x) for x in subtotal.get("members", []) if str(x) in pv.index]
        if not members:
            continue
        subtotal_vals = pv.loc[members, years].apply(pd.to_numeric, errors="coerce").sum(axis=0, min_count=1)
        row = {area_label: str(subtotal.get("label", "소계"))}
        for year in years:
            row[f"{year}년"] = subtotal_vals.get(year)
        latest_val = pd.to_numeric(subtotal_vals.get(share_year), errors="coerce")
        start_val = pd.to_numeric(subtotal_vals.get(years[0]), errors="coerce")
        end_val = pd.to_numeric(subtotal_vals.get(years[-1]), errors="coerce")
        row["비중"] = (
            round((latest_val / national_val) * 100, 1)
            if pd.notna(latest_val) and pd.notna(national_val) and national_val not in (0, 0.0)
            else pd.NA
        )
        row[cagr_label] = (
            round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
            else pd.NA
        )
        rows.append(row)

    return pd.DataFrame(rows, columns=[area_label] + [f"{y}년" for y in years] + ["비중", cagr_label])


def make_category_timeseries_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"category_timeseries_summary columns missing: {missing}")

    category_col = str(pivot_cfg.get("category_col", "C2_NM"))
    if category_col not in df.columns:
        raise RuntimeError(f"category_timeseries_summary category column missing: {category_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("category_timeseries_summary requires years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin(years)].copy()

    row_order = [str(x) for x in pivot_cfg.get("category_order", [])]
    if not row_order:
        row_order = list(dict.fromkeys(d[category_col].astype(str)))
    row_label = str(pivot_cfg.get("row_label", "구분"))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)

    pv = d.pivot_table(
        index=category_col,
        columns="PRD_DE",
        values="DT",
        aggfunc="first",
        sort=False,
        observed=True,
    )
    for y in years:
        if y not in pv.columns:
            pv[y] = pd.NA

    out = pd.DataFrame(index=row_order)
    for y in years:
        out[f"{y}년"] = [pv.loc[r, y] if r in pv.index else pd.NA for r in row_order]

    start_vals = pd.to_numeric(out[f"{years[0]}년"], errors="coerce")
    end_vals = pd.to_numeric(out[f"{years[-1]}년"], errors="coerce")
    cagr_vals = pd.Series(pd.NA, index=out.index, dtype="object")
    mask = start_vals.notna() & end_vals.notna() & (start_vals > 0) & (end_vals > 0)
    cagr_vals.loc[mask] = ((((end_vals.loc[mask] / start_vals.loc[mask]) ** (1 / periods)) - 1) * 100).round(1)
    out[cagr_label] = cagr_vals
    out.index.name = row_label
    return out


def make_hierarchy_timeseries_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"hierarchy_timeseries_summary columns missing: {missing}")

    group_col = str(pivot_cfg.get("group_col", "GROUP_NM"))
    detail_col = str(pivot_cfg.get("detail_col", "DETAIL_NM"))
    order_col = str(pivot_cfg.get("order_col", "DISPLAY_ORDER"))
    for c in [group_col, detail_col]:
        if c not in df.columns:
            raise RuntimeError(f"hierarchy_timeseries_summary column missing: {c}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if not years:
        raise RuntimeError("hierarchy_timeseries_summary requires years")

    group_label = str(pivot_cfg.get("group_label", "구분"))
    detail_label = str(pivot_cfg.get("detail_label", "세부"))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(str(years[-1])[:4]) - int(str(years[0])[:4]), 1)

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d = d[d["PRD_DE"].isin(years)].copy()
    if order_col in d.columns:
        d[order_col] = pd.to_numeric(d[order_col], errors="coerce")
    else:
        d[order_col] = pd.NA

    rows: List[Dict[str, Any]] = []
    keys = [group_col, detail_col, order_col]
    dedup = d[keys].drop_duplicates()
    dedup = dedup.sort_values(order_col, kind="stable")
    for _, rec in dedup.iterrows():
        g = str(rec[group_col]) if pd.notna(rec[group_col]) else ""
        det_raw = rec[detail_col]
        det = str(det_raw) if pd.notna(det_raw) else ""
        block = d[(d[group_col].astype(str) == g) & (d[detail_col].fillna("").astype(str) == det)].copy()
        row: Dict[str, Any] = {group_label: g, detail_label: det}
        for y in years:
            val = block[block["PRD_DE"] == y]["DT"]
            row[f"{y}년"] = val.iloc[0] if not val.empty else pd.NA
        start_val = pd.to_numeric(row.get(f"{years[0]}년"), errors="coerce")
        end_val = pd.to_numeric(row.get(f"{years[-1]}년"), errors="coerce")
        row[cagr_label] = (
            round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 1)
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
            else pd.NA
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=[group_label, detail_label] + [f"{y}년" for y in years] + [cagr_label])


def make_category_compare_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"category_compare_summary columns missing: {missing}")

    category_col = str(pivot_cfg.get("category_col", "C2_NM"))
    if category_col not in df.columns:
        raise RuntimeError(f"category_compare_summary category column missing: {category_col}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if len(years) < 2:
        raise RuntimeError("category_compare_summary requires at least 2 years")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")

    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()

    pv = d.pivot_table(index=category_col, columns="PRD_DE", values="DT", aggfunc="first", sort=False, observed=True)
    for y in years:
        if y not in pv.columns:
            pv[y] = pd.NA

    group_col_name = str(pivot_cfg.get("group_col_name", "구분"))
    detail_col_name = str(pivot_cfg.get("detail_col_name", "세부"))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    annual_change_label = str(pivot_cfg.get("annual_change_label", "")).strip()
    annual_change_categories = {str(x) for x in pivot_cfg.get("annual_change_categories", [])}
    periods = max(int(years[-1]) - int(years[0]), 1)

    row_defs = pivot_cfg.get("row_defs", [])
    rows: List[Dict[str, Any]] = []
    for item in row_defs:
        if not isinstance(item, dict):
            continue
        group_val = str(item.get("group", ""))
        detail_val = str(item.get("detail", ""))
        cat = str(item.get("category", ""))
        row: Dict[str, Any] = {group_col_name: group_val, detail_col_name: detail_val}
        for y in years:
            row[f"{y}년"] = pv.loc[cat, y] if cat in pv.index else pd.NA
        start_val = pd.to_numeric(row.get(f"{years[0]}년"), errors="coerce")
        end_val = pd.to_numeric(row.get(f"{years[-1]}년"), errors="coerce")
        row[cagr_label] = (
            round((((end_val / start_val) ** (1 / periods)) - 1) * 100, 2)
            if pd.notna(start_val) and pd.notna(end_val) and start_val > 0 and end_val > 0
            else pd.NA
        )
        if annual_change_label:
            row[annual_change_label] = (
                round((end_val - start_val) / periods, 2)
                if cat in annual_change_categories and pd.notna(start_val) and pd.notna(end_val)
                else pd.NA
            )
        rows.append(row)

    cols = [group_col_name, detail_col_name] + [f"{y}년" for y in years] + [cagr_label]
    if annual_change_label:
        cols.append(annual_change_label)
    return pd.DataFrame(rows, columns=cols)


def make_fertility_latest_compare_summary_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["C1_NM", "ITM_ID", "PRD_DE", "DT"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"fertility_latest_compare_summary columns missing: {missing}")

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d["ITM_ID"] = d["ITM_ID"].astype(str)
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    filters = pivot_cfg.get("filters", {})
    for col, vals in filters.items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()

    birth_item_id = str(pivot_cfg.get("birth_item_id", "T1"))
    fert_item_id = str(pivot_cfg.get("fertility_item_id", "T2"))
    left_year = str(pivot_cfg.get("left_year", "2024"))
    right_year = str(pivot_cfg.get("right_year", "2025"))
    share_year = str(pivot_cfg.get("share_year", right_year))
    yoy_label = str(pivot_cfg.get("yoy_label", "전년대비"))
    share_label = str(pivot_cfg.get("share_label", f"비중('{share_year[2:]})"))
    area_label = str(pivot_cfg.get("area_label", "구분"))
    national_name = str(pivot_cfg.get("national_name", "전국"))
    national_alias = str(pivot_cfg.get("national_alias", "계"))

    pv = d.pivot_table(index="C1_NM", columns=["ITM_ID", "PRD_DE"], values="DT", aggfunc="first", sort=False, observed=True)
    region_order = [str(x) for x in pivot_cfg.get("region_order", [])] or list(dict.fromkeys(d["C1_NM"].astype(str)))
    national_birth = pd.to_numeric(pv.loc[national_name, (birth_item_id, share_year)], errors="coerce") if national_name in pv.index and (birth_item_id, share_year) in pv.columns else pd.NA

    cols = [
        ("출생아수", f"{left_year}년"), ("출생아수", f"{right_year}년"), ("출생아수", share_label), ("출생아수", yoy_label),
        ("합계출산율", f"{left_year}년"), ("합계출산율", f"{right_year}년"), ("합계출산율", yoy_label),
    ]
    out = pd.DataFrame(index=[], columns=pd.MultiIndex.from_tuples(cols), dtype="object")
    for region in region_order:
        if region not in pv.index:
            continue
        name = national_alias if region == national_name else region
        b_left = pd.to_numeric(pv.loc[region, (birth_item_id, left_year)], errors="coerce") if (birth_item_id, left_year) in pv.columns else pd.NA
        b_right = pd.to_numeric(pv.loc[region, (birth_item_id, right_year)], errors="coerce") if (birth_item_id, right_year) in pv.columns else pd.NA
        f_left = pd.to_numeric(pv.loc[region, (fert_item_id, left_year)], errors="coerce") if (fert_item_id, left_year) in pv.columns else pd.NA
        f_right = pd.to_numeric(pv.loc[region, (fert_item_id, right_year)], errors="coerce") if (fert_item_id, right_year) in pv.columns else pd.NA
        out.loc[name, ("출생아수", f"{left_year}년")] = b_left
        out.loc[name, ("출생아수", f"{right_year}년")] = b_right
        out.loc[name, ("출생아수", share_label)] = round((b_right / national_birth) * 100, 1) if pd.notna(b_right) and pd.notna(national_birth) and national_birth not in (0, 0.0) else pd.NA
        out.loc[name, ("출생아수", yoy_label)] = round(((b_right - b_left) / b_left) * 100, 1) if pd.notna(b_left) and pd.notna(b_right) and b_left not in (0, 0.0) else pd.NA
        out.loc[name, ("합계출산율", f"{left_year}년")] = f_left
        out.loc[name, ("합계출산율", f"{right_year}년")] = f_right
        out.loc[name, ("합계출산율", yoy_label)] = round(((f_right - f_left) / f_left) * 100, 1) if pd.notna(f_left) and pd.notna(f_right) and f_left not in (0, 0.0) else pd.NA
    out.index.name = area_label
    return out


def make_age_gender_share_compare_pivot(df: pd.DataFrame, pivot_cfg: dict) -> pd.DataFrame:
    required = ["PRD_DE", "DT", "ITM_ID", "C2_NM"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"age_gender_share_compare columns missing: {missing}")

    years = [str(y) for y in pivot_cfg.get("years", [])]
    if len(years) < 2:
        raise RuntimeError("age_gender_share_compare requires at least 2 years")

    region_col = str(pivot_cfg.get("region_col", "C1_NM"))
    age_col = str(pivot_cfg.get("age_col", "C2_NM"))
    item_col = str(pivot_cfg.get("item_col", "ITM_ID"))
    total_item_id = str(pivot_cfg.get("total_item_id", "T4"))
    male_item_id = str(pivot_cfg.get("male_item_id", "T2"))
    female_item_id = str(pivot_cfg.get("female_item_id", "T3"))
    area_label = str(pivot_cfg.get("area_label", "구분"))
    cagr_label = str(pivot_cfg.get("cagr_label", f"CAGR('{years[0][2:]}~'{years[-1][2:]})"))
    periods = max(int(years[-1]) - int(years[0]), 1)

    d = df.copy()
    d["PRD_DE"] = d["PRD_DE"].astype(str)
    d = d[d["PRD_DE"].isin(years)].copy()
    for col, vals in pivot_cfg.get("filters", {}).items():
        if col in d.columns:
            d = d[d[col].astype(str).isin([str(v) for v in vals])].copy()
    d["DT"] = pd.to_numeric(d["DT"], errors="coerce")
    d[age_col] = d[age_col].astype(str)
    age_order = [str(x) for x in pivot_cfg.get("age_order", [])] or list(dict.fromkeys(d[age_col]))

    year_totals = {}
    for y in years:
        v = d[(d["PRD_DE"] == y) & (d[age_col] == "계") & (d[item_col].astype(str) == total_item_id)]["DT"]
        year_totals[y] = pd.to_numeric(v.iloc[0], errors="coerce") if not v.empty else pd.NA

    rows = []
    for age_name in age_order:
        row = {area_label: age_name}
        for y in years:
            tv = d[(d["PRD_DE"] == y) & (d[age_col] == age_name) & (d[item_col].astype(str) == total_item_id)]["DT"]
            mv = d[(d["PRD_DE"] == y) & (d[age_col] == age_name) & (d[item_col].astype(str) == male_item_id)]["DT"]
            fv = d[(d["PRD_DE"] == y) & (d[age_col] == age_name) & (d[item_col].astype(str) == female_item_id)]["DT"]
            t = pd.to_numeric(tv.iloc[0], errors="coerce") if not tv.empty else pd.NA
            m = pd.to_numeric(mv.iloc[0], errors="coerce") if not mv.empty else pd.NA
            f = pd.to_numeric(fv.iloc[0], errors="coerce") if not fv.empty else pd.NA
            row[f"{y}년 인구수"] = t
            row[f"{y}년 성비"] = round((m / f) * 100, 1) if pd.notna(m) and pd.notna(f) and f not in (0, 0.0) else pd.NA
            base = pd.to_numeric(year_totals.get(y), errors="coerce")
            row[f"{y}년 비중"] = round((t / base) * 100, 1) if pd.notna(t) and pd.notna(base) and base not in (0, 0.0) else pd.NA
        s = pd.to_numeric(row.get(f"{years[0]}년 인구수"), errors="coerce")
        e = pd.to_numeric(row.get(f"{years[-1]}년 인구수"), errors="coerce")
        row[cagr_label] = round((((e / s) ** (1 / periods)) - 1) * 100, 1) if pd.notna(s) and pd.notna(e) and s > 0 and e > 0 else pd.NA
        rows.append(row)

    cols = [area_label]
    for y in years:
        cols += [f"{y}년 인구수", f"{y}년 성비", f"{y}년 비중"]
    cols += [cagr_label]
    return pd.DataFrame(rows, columns=cols)


def flatten_for_block(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [
            " ".join(str(part) for part in col if str(part) not in ("", "None")).strip()
            for col in out.columns
        ]
    else:
        out.columns = [str(c) for c in out.columns]
    return out


def substitute_template(value: Any, mapping: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") and text.endswith("}") and len(text) > 2:
            key = text[1:-1]
            if key in mapping:
                return mapping[key]
        out = value
        for k, v in mapping.items():
            out = out.replace("{" + str(k) + "}", str(v))
        return out
    if isinstance(value, list):
        return [substitute_template(v, mapping) for v in value]
    if isinstance(value, dict):
        return {k: substitute_template(v, mapping) for k, v in value.items()}
    return value


def build_single_source_view(df: pd.DataFrame, spec: dict) -> pd.DataFrame:
    d = apply_row_filters(df, spec.get("filters", {}))
    if isinstance(spec.get("preprocess"), dict):
        d = apply_preprocess(d, {"preprocess": spec["preprocess"]})
    d = apply_value_maps(d, spec)
    kind = str(spec.get("kind", "pivot")).strip().lower()

    if kind == "pivot":
        return make_custom_pivot(d, spec)
    if kind == "sum_pivot":
        group_cols = spec.get("groupby", [])
        if not isinstance(group_cols, list) or not group_cols:
            raise RuntimeError("sum_pivot requires non-empty groupby")
        work = d.copy()
        work["DT"] = pd.to_numeric(work["DT"], errors="coerce").fillna(0)
        work = work.groupby(group_cols, as_index=False, dropna=False, observed=True)["DT"].sum()
        pivot_spec = dict(spec)
        pivot_spec.pop("filters", None)
        pivot_spec.pop("groupby", None)
        pivot_spec.pop("replace_values", None)
        return make_custom_pivot(work, pivot_spec)
    if kind == "rank_timeseries":
        return make_rank_timeseries_pivot(d, spec)
    if kind == "ratio_timeseries":
        return make_ratio_timeseries_pivot(d, spec)
    if kind == "metric_summary":
        return make_metric_summary_pivot(d, spec)
    if kind == "metric_block_summary":
        return make_metric_block_summary_pivot(d, spec)
    if kind == "year_gender_mix":
        return make_year_gender_mix_pivot(d, spec)
    if kind == "latest_profile_summary":
        return make_latest_profile_summary_pivot(d, spec)
    if kind == "timeseries_profile_summary":
        return make_timeseries_profile_summary_pivot(d, spec)
    if kind == "latest_rank":
        return make_latest_rank_pivot(d, spec)
    if kind == "paired_metric_timeseries_summary":
        return make_paired_metric_timeseries_summary_pivot(d, spec)
    if kind == "paired_metric_latest_compare":
        return make_paired_metric_latest_compare_pivot(d, spec)
    if kind == "rank_and_metric_block_summary":
        return make_rank_and_metric_block_summary_pivot(d, spec)
    if kind == "age_distribution_summary":
        return make_age_distribution_summary_pivot(d, spec)
    if kind == "category_timeseries_summary":
        return make_category_timeseries_summary_pivot(d, spec)
    if kind == "hierarchy_timeseries_summary":
        return make_hierarchy_timeseries_summary_pivot(d, spec)
    if kind == "single_metric_share_summary":
        return make_single_metric_share_summary_pivot(d, spec)
    if kind == "category_compare_summary":
        return make_category_compare_summary_pivot(d, spec)
    if kind == "fertility_latest_compare_summary":
        return make_fertility_latest_compare_summary_pivot(d, spec)
    if kind == "age_gender_share_compare":
        return make_age_gender_share_compare_pivot(d, spec)
    raise RuntimeError(f"unknown view kind: {kind}")


def make_stack_blocks_view(source_frames: Dict[str, pd.DataFrame], spec: dict) -> pd.DataFrame:
    blocks = spec.get("blocks", [])
    if not isinstance(blocks, list) or not blocks:
        raise RuntimeError("stack_blocks requires non-empty blocks")

    rendered: List[pd.DataFrame] = []
    for i, block in enumerate(blocks, start=1):
        if not isinstance(block, dict):
            continue
        src_name = str(block.get("source", "")).strip()
        if not src_name or src_name not in source_frames:
            raise RuntimeError(f"stack_blocks source missing: {src_name}")

        block_df = build_single_source_view(source_frames[src_name].copy(), block)
        if bool(block.get("flatten", True)):
            block_df = flatten_for_block(block_df)

        title = str(block.get("title", "")).strip()
        if title:
            first_col = block_df.columns[0] if len(block_df.columns) else "구분"
            rendered.append(pd.DataFrame([{first_col: title}]))

        rendered.append(block_df)

        blank_rows = int(block.get("blank_rows", 1))
        if blank_rows > 0 and i < len(blocks):
            rendered.append(pd.DataFrame([{} for _ in range(blank_rows)]))

    if not rendered:
        raise RuntimeError("stack_blocks produced no blocks")

    return pd.concat(rendered, ignore_index=True, sort=False)


def build_source_views(source_frames: Dict[str, pd.DataFrame], job: dict) -> Dict[str, pd.DataFrame]:
    views: Dict[str, pd.DataFrame] = {}
    specs = job.get("views", [])
    if not isinstance(specs, list) or not specs:
        return views

    expanded_specs: List[dict] = []
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        repeat_cfg = spec.get("repeat_over")
        if isinstance(repeat_cfg, dict) and isinstance(repeat_cfg.get("items"), list):
            base = dict(spec)
            base.pop("repeat_over", None)
            for item in repeat_cfg.get("items", []):
                if not isinstance(item, dict):
                    continue
                expanded = substitute_template(base, {str(k): v for k, v in item.items()})
                if isinstance(expanded, dict):
                    expanded_specs.append(expanded)
        else:
            expanded_specs.append(spec)

    for i, spec in enumerate(expanded_specs, start=1):
        if not isinstance(spec, dict):
            continue
        sheet_name = str(spec.get("sheet_name", f"TABLE_VIEW_{i}")).strip() or f"TABLE_VIEW_{i}"
        kind = str(spec.get("kind", "pivot")).strip().lower()

        try:
            if kind == "stack_blocks":
                views[sheet_name] = make_stack_blocks_view(source_frames, spec)
            else:
                src_names = []
                if isinstance(spec.get("sources"), list):
                    src_names = [str(x).strip() for x in spec.get("sources", []) if str(x).strip()]
                else:
                    src_name = str(spec.get("source", "")).strip()
                    if src_name:
                        src_names = [src_name]

                if not src_names:
                    print("[WARN] view source missing")
                    continue

                missing = [name for name in src_names if name not in source_frames]
                if missing:
                    print(f"[WARN] view source missing: {', '.join(missing)}")
                    continue

                base_df = pd.concat([source_frames[name].copy() for name in src_names], ignore_index=True)
                views[sheet_name] = build_single_source_view(base_df, spec)
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
                elif kind == "category_compare_summary":
                    views[sheet_name] = make_category_compare_summary_pivot(df, cfg)
                elif kind == "fertility_latest_compare_summary":
                    views[sheet_name] = make_fertility_latest_compare_summary_pivot(df, cfg)
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

    if job_like.get("startPrdDe") is not None and str(job_like.get("startPrdDe")).strip() != "":
        params["startPrdDe"] = str(job_like["startPrdDe"]).strip()

    if job_like.get("endPrdDe") is not None and str(job_like.get("endPrdDe")).strip() != "":
        params["endPrdDe"] = str(job_like["endPrdDe"]).strip()

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

    raw_out: Any = {} if not bool(job.get("include_source_raw", True)) else src_raw_frames
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
        df.to_excel(writer, sheet_name=normalize_sheet_name(target_sheet), index=use_index, merge_cells=True)

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
                    if not wrote and pivot_df is None:
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

