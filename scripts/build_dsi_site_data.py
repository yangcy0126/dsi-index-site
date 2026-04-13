from __future__ import annotations

import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


SITE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SITE_ROOT.parent
SITE_DATA_DIR = SITE_ROOT / "data"
THREE_CATEGORY_DIR = WORKSPACE_ROOT / "DSI-ICF" / "data" / "三类指数结果_扩展15国"
SCORES_DIR = WORKSPACE_ROOT / "DSI-ICF" / "data" / "情绪测度结果数据_扩展15国"

ALL_COUNTRIES_DAILY_PATH = THREE_CATEGORY_DIR / "all_countries_daily.csv"
SUMMARY_PATH = THREE_CATEGORY_DIR / "summary.csv"

INDICATORS = {
    "c1": {
        "slug": "wdsi",
        "short_label": "WDSI",
        "label": "War-Related DSI",
        "description": "War-related diplomatic sentiment built from the military-security dimension.",
        "chart_title": "WDSI",
    },
    "c2": {
        "slug": "edsi",
        "short_label": "EDSI",
        "label": "Economic DSI",
        "description": "Economic diplomatic sentiment built from the economic dimension.",
        "chart_title": "Economic DSI",
    },
    "c3": {
        "slug": "odsi",
        "short_label": "ODSI",
        "label": "Other DSI",
        "description": "Other diplomatic sentiment built from the residual non-military, non-economic dimension.",
        "chart_title": "Other DSI",
    },
}

COUNTRY_ORDER = ["US", "CN", "DE", "JP", "IN", "UK", "FR", "IT", "CA", "BR", "RU", "KR", "MX", "AU", "ES"]

COUNTRY_META = {
    "US": {"label": "United States", "color": "#b85f35"},
    "CN": {"label": "China", "color": "#0f6c74"},
    "DE": {"label": "Germany", "color": "#1f3f5b"},
    "JP": {"label": "Japan", "color": "#7851a9"},
    "IN": {"label": "India", "color": "#d36a24"},
    "UK": {"label": "United Kingdom", "color": "#5c7c5a"},
    "FR": {"label": "France", "color": "#2f5aa8"},
    "IT": {"label": "Italy", "color": "#3f7562"},
    "CA": {"label": "Canada", "color": "#b24d4d"},
    "BR": {"label": "Brazil", "color": "#4d7a3e"},
    "RU": {"label": "Russia", "color": "#a24a3f"},
    "KR": {"label": "South Korea", "color": "#9a6b2f"},
    "MX": {"label": "Mexico", "color": "#2f7a70"},
    "AU": {"label": "Australia", "color": "#6c5b9a"},
    "ES": {"label": "Spain", "color": "#c37b2d"},
}

COUNTRY_WORKBOOK_COLUMNS = [
    "date",
    "publication",
    "raw",
    "rolling7",
    "rolling30",
    "c1_raw",
    "c2_raw",
    "c3_raw",
    "c1",
    "c2",
    "c3",
    "c1_3",
    "c2_3",
    "c3_3",
    "c1_7",
    "c2_7",
    "c3_7",
    "c1_30",
    "c2_30",
    "c3_30",
]

MASTER_WORKBOOK_COLUMNS = ["country_code", "country"] + COUNTRY_WORKBOOK_COLUMNS

INDICATOR_PANEL_COLUMNS = ["country_code", "country", "date", "publication", "raw", "filled", "ma3", "ma7", "ma30"]

VARIABLE_DEFINITIONS = [
    {
        "variable": "date",
        "description": "Calendar date of the daily DSI observation.",
    },
    {
        "variable": "publication",
        "description": "Whether at least one official source publication is observed on that date.",
    },
    {
        "variable": "raw",
        "description": "Backward-compatible alias for c1_raw, the WDSI publication-day raw score.",
    },
    {
        "variable": "rolling7",
        "description": "Backward-compatible alias for c1_7, the 7-day smoothed WDSI.",
    },
    {
        "variable": "rolling30",
        "description": "Backward-compatible alias for c1_30, the 30-day smoothed WDSI.",
    },
    {
        "variable": "c1_raw / c2_raw / c3_raw",
        "description": "Publication-day raw scores for the war-related, economic, and other diplomatic sentiment dimensions.",
    },
    {
        "variable": "c1 / c2 / c3",
        "description": "Forward-filled daily paths for the three DSI dimensions.",
    },
    {
        "variable": "c1_3 / c2_3 / c3_3",
        "description": "3-day rolling means on the forward-filled daily paths.",
    },
    {
        "variable": "c1_7 / c2_7 / c3_7",
        "description": "7-day rolling means on the forward-filled daily paths.",
    },
    {
        "variable": "c1_30 / c2_30 / c3_30",
        "description": "30-day rolling means on the forward-filled daily paths.",
    },
]

METHOD_NOTE = (
    "Diplomatic Sentiment Observatory (DSI) site build: each publication-day raw score uses the same-day minimum; "
    "missing dates are forward-filled; 3-day, 7-day, and 30-day series are rolling means on the filled daily path. "
    "WDSI is the c1 war-related dimension within the broader three-part DSI family."
)


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def clean_number(value: object, digits: int = 3) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    if math.isclose(numeric, round(numeric), abs_tol=1e-9):
        return int(round(numeric))
    return round(numeric, digits)


def sanitize_record(record: dict[str, object]) -> dict[str, object]:
    cleaned: dict[str, object] = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = None
        elif isinstance(value, (pd.Timestamp, datetime)):
            cleaned[key] = value.strftime("%Y-%m-%d")
        elif key == "publication":
            cleaned[key] = to_bool(value)
        elif isinstance(value, float):
            cleaned[key] = clean_number(value)
        else:
            cleaned[key] = value
    return cleaned


def compute_change(series: pd.Series, lookback_days: int) -> float | int | None:
    if len(series) <= lookback_days:
        return None
    current = clean_number(series.iloc[-1], digits=6)
    previous = clean_number(series.iloc[-1 - lookback_days], digits=6)
    if current is None or previous is None:
        return None
    return clean_number(float(current) - float(previous))


def build_latest_publication_lookup() -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for code in COUNTRY_ORDER:
        path = SCORES_DIR / f"{code}_scores.csv"
        if not path.exists():
            continue
        frame = pd.read_csv(
            path,
            usecols=["published_at", "title", "url", "score_status"],
            dtype=str,
            keep_default_na=False,
            low_memory=False,
        )
        frame = frame[frame["score_status"].eq("accepted")]
        frame = frame[frame["published_at"].ne("")]
        if frame.empty:
            continue
        frame["published_at"] = frame["published_at"].str.slice(0, 10)
        frame = frame.sort_values(["published_at"]).reset_index(drop=True)
        latest = frame.iloc[-1]
        lookup[code] = {
            "latest_publication_date": latest["published_at"],
            "latest_title": latest.get("title", ""),
            "latest_url": latest.get("url", ""),
        }
    return lookup


def write_workbook(path: Path, sheet_name: str, frame: pd.DataFrame) -> None:
    definitions = pd.DataFrame(VARIABLE_DEFINITIONS)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name=sheet_name)
        definitions.to_excel(writer, index=False, sheet_name="variable_definitions")


def write_indicator_workbook(path: Path, sheet_name: str, frame: pd.DataFrame, indicator_label: str) -> None:
    definitions = pd.DataFrame(
        [
            {"variable": "country_code", "description": "Two-letter country or region code."},
            {"variable": "country", "description": "Country or region name."},
            {"variable": "date", "description": "Calendar date of the daily observation."},
            {"variable": "publication", "description": "Whether at least one official source publication is observed on that date."},
            {"variable": "raw", "description": f"Publication-day raw score for {indicator_label}."},
            {"variable": "filled", "description": f"Forward-filled daily path for {indicator_label}."},
            {"variable": "ma3", "description": f"3-day rolling mean for {indicator_label}."},
            {"variable": "ma7", "description": f"7-day rolling mean for {indicator_label}."},
            {"variable": "ma30", "description": f"30-day rolling mean for {indicator_label}."},
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name=sheet_name)
        definitions.to_excel(writer, index=False, sheet_name="variable_definitions")


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not ALL_COUNTRIES_DAILY_PATH.exists():
        raise FileNotFoundError(f"Missing daily panel: {ALL_COUNTRIES_DAILY_PATH}")
    if not SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Missing summary file: {SUMMARY_PATH}")

    daily = pd.read_csv(ALL_COUNTRIES_DAILY_PATH, low_memory=False)
    summary = pd.read_csv(SUMMARY_PATH, low_memory=False)

    daily = daily.rename(columns={"time": "date"})
    daily["date"] = pd.to_datetime(daily["date"]).dt.strftime("%Y-%m-%d")
    daily["publication"] = daily["publication"].map(to_bool)
    for column in [
        "c1_raw",
        "c2_raw",
        "c3_raw",
        "c1",
        "c2",
        "c3",
        "c1_3",
        "c2_3",
        "c3_3",
        "c1_7",
        "c2_7",
        "c3_7",
        "c1_30",
        "c2_30",
        "c3_30",
    ]:
        daily[column] = pd.to_numeric(daily[column], errors="coerce")

    return daily, summary


def build_country_payload(
    code: str,
    frame: pd.DataFrame,
    latest_lookup: dict[str, dict[str, str]],
) -> tuple[dict[str, object], pd.DataFrame, list[dict[str, object]]]:
    meta = COUNTRY_META[code]
    records = frame.sort_values("date").reset_index(drop=True).copy()
    records["raw"] = records["c1_raw"]
    records["rolling7"] = records["c1_7"]
    records["rolling30"] = records["c1_30"]
    records = records[COUNTRY_WORKBOOK_COLUMNS].copy()

    publication_dates = frame.loc[frame["publication"], "date"].tolist()
    latest_publication_date = publication_dates[-1] if publication_dates else None
    latest_meta = latest_lookup.get(code, {})
    if latest_publication_date and latest_meta.get("latest_publication_date") != latest_publication_date:
        latest_meta = latest_meta.copy()
        latest_meta["latest_publication_date"] = latest_publication_date

    country_summary: dict[str, object] = {
        "code": code,
        "label": meta["label"],
        "label_zh": meta["label"],
        "color": meta["color"],
        "start_date": records["date"].iloc[0],
        "latest_date": records["date"].iloc[-1],
        "latest_publication_date": latest_publication_date,
        "publication_days": int(frame["publication"].sum()),
        "calendar_days": int(len(records)),
        "latest_title": latest_meta.get("latest_title", ""),
        "latest_url": latest_meta.get("latest_url", ""),
        "data_source": "dsi_icf_three_category",
        "file_json": f"data/{code}.json",
        "file_csv": f"data/{code}.csv",
        "file_xlsx": f"data/{code}.xlsx",
        "is_placeholder": False,
        "placeholder_note": "",
        "indicators": {},
    }

    current_year = int(records["date"].iloc[-1][:4])
    year_mask = records["date"].str.startswith(f"{current_year}-")

    for indicator, indicator_meta in INDICATORS.items():
        raw_col = f"{indicator}_raw"
        series_col = indicator
        rolling7_col = f"{indicator}_7"
        rolling30_col = f"{indicator}_30"

        latest_raw_value = None
        publication_rows = frame.loc[frame["publication"] & frame[raw_col].notna(), raw_col]
        if not publication_rows.empty:
            latest_raw_value = clean_number(publication_rows.iloc[-1])

        latest_filled_value = clean_number(frame[series_col].iloc[-1])
        latest_7_value = clean_number(frame[rolling7_col].iloc[-1])
        latest_30_value = clean_number(frame[rolling30_col].iloc[-1])
        current_year_mean = clean_number(records.loc[year_mask, series_col].mean())

        indicator_summary = {
            "slug": indicator_meta["slug"],
            "short_label": indicator_meta["short_label"],
            "label": indicator_meta["label"],
            "latest_raw": latest_raw_value,
            "latest_filled": latest_filled_value,
            "latest_7d": latest_7_value,
            "latest_30d": latest_30_value,
            "change_7d": compute_change(frame[rolling7_col], 7),
            "change_30d": compute_change(frame[rolling30_col], 30),
            "current_year": current_year,
            "current_year_mean": current_year_mean,
        }
        country_summary["indicators"][indicator] = indicator_summary

    # Backward-compatible WDSI aliases for the old front-end and supplements.
    c1_summary = country_summary["indicators"]["c1"]
    country_summary["latest_raw"] = c1_summary["latest_raw"]
    country_summary["latest_7d"] = c1_summary["latest_7d"]
    country_summary["latest_30d"] = c1_summary["latest_30d"]
    country_summary["change_7d"] = c1_summary["change_7d"]
    country_summary["change_30d"] = c1_summary["change_30d"]
    country_summary["current_year"] = c1_summary["current_year"]
    country_summary["current_year_mean"] = c1_summary["current_year_mean"]

    records_for_json = [sanitize_record(record) for record in records.to_dict(orient="records")]
    return country_summary, records, records_for_json


def build_indicator_panel(master_frame: pd.DataFrame, indicator: str) -> pd.DataFrame:
    panel = pd.DataFrame(
        {
            "country_code": master_frame["country_code"],
            "country": master_frame["country"],
            "date": master_frame["date"],
            "publication": master_frame["publication"],
            "raw": master_frame[f"{indicator}_raw"],
            "filled": master_frame[indicator],
            "ma3": master_frame[f"{indicator}_3"],
            "ma7": master_frame[f"{indicator}_7"],
            "ma30": master_frame[f"{indicator}_30"],
        }
    )
    return panel[INDICATOR_PANEL_COLUMNS]


def main() -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    daily, _ = load_inputs()
    latest_lookup = build_latest_publication_lookup()

    countries: list[dict[str, object]] = []
    master_frames: list[pd.DataFrame] = []

    for code in COUNTRY_ORDER:
        country_frame = daily[daily["country_code"].eq(code)].copy()
        if country_frame.empty:
            continue

        country_summary, country_records, country_records_json = build_country_payload(code, country_frame, latest_lookup)
        countries.append(country_summary)

        json_payload = {
            "code": code,
            "label": country_summary["label"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "is_placeholder": False,
            "placeholder_note": "",
            "indicators": INDICATORS,
            "records": country_records_json,
        }
        (SITE_DATA_DIR / f"{code}.json").write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        country_records.to_csv(SITE_DATA_DIR / f"{code}.csv", index=False, encoding="utf-8-sig")
        write_workbook(SITE_DATA_DIR / f"{code}.xlsx", code, country_records)

        master_country = country_records.copy()
        master_country.insert(1, "country", country_summary["label"])
        master_country.insert(0, "country_code", code)
        master_frames.append(master_country)

    if not countries:
        raise RuntimeError("No country data found for site build.")

    overall = {
        "country_count": len(countries),
        "live_country_count": len(countries),
        "placeholder_count": 0,
        "indicator_count": len(INDICATORS),
        "first_date": min(country["start_date"] for country in countries),
        "last_date": max(country["latest_date"] for country in countries),
    }

    summary_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "site_title": "Diplomatic Sentiment Observatory",
        "site_short_name": "DSI Observatory",
        "method_note": METHOD_NOTE,
        "indicators": INDICATORS,
        "downloads": {
            "dsi_xlsx": "data/dsi_all_countries.xlsx",
            "dsi_csv": "data/dsi_all_countries.csv",
            "wdsi_xlsx": "data/wdsi_all_countries.xlsx",
            "wdsi_csv": "data/wdsi_all_countries.csv",
            "edsi_xlsx": "data/edsi_all_countries.xlsx",
            "edsi_csv": "data/edsi_all_countries.csv",
            "odsi_xlsx": "data/odsi_all_countries.xlsx",
            "odsi_csv": "data/odsi_all_countries.csv",
            "summary_json": "data/summary.json",
        },
        "overall": overall,
        "countries": countries,
    }

    (SITE_DATA_DIR / "summary.json").write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_frame = pd.DataFrame(countries)
    summary_frame.to_csv(SITE_DATA_DIR / "summary.csv", index=False, encoding="utf-8-sig")
    write_workbook(SITE_DATA_DIR / "summary.xlsx", "summary", summary_frame)

    master_frame = pd.concat(master_frames, ignore_index=True)
    master_frame = master_frame[MASTER_WORKBOOK_COLUMNS]
    master_frame.to_csv(SITE_DATA_DIR / "dsi_all_countries.csv", index=False, encoding="utf-8-sig")
    write_workbook(SITE_DATA_DIR / "dsi_all_countries.xlsx", "daily_panel", master_frame)

    for indicator, indicator_meta in INDICATORS.items():
        indicator_panel = build_indicator_panel(master_frame, indicator)
        file_slug = indicator_meta["slug"]
        indicator_panel.to_csv(SITE_DATA_DIR / f"{file_slug}_all_countries.csv", index=False, encoding="utf-8-sig")
        write_indicator_workbook(
            SITE_DATA_DIR / f"{file_slug}_all_countries.xlsx",
            f"{file_slug}_panel",
            indicator_panel,
            indicator_meta["short_label"],
        )


if __name__ == "__main__":
    main()
