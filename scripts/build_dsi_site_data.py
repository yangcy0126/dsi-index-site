from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from wdsi_series import INPUTS, daily_indices, frozen_countrydays, paper_documents, period_means

SITE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SITE_ROOT.parent
SITE_DATA_DIR = SITE_ROOT / "data"
THREE_CATEGORY_DIR = WORKSPACE_ROOT / "DSI-ICF" / "data" / "三类指数结果_扩展15国"
SCORES_DIR = WORKSPACE_ROOT / "DSI-ICF" / "data" / "情绪测度结果数据_扩展15国"

ALL_COUNTRIES_DAILY_PATH = THREE_CATEGORY_DIR / "all_countries_daily.csv"

INDICATORS = {
    "c1": {
        "slug": "wdsi",
        "short_label": "WDSI",
        "label": "War-Related DSI",
        "description": "War-related diplomatic sentiment. Higher is more adverse. Each scored document in the trailing 14 calendar days has equal weight.",
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

COUNTRY_SITE_START_OVERRIDES = {}

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
    "rolling14",
    "c1_14",
    "doc_count",
    "doc_sum",
]

MASTER_WORKBOOK_COLUMNS = ["country_code", "country"] + COUNTRY_WORKBOOK_COLUMNS

VARIABLE_DEFINITIONS = [
    {
        "variable": "date",
        "description": "Calendar date of the daily DSI observation.",
    },
    {
        "variable": "publication",
        "description": "Publication flag in the reviewed three-branch calendar. For scored WDSI documents on this date, use doc_count > 0.",
    },
    {
        "variable": "raw",
        "description": "Backward-compatible alias for c1_raw, the c1 publication-day raw score (war-related DSI / WDSI).",
    },
    {
        "variable": "rolling7",
        "description": "Backward-compatible alias for c1_7, the 7-day smoothed c1 series (war-related DSI / WDSI).",
    },
    {
        "variable": "rolling30",
        "description": "Backward-compatible alias for c1_30, the 30-day smoothed c1 series (war-related DSI / WDSI).",
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
        "description": "Trailing 3 days: mean across documents for WDSI; mean of carried daily scores for EDSI and ODSI.",
    },
    {
        "variable": "c1_7 / c2_7 / c3_7",
        "description": "Trailing 7 days: mean across documents for WDSI; mean of carried daily scores for EDSI and ODSI.",
    },
    {
        "variable": "c1_30 / c2_30 / c3_30",
        "description": "Trailing 30 days: mean across documents for WDSI; mean of carried daily scores for EDSI and ODSI.",
    },
    {"variable": "doc_count / doc_sum", "description": "Number and adverse-positive score sum of accepted WDSI documents on this date. Includes zero scores. Both are zero on days without scored documents."},
    {"variable": "c1_raw / c1", "description": "WDSI raw is the mean of scored documents on a publication date; c1 is its carried daily path for reference, not the input to window averages. +3 is most adverse, -3 most conciliatory."},
    {"variable": "rolling14 / c1_14 / ma14", "description": "Site baseline: total doc_sum divided by total doc_count over 14 calendar days including today. Carry the last valid mean only across wholly empty windows. First 13 dates are blank."},
    {"variable": "wdsi14_document", "description": "Month or quarter mean of available daily 14-day document-weighted WDSI values. Quarters weight days, not months. The frozen paper international file uses wdsi14_countryday instead."},
    {"variable": "signal_days / required_days / full_signal_period", "description": "Valid daily 14-day values / calendar days in the full period / whether these counts match. No data after the last observed country date."},
]

METHOD_NOTE = (
    "All 15 countries: each accepted scored document in the trailing 14 calendar days has equal weight, including zero scores. "
    "Higher WDSI means more adverse rhetoric. Monthly and quarterly values average the available daily indices. "
    "The paper's international country-day series is preserved separately in the replication files. "
    "EDSI and ODSI retain their original direction and windows."
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
            cleaned[key] = clean_number(value, 15 if key == "raw" or key.startswith(("c1", "rolling")) else 3)
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


def write_indicator_workbook(path: Path, sheet_name: str, frame: pd.DataFrame, indicator_label: str, periods=None) -> None:
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
    if periods is not None:
        definitions.loc[definitions.variable.eq("publication"), "description"] = "At least one accepted WDSI document on the date (doc_count > 0)."
        for n in (3, 7, 30):
            definitions.loc[definitions.variable.eq(f"ma{n}"), "description"] = f"Equal weight per scored document in the trailing {n} calendar days, including today."
        definitions = pd.concat([definitions, pd.DataFrame(VARIABLE_DEFINITIONS[-5:])], ignore_index=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name=sheet_name)
        definitions.to_excel(writer, index=False, sheet_name="variable_definitions")
        for name, table in (periods or {}).items():
            table.to_excel(writer, index=False, sheet_name=name)


def load_inputs():
    daily = pd.read_csv(INPUTS / "reviewed_daily.csv.gz")
    if set(daily.country_code) != set(COUNTRY_ORDER):
        raise ValueError("Reviewed input must cover all 15 countries")
    for code, g in daily.groupby("country_code"):
        records = pd.read_csv(SITE_ROOT / "records" / f"{code}.csv", usecols=["published_at"])
        if records.published_at.max()[:10] > g.date.max():
            raise ValueError(f"{code}: newer records exist. Refresh the reviewed three-branch input with import_wdsi_inputs.py before publishing.")
    return pd.concat([daily_indices(g) for _, g in daily.groupby("country_code")], ignore_index=True)


def build_country_payload(
    code: str,
    frame: pd.DataFrame,
    latest_lookup: dict[str, dict[str, str]],
) -> tuple[dict[str, object], pd.DataFrame, list[dict[str, object]]]:
    meta = COUNTRY_META[code]
    records = frame.sort_values("date").reset_index(drop=True).copy()
    records["raw"] = records["c1_raw"]
    records["rolling7"] = records["c1_7"]
    records["rolling14"] = records["c1_14"]
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
        current_year_mean = clean_number(records.loc[year_mask, "c1_14" if indicator == "c1" else series_col].mean())

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
        if indicator == "c1":
            indicator_summary["latest_14d"] = clean_number(frame["c1_14"].iloc[-1], 15)
            scored_dates = frame.loc[frame.doc_count.gt(0), "date"]
            indicator_summary["publication_days"] = len(scored_dates)
            indicator_summary["latest_publication_date"] = scored_dates.max()

    # Backward-compatible WDSI aliases for the old front-end and supplements.
    c1_summary = country_summary["indicators"]["c1"]
    country_summary["latest_14d"] = c1_summary["latest_14d"]
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
    if indicator == "c1":
        panel["publication"] = master_frame.doc_count.gt(0)
        panel["ma14"] = master_frame["c1_14"]
        panel[["doc_count", "doc_sum"]] = master_frame[["doc_count", "doc_sum"]]
    return panel


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-paper", action="store_true", help="Also rebuild the frozen paper replication exports.")
    args = parser.parse_args()
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    daily = load_inputs()
    latest_lookup = json.loads((INPUTS / "latest_publications.json").read_text(encoding="utf-8"))

    countries: list[dict[str, object]] = []
    master_frames: list[pd.DataFrame] = []

    for code in COUNTRY_ORDER:
        country_frame = daily[daily["country_code"].eq(code)].copy()
        if country_frame.empty:
            continue
        start_override = COUNTRY_SITE_START_OVERRIDES.get(code)
        if start_override:
            country_frame = country_frame[country_frame["date"].ge(start_override)].copy()
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
            "wdsi_site_periods": "data/wdsi_site_periods.csv",
            "wdsi_paper_periods": "data/wdsi_paper_periods.csv",
            "wdsi_replication": "data/wdsi_paper_replication.xlsx",
            "wdsi_weighting_comparison": "data/wdsi_weighting_comparison_20261119.zip",
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

    site_periods = period_means(master_frame, "wdsi14_document")
    site_periods.to_csv(SITE_DATA_DIR / "wdsi_site_periods.csv", index=False, encoding="utf-8-sig")
    if args.include_paper or not all((SITE_DATA_DIR / name).is_file() for name in ("wdsi_paper_periods.csv", "wdsi_paper_replication.xlsx")):
        write_frozen_paper_exports()
    for indicator, indicator_meta in INDICATORS.items():
        indicator_panel = build_indicator_panel(master_frame, indicator)
        file_slug = indicator_meta["slug"]
        if indicator != "c1" and (SITE_DATA_DIR / f"{file_slug}_all_countries.xlsx").is_file():
            path = SITE_DATA_DIR / f"{file_slug}_all_countries.csv"
            if path.is_file():
                try:
                    pd.testing.assert_frame_equal(pd.read_csv(path), indicator_panel, atol=1e-14, rtol=0)
                except AssertionError:
                    pass
                else:
                    continue
        indicator_panel.to_csv(SITE_DATA_DIR / f"{file_slug}_all_countries.csv", index=False, encoding="utf-8-sig")
        write_indicator_workbook(
            SITE_DATA_DIR / f"{file_slug}_all_countries.xlsx",
            f"{file_slug}_panel",
            indicator_panel,
            indicator_meta["short_label"],
            {"site_periods": site_periods} if indicator == "c1" else None,
        )


def write_frozen_paper_exports():
    frozen = frozen_countrydays()
    paper_periods = period_means(frozen, "wdsi14_countryday")
    paper_periods.to_csv(SITE_DATA_DIR / "wdsi_paper_periods.csv", index=False, encoding="utf-8-sig")
    docs = paper_documents(pd.read_csv(INPUTS / "paper_document_days.csv.gz"))
    notes = [
        ("international_daily", "Frozen country-day series for 15 economies. Higher WDSI is more adverse. The Korean regression source is the archived k1 series; other countries use the shared archived country-day input."),
        ("international_periods", "Monthly and quarterly means of the available daily c1_14 values. Counts and full_signal_period distinguish complete periods. These are signal panels, not outcome-specific financial regression samples."),
        ("stock_connect_daily", "Figure 2 / Table 4: historical document-weighted WDSI on the paper plotting calendar. The paper describes ten official Chinese source categories. Warm-up uses the available earlier history."),
        ("doc_count / doc_sum", "Daily number of accepted source records, including non-war zeros, and sum of adverse-positive scores. Both zero on days without records."),
        ("wdsi14_document", "Sum of scores / number of records in the trailing 14 calendar days, carried only when the whole window is empty. 7- and 30-day windows are sensitivity measures.")
    ]
    with pd.ExcelWriter(SITE_DATA_DIR / "wdsi_paper_replication.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(notes, columns=["series", "definition"]).to_excel(writer, sheet_name="readme", index=False)
        frozen.to_excel(writer, sheet_name="international_daily", index=False)
        paper_periods.to_excel(writer, sheet_name="international_periods", index=False)
        docs.loc[docs.date.ge("2010-01-01")].to_excel(writer, sheet_name="stock_connect_daily", index=False)


if __name__ == "__main__":
    main()
