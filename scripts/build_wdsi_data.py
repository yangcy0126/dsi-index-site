from __future__ import annotations

import json
import re
import shutil
import subprocess
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

import pandas as pd
from openpyxl.styles import Font


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data"
RECORDS_DIR = ROOT / "records"
PLACEHOLDER_NOTE = "Reserved top-15 GDP slot. WDSI source onboarding and validation are pending."
LEGACY_DIR = ROOT.parent / "data" / "情绪测度结果数据"
AUTHORITATIVE_METHOD_NOTE = (
    "Locked to DSI-ICF/code/data_integration.executed.ipynb: "
    "publication-day raw score = same-day minimum, then forward-fill missing days, "
    "then compute rolling means on the filled daily path."
)
COUNTRY_WORKBOOK_COLUMNS = ["date", "raw", "rolling7", "rolling30", "publication"]
MASTER_WORKBOOK_COLUMNS = ["code", "country", "date", "raw", "rolling7", "rolling30", "publication"]
COUNTRY_VARIABLE_DEFINITIONS = [
    {
        "variable": "date",
        "description": "Calendar date of the WDSI daily observation.",
        "units_or_scale": "YYYY-MM-DD",
        "notes": "Daily calendarized series after forward-filling missing days.",
    },
    {
        "variable": "raw",
        "description": "Publication-day raw WDSI score using the same-day minimum when multiple texts are released.",
        "units_or_scale": "integer from -3 to 3",
        "notes": "Blank on non-publication days.",
    },
    {
        "variable": "rolling7",
        "description": "7-day rolling mean computed on the forward-filled daily path.",
        "units_or_scale": "continuous index",
        "notes": "Primary short-horizon WDSI view shown on the site.",
    },
    {
        "variable": "rolling30",
        "description": "30-day rolling mean computed on the forward-filled daily path.",
        "units_or_scale": "continuous index",
        "notes": "Smoother medium-horizon WDSI trend.",
    },
    {
        "variable": "publication",
        "description": "Indicator for whether an official source publication was observed on that date.",
        "units_or_scale": "True / False",
        "notes": "False means the series value is carried forward from the most recent publication day.",
    },
]
MASTER_EXTRA_VARIABLE_DEFINITIONS = [
    {
        "variable": "code",
        "description": "Two-letter country or region code used by the WDSI site.",
        "units_or_scale": "categorical text",
        "notes": "Matches the tab and download naming convention on the site.",
    },
    {
        "variable": "country",
        "description": "English country or region label.",
        "units_or_scale": "categorical text",
        "notes": "Human-readable display label.",
    },
]
VISITOR_COUNTER_ID = "DVgZ"
VISITOR_OVERVIEW_URL = f"https://s01.flagcounter.com/more/{VISITOR_COUNTER_ID}/"
VISITOR_COUNTRIES_URL = f"https://s01.flagcounter.com/countries/{VISITOR_COUNTER_ID}/"
VISITOR_HIDDEN_COUNTRIES = {"Taiwan"}

COUNTRIES = [
    {
        "code": "CN",
        "label": "China",
        "label_zh": "中国",
        "series_zh": "中国外交部例行记者会",
        "legacy_filename": "中国外交部例行记者会情绪测度结果.xlsx",
        "color": "#0f6c74",
    },
    {
        "code": "US",
        "label": "United States",
        "label_zh": "美国",
        "series_zh": "美国国务院新闻办公室发言稿",
        "legacy_filename": "美国国务院新闻办公室发言稿情绪测度结果.xlsx",
        "color": "#b85f35",
    },
    {
        "code": "UK",
        "label": "United Kingdom",
        "label_zh": "英国",
        "series_zh": "英国外交办公室新闻稿",
        "legacy_filename": "英国外交办公室新闻稿情绪测度结果.xlsx",
        "color": "#5c7c5a",
    },
    {
        "code": "JP",
        "label": "Japan",
        "label_zh": "日本",
        "series_zh": "日本外交部官方文本",
        "legacy_filename": "日本外交部数据情绪测度结果.xlsx",
        "color": "#7851a9",
    },
    {
        "code": "KR",
        "label": "South Korea",
        "label_zh": "韩国",
        "series_zh": "韩国外交部新闻稿",
        "legacy_filename": "韩国外交部新闻稿情绪测度结果.xlsx",
        "color": "#9a6b2f",
    },
    {
        "code": "DE",
        "label": "Germany",
        "label_zh": "Germany",
        "series_zh": "Federal Foreign Office newsroom",
        "legacy_filename": "",
        "color": "#1f3f5b",
    },
    {
        "code": "IN",
        "label": "India",
        "label_zh": "India",
        "series_zh": "India MEA official texts",
        "legacy_filename": "",
        "color": "#d36a24",
    },
    {
        "code": "FR",
        "label": "France",
        "label_zh": "France",
        "series_zh": "France MFA spokesperson live Q&A",
        "legacy_filename": "",
        "color": "#2f5aa8",
    },
    {
        "code": "IT",
        "label": "Italy",
        "label_zh": "Italy",
        "series_zh": "Italian MFA press releases",
        "legacy_filename": "",
        "color": "#3f7562",
        "placeholder": True,
    },
    {
        "code": "CA",
        "label": "Canada",
        "label_zh": "Canada",
        "series_zh": "Global Affairs Canada official news feed",
        "legacy_filename": "",
        "color": "#b24d4d",
        "placeholder": True,
    },
    {
        "code": "BR",
        "label": "Brazil",
        "label_zh": "Brazil",
        "series_zh": "Brazilian MFA press releases",
        "legacy_filename": "",
        "color": "#4d7a3e",
        "placeholder": True,
    },
    {
        "code": "RU",
        "label": "Russia",
        "label_zh": "Russia",
        "series_zh": "Russian MFA foreign policy news",
        "legacy_filename": "",
        "color": "#a24a3f",
    },
    {
        "code": "MX",
        "label": "Mexico",
        "label_zh": "Mexico",
        "series_zh": "Mexico SRE press archive",
        "legacy_filename": "",
        "color": "#2f7a70",
        "placeholder": True,
    },
    {
        "code": "AU",
        "label": "Australia",
        "label_zh": "Australia",
        "series_zh": "Australian Foreign Minister media releases",
        "legacy_filename": "",
        "color": "#6c5b9a",
        "placeholder": True,
    },
    {
        "code": "ES",
        "label": "Spain",
        "label_zh": "Spain",
        "series_zh": "Spanish MFA ministerial statements",
        "legacy_filename": "",
        "color": "#c37b2d",
        "placeholder": True,
    },
]

COUNTRY_ORDER = ["US", "CN", "DE", "JP", "IN", "UK", "FR", "IT", "CA", "BR", "RU", "KR", "MX", "AU", "ES"]
COUNTRIES = sorted(COUNTRIES, key=lambda meta: COUNTRY_ORDER.index(meta["code"]))

EVENTS = [
    {"date": "2014-03-18", "title_zh": "克里米亚危机升级", "title_en": "Crimea crisis escalates"},
    {"date": "2016-07-12", "title_zh": "南海仲裁结果公布", "title_en": "South China Sea arbitration"},
    {"date": "2022-02-24", "title_zh": "俄乌战争爆发", "title_en": "Russia-Ukraine war"},
    {"date": "2023-10-07", "title_zh": "新一轮巴以冲突升级", "title_en": "Israel-Hamas war escalation"},
]


def strip_html_text(html: str) -> str:
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def fetch_url_text_with_curl(url: str) -> str:
    curl_bin = shutil.which("curl") or shutil.which("curl.exe")
    if not curl_bin:
        raise FileNotFoundError("curl is not available")
    result = subprocess.run(
        [
            curl_bin,
            "-fsSL",
            "--http1.1",
            "--retry",
            "2",
            "--retry-delay",
            "1",
            "--connect-timeout",
            "15",
            "--max-time",
            "40",
            "-A",
            "Mozilla/5.0",
            url,
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="latin-1",
    )
    return result.stdout


def fetch_url_text_with_urllib(url: str) -> str:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=40) as response:
        charset = response.headers.get_content_charset() or "latin-1"
        return response.read().decode(charset, errors="replace")


def fetch_url_text(url: str) -> str:
    errors: list[str] = []
    for fetcher in (fetch_url_text_with_curl, fetch_url_text_with_urllib):
        try:
            return fetcher(url)
        except (FileNotFoundError, subprocess.CalledProcessError, TimeoutError, OSError, URLError) as exc:
            errors.append(f"{fetcher.__name__}: {exc}")
    raise RuntimeError(f"Failed to fetch {url}: {'; '.join(errors)}")


def extract_required_match(text: str, pattern: str, label: str) -> re.Match[str]:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not parse {label} from visitor counter snapshot.")
    return match


def parse_visitor_overview(overview_html: str) -> dict[str, object]:
    text = strip_html_text(overview_html)
    country_match = extract_required_match(
        text,
        r"(\d+)\s+different countries have visited this site\.\s+(\d+)\s+flags collected\.",
        "visitor coverage",
    )
    visitors_match = extract_required_match(
        text,
        r"Visitors\s+Yesterday:\s*(\d+)\s+30 day average:\s*(\d+)\s+Record:\s*(\d+)\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        "visitor totals",
    )
    views_match = extract_required_match(
        text,
        r"Flag Counter Views\s+Yesterday:\s*(\d+)\s+30 day average:\s*(\d+)\s+Record:\s*(\d+)\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        "counter views",
    )
    return {
        "total_countries": int(country_match.group(1)),
        "flags_collected": int(country_match.group(2)),
        "visitors_yesterday": int(visitors_match.group(1)),
        "visitors_30d_average": int(visitors_match.group(2)),
        "visitors_record": int(visitors_match.group(3)),
        "visitors_record_date": visitors_match.group(4),
        "views_yesterday": int(views_match.group(1)),
        "views_30d_average": int(views_match.group(2)),
        "views_record": int(views_match.group(3)),
        "views_record_date": views_match.group(4),
    }


def parse_visitor_countries(countries_html: str) -> list[dict[str, object]]:
    countries: list[dict[str, object]] = []
    for row_html in re.findall(r"<tr>(.*?)</tr>", countries_html, flags=re.IGNORECASE | re.DOTALL):
        if "style=\"display:none;\"" in row_html.lower():
            continue
        if "/flag_details/" not in row_html or "/factbook/" not in row_html:
            continue
        code_match = re.search(r"/factbook/([a-z]{2})/DVgZ", row_html, flags=re.IGNORECASE)
        country_match = re.search(r"/factbook/[a-z]{2}/DVgZ[^>]*><u>([^<]+)</u></a>", row_html, flags=re.IGNORECASE)
        visitors_match = re.search(
            r"</a></font></td>\s*<td[^>]*><font[^>]*>(\d+)</font></td>",
            row_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        last_seen_match = re.search(r"<td>([^<]+)</td>\s*$", row_html, flags=re.IGNORECASE | re.DOTALL)
        if not code_match or not country_match or not visitors_match or not last_seen_match:
            continue
        countries.append(
            {
                "code": code_match.group(1).upper(),
                "country": unescape(country_match.group(1)).strip(),
                "visitors": int(visitors_match.group(1)),
                "last_seen": strip_html_text(last_seen_match.group(1)),
            }
        )
    return countries


def build_visitor_snapshot() -> dict[str, object]:
    overview_html = fetch_url_text(VISITOR_OVERVIEW_URL)
    countries_html = fetch_url_text(VISITOR_COUNTRIES_URL)
    overview = parse_visitor_overview(overview_html)
    countries = parse_visitor_countries(countries_html)
    visible_countries = [
        country for country in countries if country["country"] not in VISITOR_HIDDEN_COUNTRIES
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "available": True,
        "counter_id": VISITOR_COUNTER_ID,
        "source": "Flag Counter public overview",
        "hidden_countries": sorted(VISITOR_HIDDEN_COUNTRIES),
        "total_countries": overview["total_countries"],
        "flags_collected": overview["flags_collected"],
        "visitors_yesterday": overview["visitors_yesterday"],
        "visitors_30d_average": overview["visitors_30d_average"],
        "visitors_record": overview["visitors_record"],
        "visitors_record_date": overview["visitors_record_date"],
        "views_yesterday": overview["views_yesterday"],
        "views_30d_average": overview["views_30d_average"],
        "views_record": overview["views_record"],
        "views_record_date": overview["views_record_date"],
        "countries": visible_countries,
        "top_countries": visible_countries[:5],
    }


def round_or_none(value: float | None, digits: int = 3) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def int_or_none(value: float | int | None) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(round(float(value)))


def autosize_sheet(worksheet) -> None:
    for column_cells in worksheet.columns:
        letter = column_cells[0].column_letter
        max_length = max(len(str(cell.value or "")) for cell in column_cells)
        worksheet.column_dimensions[letter].width = min(max(max_length + 2, 12), 52)


def write_workbook(
    frame: pd.DataFrame,
    output_path: Path,
    *,
    title: str,
    subtitle: str,
    variable_definitions: list[dict[str, str]],
) -> None:
    definitions = pd.DataFrame(variable_definitions)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="daily_data", index=False)
        definitions.to_excel(writer, sheet_name="variable_definitions", index=False)

        data_sheet = writer.sheets["daily_data"]
        defs_sheet = writer.sheets["variable_definitions"]
        data_sheet.freeze_panes = "A2"
        defs_sheet.freeze_panes = "A2"
        defs_sheet.insert_rows(1, amount=4)
        defs_sheet["A1"] = title
        defs_sheet["A2"] = subtitle
        defs_sheet["A3"] = AUTHORITATIVE_METHOD_NOTE
        defs_sheet["A1"].font = Font(bold=True, size=14)
        defs_sheet["A2"].font = Font(italic=True)
        defs_sheet["A3"].font = Font(italic=True)

        autosize_sheet(data_sheet)
        autosize_sheet(defs_sheet)


def collapse_to_daily_minimum(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply the original DSI-ICF daily aggregation rule: one day, one minimum raw score."""
    return (
        frame.sort_values(["date", "raw", "title", "url"], ascending=[True, True, True, True])
        .groupby("date", as_index=False)
        .first()
        .sort_values("date")
        .reset_index(drop=True)
    )


def build_filled_daily_series(daily: pd.DataFrame, end_date: pd.Timestamp) -> pd.DataFrame:
    """Reproduce the original DSI-ICF notebook path: calendarize, forward-fill, then smooth."""
    calendar = pd.DataFrame({"date": pd.date_range(daily["date"].min(), end_date, freq="D")})
    merged = calendar.merge(daily.assign(publication=True), on="date", how="left")
    merged["publication"] = merged["publication"].notna()
    merged["filled"] = merged["raw"].ffill()
    merged["rolling7"] = merged["filled"].rolling(7).mean()
    merged["rolling30"] = merged["filled"].rolling(30).mean()
    return merged


def validate_method_lock(frame: pd.DataFrame) -> None:
    raw_values = frame["raw"].dropna()
    if not raw_values.empty and not (raw_values == raw_values.round()).all():
        raise ValueError("Method lock failed: raw publication-day scores must remain integers.")
    if len(frame) >= 6 and frame["rolling7"].iloc[:6].notna().any():
        raise ValueError("Method lock failed: 7-day rolling series should be empty before day 7.")
    if len(frame) >= 29 and frame["rolling30"].iloc[:29].notna().any():
        raise ValueError("Method lock failed: 30-day rolling series should be empty before day 30.")


def load_from_records(meta: dict[str, str]) -> pd.DataFrame:
    path = RECORDS_DIR / f"{meta['code']}.csv"
    if not path.exists():
        raise FileNotFoundError(path)

    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "score" not in frame.columns or "published_at" not in frame.columns:
        raise ValueError(f"Missing required columns in {path}")

    daily = pd.DataFrame(
        {
            "date": pd.to_datetime(frame["published_at"], errors="coerce"),
            "raw": pd.to_numeric(frame["score"], errors="coerce"),
            "title": frame.get("title", ""),
            "url": frame.get("url", ""),
        }
    )
    daily = daily.dropna(subset=["date", "raw"])
    daily = collapse_to_daily_minimum(daily)
    return daily


def load_from_legacy(meta: dict[str, str]) -> pd.DataFrame:
    if not meta.get("legacy_filename"):
        raise FileNotFoundError(f"No legacy baseline configured for {meta['code']}")
    path = LEGACY_DIR / meta["legacy_filename"]
    if not path.exists():
        raise FileNotFoundError(path)

    frame = pd.read_excel(path)
    score_col = next(column for column in frame.columns if str(column) == "2")
    date_col = next(column for column in frame.columns if str(column).lower() == "time")

    daily = frame[[date_col, score_col]].copy()
    daily[date_col] = pd.to_datetime(daily[date_col], errors="coerce")
    daily[score_col] = pd.to_numeric(daily[score_col], errors="coerce")
    daily = daily.dropna(subset=[date_col, score_col])
    daily = (
        daily.groupby(date_col, as_index=False)[score_col]
        .min()
        .rename(columns={date_col: "date", score_col: "raw"})
        .sort_values("date")
        .reset_index(drop=True)
    )
    daily["title"] = ""
    daily["url"] = ""
    return daily


def read_country(meta: dict[str, str]) -> tuple[dict[str, object], pd.DataFrame]:
    try:
        daily = load_from_records(meta)
        data_source = "records"
    except FileNotFoundError:
        daily = load_from_legacy(meta)
        data_source = "legacy_excel"

    if daily.empty:
        raise ValueError(f"No usable observations for {meta['code']}")

    today = pd.Timestamp(datetime.now(timezone.utc).date())
    end_date = max(daily["date"].max(), today)
    merged = build_filled_daily_series(daily, end_date)

    latest_rolling = float(merged["rolling7"].iloc[-1])
    latest_rolling30 = float(merged["rolling30"].iloc[-1])
    previous_7 = float(merged["rolling7"].shift(7).iloc[-1]) if len(merged) > 7 else None
    previous_30 = float(merged["rolling7"].shift(30).iloc[-1]) if len(merged) > 30 else None
    current_year = int(merged["date"].dt.year.max())

    latest_publication = daily.iloc[-1]
    summary = {
        "code": meta["code"],
        "label": meta["label"],
        "label_zh": meta["label_zh"],
        "series_zh": meta["series_zh"],
        "color": meta["color"],
        "start_date": merged["date"].iloc[0].date().isoformat(),
        "latest_date": merged["date"].iloc[-1].date().isoformat(),
        "latest_publication_date": latest_publication["date"].date().isoformat(),
        "publication_days": int(len(daily)),
        "calendar_days": int(len(merged)),
        "latest_raw": int_or_none(latest_publication["raw"]),
        "latest_7d": round_or_none(latest_rolling),
        "latest_30d": round_or_none(latest_rolling30),
        "change_7d": round_or_none(latest_rolling - previous_7) if previous_7 is not None else None,
        "change_30d": round_or_none(latest_rolling - previous_30) if previous_30 is not None else None,
        "current_year": current_year,
        "current_year_mean": round_or_none(
            float(merged.loc[merged["date"].dt.year == current_year, "rolling7"].mean())
        ),
        "latest_title": str(latest_publication.get("title", "") or ""),
        "latest_url": str(latest_publication.get("url", "") or ""),
        "data_source": data_source,
        "file_json": f"data/{meta['code']}.json",
        "file_csv": f"data/{meta['code']}.csv",
        "file_xlsx": f"data/{meta['code']}.xlsx",
        "is_placeholder": False,
        "placeholder_note": "",
    }

    export_frame = merged[["date", "raw", "rolling7", "rolling30", "publication"]].copy()
    export_frame["date"] = export_frame["date"].dt.strftime("%Y-%m-%d")
    export_frame["raw"] = export_frame["raw"].round().astype("Int64")
    validate_method_lock(export_frame)
    return summary, export_frame


def build_placeholder_summary(meta: dict[str, str]) -> dict[str, object]:
    return {
        "code": meta["code"],
        "label": meta["label"],
        "label_zh": meta["label_zh"],
        "series_zh": meta["series_zh"],
        "color": meta["color"],
        "start_date": None,
        "latest_date": None,
        "latest_publication_date": None,
        "publication_days": 0,
        "calendar_days": 0,
        "latest_raw": None,
        "latest_7d": None,
        "latest_30d": None,
        "change_7d": None,
        "change_30d": None,
        "current_year": None,
        "current_year_mean": None,
        "latest_title": "",
        "latest_url": "",
        "data_source": "placeholder",
        "file_json": f"data/{meta['code']}.json",
        "file_csv": f"data/{meta['code']}.csv",
        "file_xlsx": f"data/{meta['code']}.xlsx",
        "is_placeholder": True,
        "placeholder_note": PLACEHOLDER_NOTE,
    }


def build_placeholder_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "raw", "rolling7", "rolling30", "publication"])


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    countries: list[dict[str, object]] = []
    all_rows: list[pd.DataFrame] = []

    for meta in COUNTRIES:
        try:
            summary, frame = read_country(meta)
        except (FileNotFoundError, ValueError) as exc:
            if meta.get("placeholder"):
                print(f"Placeholder {meta['code']}: {exc}")
                summary = build_placeholder_summary(meta)
                frame = build_placeholder_frame()
            else:
                print(f"Skipping {meta['code']}: {exc}")
                continue
        countries.append(summary)

        json_payload = {
            "code": summary["code"],
            "label": summary["label"],
            "label_zh": summary["label_zh"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "is_placeholder": bool(summary.get("is_placeholder")),
            "placeholder_note": str(summary.get("placeholder_note", "") or ""),
            "records": [
                {
                    "date": row.date,
                    "raw": int_or_none(row.raw),
                    "rolling7": round_or_none(row.rolling7),
                    "rolling30": round_or_none(row.rolling30),
                    "publication": bool(row.publication),
                }
                for row in frame.itertuples(index=False)
            ],
        }

        (OUTPUT_DIR / f"{meta['code']}.json").write_text(
            json.dumps(json_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        frame.to_csv(OUTPUT_DIR / f"{meta['code']}.csv", index=False, encoding="utf-8-sig")
        if not summary["is_placeholder"]:
            write_workbook(
                frame[COUNTRY_WORKBOOK_COLUMNS].copy(),
                OUTPUT_DIR / f"{meta['code']}.xlsx",
                title=f"{meta['label']} WDSI Data Workbook",
                subtitle=(
                    f"Coverage: {summary['start_date']} to {summary['latest_date']} | "
                    f"Publication days: {summary['publication_days']}"
                ),
                variable_definitions=COUNTRY_VARIABLE_DEFINITIONS,
            )

        if not summary["is_placeholder"]:
            country_frame = frame.copy()
            country_frame.insert(0, "country", meta["label"])
            country_frame.insert(0, "code", meta["code"])
            all_rows.append(country_frame)

    live_countries = [country for country in countries if not country.get("is_placeholder")]
    if not countries or not all_rows or not live_countries:
        raise RuntimeError("No country data available to build site assets.")

    full_daily = pd.concat(all_rows, ignore_index=True)
    full_daily.to_csv(OUTPUT_DIR / "wdsi_all_countries.csv", index=False, encoding="utf-8-sig")
    write_workbook(
        full_daily[MASTER_WORKBOOK_COLUMNS].copy(),
        OUTPUT_DIR / "wdsi_all_countries.xlsx",
        title="WDSI Full Daily Dataset Workbook",
        subtitle=(
            f"Coverage: {min(country['start_date'] for country in live_countries)} to "
            f"{max(country['latest_date'] for country in live_countries)} | "
            f"Countries / regions: {len(live_countries)}"
        ),
        variable_definitions=MASTER_EXTRA_VARIABLE_DEFINITIONS + COUNTRY_VARIABLE_DEFINITIONS,
    )

    summary_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method_note": AUTHORITATIVE_METHOD_NOTE,
        "overall": {
            "country_count": len(countries),
            "live_country_count": len(live_countries),
            "placeholder_count": len(countries) - len(live_countries),
            "first_date": min(country["start_date"] for country in live_countries),
            "last_date": max(country["latest_date"] for country in live_countries),
        },
        "countries": countries,
    }

    (OUTPUT_DIR / "summary.json").write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "events.json").write_text(
        json.dumps({"events": EVENTS}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    try:
        visitor_payload = build_visitor_snapshot()
    except Exception as exc:
        print(f"Visitor snapshot unavailable: {exc}")
        visitor_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "counter_id": VISITOR_COUNTER_ID,
            "source": "Flag Counter public overview",
            "hidden_countries": sorted(VISITOR_HIDDEN_COUNTRIES),
            "countries": [],
            "top_countries": [],
        }
    (OUTPUT_DIR / "visitor_stats.json").write_text(
        json.dumps(visitor_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Built WDSI web data in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
