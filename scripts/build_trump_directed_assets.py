from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from openpyxl.styles import Font


SITE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SITE_ROOT.parent
SOURCE_DAILY_CSV = WORKSPACE_ROOT / "data" / "trump_index" / "daily" / "trump_country_directed_daily.csv"
SOURCE_SUMMARY_JSON = WORKSPACE_ROOT / "data" / "trump_index" / "daily" / "trump_country_directed_summary.json"
SITE_DATA_DIR = SITE_ROOT / "data"
SITE_COUNTRY_DIR = SITE_DATA_DIR / "trump_directed"
SITE_SUMMARY_JSON = SITE_DATA_DIR / "trump_directed_summary.json"
SITE_XLSX = SITE_DATA_DIR / "trump_directed_workbook.xlsx"

COUNTRY_ORDER = ["CN", "DE", "JP", "IN", "UK", "FR", "IT", "CA", "BR", "RU", "KR", "MX", "AU", "ES"]
JSON_COLUMNS = [
    "date",
    "directed_posts",
    "directed_unique_texts",
    "directed_tone_index",
    "directed_tone_index_7d",
    "directed_tone_index_30d",
    "directed_geopolitical_index",
    "directed_geopolitical_index_7d",
    "directed_geopolitical_index_30d",
    "directed_attention_index",
    "directed_attention_index_7d",
    "directed_attention_index_30d",
]

VARIABLE_DEFINITIONS = [
    {
        "variable": "date",
        "category": "index key",
        "description": "Calendar date of the country-directed Trump record.",
        "units_or_scale": "YYYY-MM-DD",
        "recommended_use": "core",
    },
    {
        "variable": "country_code",
        "category": "country key",
        "description": "Two-letter country code used by the WDSI site.",
        "units_or_scale": "categorical text",
        "recommended_use": "core",
    },
    {
        "variable": "country_label",
        "category": "country key",
        "description": "English country label used on the site.",
        "units_or_scale": "categorical text",
        "recommended_use": "core",
    },
    {
        "variable": "authored_posts_total",
        "category": "coverage",
        "description": "Total authored Trump text posts in the full archive on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "directed_posts",
        "category": "attention",
        "description": "Number of authored Trump posts on that date that materially mention the given country.",
        "units_or_scale": "count",
        "recommended_use": "core",
    },
    {
        "variable": "directed_unique_texts",
        "category": "attention",
        "description": "Unique-text count among materially directed posts for that country-date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "directed_tone_index",
        "category": "tone",
        "description": "Daily mean Trump attitude toward the country on days with a material mention. Higher is more positive, lower is more hostile.",
        "units_or_scale": "-3 to 3",
        "recommended_use": "core",
    },
    {
        "variable": "directed_geopolitical_index",
        "category": "geopolitics",
        "description": "Daily mean geopolitical stance toward the country on days with a material mention. Higher is more cooperative, lower is more escalatory or threatening.",
        "units_or_scale": "-3 to 3",
        "recommended_use": "core",
    },
    {
        "variable": "directed_attention_index",
        "category": "attention",
        "description": "Log(1 + directed_posts) on that country-date, used as a compact attention measure.",
        "units_or_scale": "log-count index",
        "recommended_use": "core",
    },
    {
        "variable": "directed_tone_index_7d",
        "category": "smoothed",
        "description": "7-day rolling mean of directed_tone_index, leaving windows with no recent mention as blank.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "directed_tone_index_30d",
        "category": "smoothed",
        "description": "30-day rolling mean of directed_tone_index, leaving windows with no recent mention as blank.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "directed_geopolitical_index_7d",
        "category": "smoothed",
        "description": "7-day rolling mean of directed_geopolitical_index, leaving windows with no recent mention as blank.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "directed_geopolitical_index_30d",
        "category": "smoothed",
        "description": "30-day rolling mean of directed_geopolitical_index, leaving windows with no recent mention as blank.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "directed_attention_index_7d",
        "category": "smoothed",
        "description": "7-day rolling mean of directed_attention_index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "directed_attention_index_30d",
        "category": "smoothed",
        "description": "30-day rolling mean of directed_attention_index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "attention_share_authored",
        "category": "attention",
        "description": "Share of same-day authored Trump posts that materially mention the country.",
        "units_or_scale": "ratio from 0 to 1",
        "recommended_use": "supporting",
    },
    {
        "variable": "country_leader_mentions",
        "category": "mention type",
        "description": "Count of directed mentions routed through named leaders on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "country_government_mentions",
        "category": "mention type",
        "description": "Count of directed mentions routed through the government or regime on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "country_capital_mentions",
        "category": "mention type",
        "description": "Count of directed mentions routed through the capital city on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
]


def round_or_none(value: object, digits: int = 4) -> object:
    if value == "" or pd.isna(value):
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return value


def round_or_zero(value: object, digits: int = 4) -> object:
    if value == "" or pd.isna(value):
        return 0.0
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return value


def build_country_payload(frame: pd.DataFrame, summary_item: dict[str, object]) -> dict[str, object]:
    records: list[dict[str, object]] = []
    for row in frame[JSON_COLUMNS].itertuples(index=False):
        record = {
            "date": getattr(row, "date"),
            "directed_posts": int(getattr(row, "directed_posts")),
            "directed_unique_texts": int(getattr(row, "directed_unique_texts")),
            "directed_tone_index": round_or_none(getattr(row, "directed_tone_index")),
            "directed_tone_index_7d": round_or_none(getattr(row, "directed_tone_index_7d")),
            "directed_tone_index_30d": round_or_none(getattr(row, "directed_tone_index_30d")),
            "directed_geopolitical_index": round_or_none(getattr(row, "directed_geopolitical_index")),
            "directed_geopolitical_index_7d": round_or_none(getattr(row, "directed_geopolitical_index_7d")),
            "directed_geopolitical_index_30d": round_or_none(getattr(row, "directed_geopolitical_index_30d")),
            "directed_attention_index": round_or_zero(getattr(row, "directed_attention_index")),
            "directed_attention_index_7d": round_or_zero(getattr(row, "directed_attention_index_7d")),
            "directed_attention_index_30d": round_or_zero(getattr(row, "directed_attention_index_30d")),
        }
        records.append(record)
    return {
        "code": summary_item["code"],
        "label": summary_item["label"],
        "last_mention_date": summary_item["last_mention_date"],
        "directed_posts_total": summary_item["directed_posts_total"],
        "directed_days": summary_item["directed_days"],
        "records": records,
    }


def build_directed_assets() -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    SITE_COUNTRY_DIR.mkdir(parents=True, exist_ok=True)

    daily = pd.read_csv(SOURCE_DAILY_CSV, low_memory=False)
    summary = json.loads(SOURCE_SUMMARY_JSON.read_text(encoding="utf-8"))
    daily = daily.sort_values(["country_code", "date"]).reset_index(drop=True)

    countries_summary: list[dict[str, object]] = []
    grouped = {code: frame.copy() for code, frame in daily.groupby("country_code", sort=False)}
    for code in COUNTRY_ORDER:
        frame = grouped.get(code, pd.DataFrame(columns=daily.columns))
        source_meta = (summary.get("countries") or {}).get(code, {})
        latest_row = frame.iloc[-1] if not frame.empty else None
        last_mention_date = source_meta.get("end")
        if last_mention_date:
            last_mention_date = last_mention_date[:10]

        item = {
            "code": code,
            "label": source_meta.get("label", frame["country_label"].iloc[0] if not frame.empty else code),
            "directed_posts_total": int(source_meta.get("directed_posts", 0)),
            "directed_days": int(source_meta.get("directed_days", 0)),
            "last_mention_date": last_mention_date,
            "latest_tone_7d": round_or_none(latest_row["directed_tone_index_7d"]) if latest_row is not None else None,
            "latest_geopolitical_7d": round_or_none(latest_row["directed_geopolitical_index_7d"]) if latest_row is not None else None,
            "latest_attention_7d": round_or_zero(latest_row["directed_attention_index_7d"]) if latest_row is not None else 0.0,
            "latest_tone_30d": round_or_none(latest_row["directed_tone_index_30d"]) if latest_row is not None else None,
            "latest_geopolitical_30d": round_or_none(latest_row["directed_geopolitical_index_30d"]) if latest_row is not None else None,
            "latest_attention_30d": round_or_zero(latest_row["directed_attention_index_30d"]) if latest_row is not None else 0.0,
        }
        countries_summary.append(item)

        country_payload = build_country_payload(frame, item)
        (SITE_COUNTRY_DIR / f"{code}.json").write_text(
            json.dumps(country_payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )

    summary_payload = {
        "generated_at": summary["built_at_utc"],
        "coverage_start": summary["coverage_start"],
        "coverage_end": summary["coverage_end"],
        "model": summary["model"],
        "candidate_texts_scored": summary["candidate_texts_scored"],
        "accepted_candidate_texts": summary["accepted_candidate_texts"],
        "provider_blocked_candidate_texts": summary["provider_blocked_candidate_texts"],
        "post_country_rows": summary["post_country_rows"],
        "country_panel_rows": summary["country_panel_rows"],
        "countries": countries_summary,
        "note": summary["note"],
    }
    SITE_SUMMARY_JSON.write_text(
        json.dumps(summary_payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    workbook_daily = daily.copy()
    workbook_summary = pd.DataFrame(countries_summary)
    definitions = pd.DataFrame(VARIABLE_DEFINITIONS)
    with pd.ExcelWriter(SITE_XLSX, engine="openpyxl") as writer:
        workbook_daily.to_excel(writer, sheet_name="country_daily_panel", index=False)
        workbook_summary.to_excel(writer, sheet_name="country_summary", index=False)
        definitions.to_excel(writer, sheet_name="variable_definitions", index=False)
        workbook = writer.book
        defs_sheet = writer.sheets["variable_definitions"]
        defs_sheet.freeze_panes = "A2"
        defs_sheet.insert_rows(1, amount=4)
        defs_sheet["A1"] = "Trump Directed Sentiment by Country"
        defs_sheet["A2"] = (
            f"Coverage: {summary_payload['coverage_start']} to {summary_payload['coverage_end']} | "
            f"Candidate texts scored: {summary_payload['accepted_candidate_texts']} / {summary_payload['candidate_texts_scored']}"
        )
        defs_sheet["A3"] = (
            "Country-directed tone and geopolitical series stay blank on dates with no material mention of that country. "
            "Attention remains available as log(1 + directed posts)."
        )
        defs_sheet["A1"].font = Font(bold=True, size=14)
        defs_sheet["A2"].font = Font(italic=True)
        defs_sheet["A3"].font = Font(italic=True)
        for sheet in writer.sheets.values():
            sheet.freeze_panes = sheet.freeze_panes or "A2"
            for column_cells in sheet.columns:
                letter = column_cells[0].column_letter
                max_length = max(len(str(cell.value or "")) for cell in column_cells)
                sheet.column_dimensions[letter].width = min(max(max_length + 2, 12), 48)
        workbook.save(SITE_XLSX)

    print(f"[OK] saved: {SITE_SUMMARY_JSON}")
    print(f"[OK] saved: {SITE_XLSX}")
    for code in COUNTRY_ORDER:
        print(f"[OK] saved: {SITE_COUNTRY_DIR / f'{code}.json'}")


def main() -> None:
    build_directed_assets()


if __name__ == "__main__":
    main()
