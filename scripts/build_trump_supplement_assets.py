from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

for _site_path in filter(
    None,
    [
        os.getenv("CODEX_PYTHON_SITE"),
        r"C:\codex_sitepkgs",
    ],
):
    if Path(_site_path).exists() and _site_path not in sys.path:
        sys.path.insert(0, _site_path)

import pandas as pd
from openpyxl.styles import Font

from build_trump_directed_assets import build_directed_assets


SITE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = SITE_ROOT.parent
SOURCE_DAILY_CSV = WORKSPACE_ROOT / "data" / "trump_index" / "daily" / "trump_daily_indices_llm.csv"
SOURCE_SUMMARY_JSON = WORKSPACE_ROOT / "data" / "trump_index" / "daily" / "trump_index_summary_llm.json"
SITE_DATA_DIR = SITE_ROOT / "data"
SITE_JSON = SITE_DATA_DIR / "trump_indices.json"
SITE_XLSX = SITE_DATA_DIR / "trump_indices_workbook.xlsx"
SYNC_REPO_SCRIPT = WORKSPACE_ROOT / "DSI-ICF" / "code" / "sync_trump_index_data_repo.py"


JSON_COLUMNS = [
    "date",
    "trump_tone_index",
    "trump_tone_index_7d",
    "trump_tone_index_30d",
    "trump_geopolitical_index",
    "trump_geopolitical_index_7d",
    "trump_geopolitical_index_30d",
    "trump_shock_index",
    "trump_shock_index_7d",
    "trump_shock_index_30d",
]


VARIABLE_DEFINITIONS = [
    {
        "variable": "date",
        "category": "index key",
        "description": "Calendar date of the daily Trump supplement record.",
        "units_or_scale": "YYYY-MM-DD",
        "recommended_use": "core",
    },
    {
        "variable": "posts_total",
        "category": "activity",
        "description": "Total archived Trump-related posts on that date, including originals and reblogs across platforms.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "posts_twitter",
        "category": "activity",
        "description": "Total archived posts from Twitter on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "posts_truthsocial",
        "category": "activity",
        "description": "Total archived posts from Truth Social on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "posts_authored",
        "category": "activity",
        "description": "Number of authored Trump posts on that date after platform harmonization.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "posts_reblogs",
        "category": "activity",
        "description": "Number of reblogs or repost-like items on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "posts_with_text",
        "category": "activity",
        "description": "Number of archived posts containing non-empty text on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "uppercase_ratio_mean",
        "category": "style",
        "description": "Mean share of uppercase letters across authored text posts on that date.",
        "units_or_scale": "ratio from 0 to 1",
        "recommended_use": "supporting",
    },
    {
        "variable": "exclamation_count_mean",
        "category": "style",
        "description": "Mean number of exclamation marks across authored text posts on that date.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "scored_authored_posts",
        "category": "coverage",
        "description": "Number of authored text posts on that date that received an accepted LLM score.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "tone_score_llm_mean",
        "category": "tone",
        "description": "Daily mean raw LLM tone score on authored text posts.",
        "units_or_scale": "-3 to 3",
        "recommended_use": "supporting",
    },
    {
        "variable": "tone_index_llm",
        "category": "tone",
        "description": "Legacy platform-standardized daily tone series retained for backward compatibility.",
        "units_or_scale": "z-like standardized score",
        "recommended_use": "legacy",
    },
    {
        "variable": "tone_abs_llm_mean",
        "category": "tone",
        "description": "Daily mean absolute tone intensity irrespective of direction.",
        "units_or_scale": "0 to 3",
        "recommended_use": "supporting",
    },
    {
        "variable": "rhetorical_heat_llm_mean",
        "category": "tone",
        "description": "Daily mean LLM rhetorical heat score, capturing emotional and mobilizing intensity.",
        "units_or_scale": "0 to 3",
        "recommended_use": "supporting",
    },
    {
        "variable": "geo_posts_llm",
        "category": "geopolitics",
        "description": "Number of authored text posts that the model classified as geopolitical or cross-border.",
        "units_or_scale": "count",
        "recommended_use": "supporting",
    },
    {
        "variable": "geo_share_llm",
        "category": "geopolitics",
        "description": "Share of scored authored posts on that date that were classified as geopolitical.",
        "units_or_scale": "ratio from 0 to 1",
        "recommended_use": "supporting",
    },
    {
        "variable": "geopolitical_score_llm_mean",
        "category": "geopolitics",
        "description": "Daily mean raw geopolitical score among geopolitical posts only.",
        "units_or_scale": "-3 to 3",
        "recommended_use": "supporting",
    },
    {
        "variable": "geopolitical_index_llm",
        "category": "geopolitics",
        "description": "Legacy platform-standardized geopolitical series retained for backward compatibility.",
        "units_or_scale": "z-like standardized score",
        "recommended_use": "legacy",
    },
    {
        "variable": "scored_share_authored",
        "category": "coverage",
        "description": "Share of authored posts on that date that received an accepted score.",
        "units_or_scale": "ratio from 0 to 1",
        "recommended_use": "supporting",
    },
    {
        "variable": "reblog_density",
        "category": "activity",
        "description": "Share of same-day archived posts that were reblogs or repost-like items.",
        "units_or_scale": "ratio from 0 to 1",
        "recommended_use": "supporting",
    },
    {
        "variable": "platform_regime",
        "category": "platform",
        "description": "Indicator for whether the day is in the Twitter period, Truth Social period, mixed period, or has no posts.",
        "units_or_scale": "categorical text",
        "recommended_use": "supporting",
    },
    {
        "variable": "trump_tone_index",
        "category": "headline",
        "description": "Main Trump Tone Index. Daily mean LLM tone score on all authored text posts.",
        "units_or_scale": "-3 to 3",
        "recommended_use": "core",
    },
    {
        "variable": "trump_geopolitical_index",
        "category": "headline",
        "description": "Main Trump Geopolitical Index. Daily mean LLM geopolitical score on geopolitical posts only.",
        "units_or_scale": "-3 to 3",
        "recommended_use": "core",
    },
    {
        "variable": "shock_posts_component",
        "category": "shock components",
        "description": "Standardized posting-volume component inside the Trump Shock Index.",
        "units_or_scale": "standardized component",
        "recommended_use": "supporting",
    },
    {
        "variable": "shock_tone_component",
        "category": "shock components",
        "description": "Standardized tone-intensity component inside the Trump Shock Index.",
        "units_or_scale": "standardized component",
        "recommended_use": "supporting",
    },
    {
        "variable": "shock_heat_component",
        "category": "shock components",
        "description": "Standardized rhetorical-heat component inside the Trump Shock Index.",
        "units_or_scale": "standardized component",
        "recommended_use": "supporting",
    },
    {
        "variable": "shock_uppercase_component",
        "category": "shock components",
        "description": "Standardized all-caps usage component inside the Trump Shock Index.",
        "units_or_scale": "standardized component",
        "recommended_use": "supporting",
    },
    {
        "variable": "shock_exclamation_component",
        "category": "shock components",
        "description": "Standardized exclamation-mark intensity component inside the Trump Shock Index.",
        "units_or_scale": "standardized component",
        "recommended_use": "supporting",
    },
    {
        "variable": "shock_reblog_component",
        "category": "shock components",
        "description": "Standardized reblog-density component inside the Trump Shock Index.",
        "units_or_scale": "standardized component",
        "recommended_use": "supporting",
    },
    {
        "variable": "trump_shock_index_raw",
        "category": "shock",
        "description": "Weighted raw composite before final standardization into the Trump Shock Index.",
        "units_or_scale": "weighted composite",
        "recommended_use": "supporting",
    },
    {
        "variable": "trump_shock_index",
        "category": "headline",
        "description": "Main Trump Shock Index. Composite z-score of volume, intensity, all-caps, exclamations, and reblog density.",
        "units_or_scale": "z-score-like standardized index",
        "recommended_use": "core",
    },
    {
        "variable": "shock_raw_llm",
        "category": "shock",
        "description": "Legacy alias for trump_shock_index_raw.",
        "units_or_scale": "weighted composite",
        "recommended_use": "legacy",
    },
    {
        "variable": "shock_index_llm",
        "category": "shock",
        "description": "Legacy alias for trump_shock_index.",
        "units_or_scale": "z-score-like standardized index",
        "recommended_use": "legacy",
    },
    {
        "variable": "tone_index_llm_7d",
        "category": "legacy smoothed",
        "description": "7-day rolling mean of the legacy platform-standardized tone series.",
        "units_or_scale": "rolling average",
        "recommended_use": "legacy",
    },
    {
        "variable": "tone_index_llm_30d",
        "category": "legacy smoothed",
        "description": "30-day rolling mean of the legacy platform-standardized tone series.",
        "units_or_scale": "rolling average",
        "recommended_use": "legacy",
    },
    {
        "variable": "geopolitical_index_llm_7d",
        "category": "legacy smoothed",
        "description": "7-day rolling mean of the legacy platform-standardized geopolitical series.",
        "units_or_scale": "rolling average",
        "recommended_use": "legacy",
    },
    {
        "variable": "geopolitical_index_llm_30d",
        "category": "legacy smoothed",
        "description": "30-day rolling mean of the legacy platform-standardized geopolitical series.",
        "units_or_scale": "rolling average",
        "recommended_use": "legacy",
    },
    {
        "variable": "shock_index_llm_7d",
        "category": "legacy smoothed",
        "description": "7-day rolling mean of the legacy shock series.",
        "units_or_scale": "rolling average",
        "recommended_use": "legacy",
    },
    {
        "variable": "shock_index_llm_30d",
        "category": "legacy smoothed",
        "description": "30-day rolling mean of the legacy shock series.",
        "units_or_scale": "rolling average",
        "recommended_use": "legacy",
    },
    {
        "variable": "trump_tone_index_7d",
        "category": "headline smoothed",
        "description": "7-day rolling mean of the Trump Tone Index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "trump_tone_index_30d",
        "category": "headline smoothed",
        "description": "30-day rolling mean of the Trump Tone Index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "trump_geopolitical_index_7d",
        "category": "headline smoothed",
        "description": "7-day rolling mean of the Trump Geopolitical Index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "trump_geopolitical_index_30d",
        "category": "headline smoothed",
        "description": "30-day rolling mean of the Trump Geopolitical Index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "trump_shock_index_7d",
        "category": "headline smoothed",
        "description": "7-day rolling mean of the Trump Shock Index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
    {
        "variable": "trump_shock_index_30d",
        "category": "headline smoothed",
        "description": "30-day rolling mean of the Trump Shock Index.",
        "units_or_scale": "rolling average",
        "recommended_use": "core",
    },
]


def round_if_numeric(value: object, digits: int = 4) -> object:
    if value == "" or pd.isna(value):
        return 0.0
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return value


def build_compact_json(daily: pd.DataFrame, summary: dict[str, object]) -> dict[str, object]:
    records: list[dict[str, object]] = []
    for row in daily[JSON_COLUMNS].itertuples(index=False):
        records.append({column: round_if_numeric(getattr(row, column)) for column in JSON_COLUMNS})
    return {
        "generated_at": summary["built_at_utc"],
        "coverage_start": summary["coverage_start"],
        "coverage_end": summary["coverage_end"],
        "model": summary["model"],
        "scoring_coverage": summary["scoring_coverage"],
        "records": records,
    }


def write_workbook(daily: pd.DataFrame, summary: dict[str, object]) -> None:
    definitions = pd.DataFrame(VARIABLE_DEFINITIONS)
    with pd.ExcelWriter(SITE_XLSX, engine="openpyxl") as writer:
        daily.to_excel(writer, sheet_name="daily_indices", index=False)
        definitions.to_excel(writer, sheet_name="variable_definitions", index=False)
        workbook = writer.book
        data_sheet = writer.sheets["daily_indices"]
        defs_sheet = writer.sheets["variable_definitions"]

        data_sheet.freeze_panes = "A2"
        defs_sheet.freeze_panes = "A2"
        defs_sheet.insert_rows(1, amount=4)
        defs_sheet["A1"] = "Trump Indices Supplement Workbook"
        defs_sheet["A2"] = (
            f"Coverage: {summary['coverage_start']} to {summary['coverage_end']} | "
            f"Scored posts: {summary['scoring_coverage']['accepted_scored_posts']} / "
            f"{summary['scoring_coverage']['authored_text_posts']}"
        )
        defs_sheet["A3"] = (
            "Core columns are the three trump_* indices and their 7-day and 30-day rolling averages. "
            "Legacy columns are retained for backward compatibility."
        )
        defs_sheet["A1"].font = Font(bold=True, size=14)
        defs_sheet["A2"].font = Font(italic=True)
        defs_sheet["A3"].font = Font(italic=True)

        for sheet in (data_sheet, defs_sheet):
            for column_cells in sheet.columns:
                letter = column_cells[0].column_letter
                max_length = max(len(str(cell.value or "")) for cell in column_cells)
                sheet.column_dimensions[letter].width = min(max(max_length + 2, 12), 48)

        workbook.save(SITE_XLSX)


def sync_trump_raw_repo() -> None:
    if not SYNC_REPO_SCRIPT.exists():
        return
    subprocess.run([sys.executable, str(SYNC_REPO_SCRIPT)], cwd=str(WORKSPACE_ROOT), check=True)


def main() -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    daily = pd.read_csv(SOURCE_DAILY_CSV, low_memory=False)
    summary = json.loads(SOURCE_SUMMARY_JSON.read_text(encoding="utf-8"))

    legacy_csv = SITE_DATA_DIR / "trump_daily_indices_llm.csv"
    if legacy_csv.exists():
        legacy_csv.unlink()
    compact_payload = build_compact_json(daily, summary)
    SITE_JSON.write_text(json.dumps(compact_payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    write_workbook(daily, summary)
    build_directed_assets()
    sync_trump_raw_repo()

    print(f"[OK] saved: {SITE_JSON}")
    print(f"[OK] saved: {SITE_XLSX}")


if __name__ == "__main__":
    main()
