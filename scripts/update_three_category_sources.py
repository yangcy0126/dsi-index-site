from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from openpyxl.utils.exceptions import IllegalCharacterError

from wdsi_pipeline import (
    AustraliaForeignMinisterMediaReleaseSource,
    BrazilItamaratyPressReleaseSource,
    CanadaGlobalAffairsNewsSource,
    ChinaMfaRegularPressSource,
    FranceMfaSpokespersonSource,
    GermanyForeignOfficeSource,
    IndiaMeaOfficialSource,
    ItalyMfaPressReleaseSource,
    JapanMofaPressReleaseSource,
    KoreaMofaPressReleaseSource,
    MexicoSrePressArchiveSource,
    RussiaMfaNewsSource,
    SpainMfaComunicadosSource,
    UkFcdoNewsSource,
    UsStateDepartmentSource,
    clean_text,
    normalize_compare_text,
    normalize_generic_url,
)


SITE_ROOT = Path(__file__).resolve().parents[1]
DSI_ROOT = SITE_ROOT.parent / "DSI-ICF"
LOCAL_TEXT_ROOT = DSI_ROOT / "data" / "外交部文本数据_15国本地"
OVERLAY_ROOT = DSI_ROOT / "data" / "derived" / "local_text_overlays"

COUNTRY_NAMES = {
    "AU": "Australia",
    "BR": "Brazil",
    "CA": "Canada",
    "CN": "China",
    "DE": "Germany",
    "ES": "Spain",
    "FR": "France",
    "IN": "India",
    "IT": "Italy",
    "JP": "Japan",
    "KR": "Korea",
    "MX": "Mexico",
    "RU": "Russia",
    "UK": "United Kingdom",
    "US": "United States",
}

SOURCE_CLASSES = {
    "AU": AustraliaForeignMinisterMediaReleaseSource,
    "BR": BrazilItamaratyPressReleaseSource,
    "CA": CanadaGlobalAffairsNewsSource,
    "CN": ChinaMfaRegularPressSource,
    "DE": GermanyForeignOfficeSource,
    "ES": SpainMfaComunicadosSource,
    "FR": FranceMfaSpokespersonSource,
    "IN": IndiaMeaOfficialSource,
    "IT": ItalyMfaPressReleaseSource,
    "JP": JapanMofaPressReleaseSource,
    "KR": KoreaMofaPressReleaseSource,
    "MX": MexicoSrePressArchiveSource,
    "RU": RussiaMfaNewsSource,
    "UK": UkFcdoNewsSource,
    "US": UsStateDepartmentSource,
}

OUTPUT_COLUMNS = [
    "time",
    "published_at",
    "title",
    "name",
    "speaker",
    "url",
    "content",
    "source_kind",
    "language",
    "country_code",
    "country",
    "content_chars",
    "content_hash",
    "origin",
    "collected_at",
]


def normalize_url(value: object) -> str:
    text = clean_text(str(value or ""))
    if not text:
        return ""
    try:
        return normalize_generic_url(text)
    except Exception:
        return text


def normalize_date(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.date().isoformat()


def content_hash(value: object) -> str:
    return hashlib.sha1(clean_text(str(value or "")).encode("utf-8")).hexdigest()


def standardize(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    for column in OUTPUT_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    working["published_at"] = working["published_at"].map(normalize_date)
    working["time"] = working["published_at"]
    for column in ("title", "name", "speaker", "content", "source_kind", "language", "origin", "collected_at"):
        working[column] = working[column].map(lambda value: clean_text(str(value or "")))
    working["url"] = working["url"].map(normalize_url)
    working["country_code"] = working["country_code"].astype(str).str.upper()
    working["content_chars"] = working["content"].map(len)
    working["content_hash"] = working["content"].map(content_hash)
    return working[OUTPUT_COLUMNS].copy()


def row_key(row: pd.Series) -> str:
    url = normalize_url(row.get("url", ""))
    if url:
        return f"url::{url}"
    return "hash::{date}::{title}::{digest}".format(
        date=row.get("published_at", ""),
        title=clean_text(str(row.get("title", ""))),
        digest=row.get("content_hash", "") or content_hash(row.get("content", "")),
    )


def dedupe(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    working = standardize(frame)
    working["_key"] = [row_key(row) for _, row in working.iterrows()]
    working = working.drop_duplicates(subset=["_key"], keep="last").drop(columns=["_key"])
    working["_date"] = pd.to_datetime(working["published_at"], errors="coerce")
    working = working.sort_values(["_date", "title", "url"]).drop(columns=["_date"])
    return standardize(working.reset_index(drop=True))


def load_local(code: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    canonical = LOCAL_TEXT_ROOT / code / "texts.csv"
    if canonical.exists():
        frames.append(pd.read_csv(canonical, dtype=str, keep_default_na=False, low_memory=False))
    overlay_dir = OVERLAY_ROOT / code
    if overlay_dir.exists():
        for path in sorted(overlay_dir.glob("*.csv")):
            frames.append(pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False))
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return dedupe(pd.concat(frames, ignore_index=True))


def configure_source(code: str, source: object, existing: pd.DataFrame) -> None:
    if code != "RU":
        return
    source.known_urls = {
        normalize_url(value) for value in existing.get("url", pd.Series(dtype=str)) if normalize_url(value)
    }
    source.known_title_keys = {
        (str(row.get("published_at", "")), normalize_compare_text(str(row.get("title", ""))))
        for row in existing.to_dict(orient="records")
        if str(row.get("published_at", "")).strip() and str(row.get("title", "")).strip()
    }


def fetch_records(code: str, source: object, start_date: str, end_date: str, max_pages: int) -> list[object]:
    try:
        return source.fetch_between(start_date, end_date, max_pages=max_pages)
    except TypeError:
        return source.fetch_between(start_date, end_date)


def records_frame(code: str, records: list[object]) -> pd.DataFrame:
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    rows = []
    for record in records:
        rows.append(
            {
                "published_at": record.published_at,
                "title": record.title,
                "name": record.speaker,
                "speaker": record.speaker,
                "url": record.url,
                "content": record.content,
                "source_kind": record.source_kind,
                "language": record.language,
                "country_code": code,
                "country": COUNTRY_NAMES[code],
                "origin": "latest_official_refresh_20260806",
                "collected_at": collected_at,
            }
        )
    return standardize(pd.DataFrame(rows)) if rows else pd.DataFrame(columns=OUTPUT_COLUMNS)


def save_local(code: str, frame: pd.DataFrame) -> None:
    destination = LOCAL_TEXT_ROOT / code
    destination.mkdir(parents=True, exist_ok=True)
    csv_path = destination / "texts.csv"
    xlsx_path = destination / "texts.xlsx"
    manifest_path = destination / "manifest.json"
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        frame.to_excel(xlsx_path, index=False)
    except (IllegalCharacterError, ValueError):
        pass
    dates = pd.to_datetime(frame["published_at"], errors="coerce").dropna()
    manifest = {
        "country_code": code,
        "country": COUNTRY_NAMES[code],
        "row_count": int(len(frame)),
        "start_date": dates.min().date().isoformat() if len(dates) else "",
        "end_date": dates.max().date().isoformat() if len(dates) else "",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync official source texts into the local three-category corpus.")
    parser.add_argument("--countries", required=True, help="Comma-separated country codes.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--max-pages", type=int, default=120)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    countries = [value.strip().upper() for value in args.countries.split(",") if value.strip()]
    unsupported = sorted(set(countries) - set(SOURCE_CLASSES))
    if unsupported:
        raise SystemExit(f"Unsupported countries: {', '.join(unsupported)}")

    for code in countries:
        existing = load_local(code)
        source = SOURCE_CLASSES[code](requests.Session())
        configure_source(code, source, existing)
        records = fetch_records(code, source, args.start_date, args.end_date, args.max_pages)
        fetched = records_frame(code, records)
        existing_keys = {row_key(row) for _, row in existing.iterrows()}
        fetched_keys = {row_key(row) for _, row in fetched.iterrows()}
        new_keys = fetched_keys - existing_keys
        merged = dedupe(pd.concat([existing, fetched], ignore_index=True))
        latest = fetched["published_at"].max() if not fetched.empty else ""
        print(
            f"{code}: fetched={len(fetched)} new_or_updated={len(new_keys)} "
            f"rows={len(existing)}->{len(merged)} latest={latest}"
        )
        if not args.dry_run and not fetched.empty:
            save_local(code, merged)


if __name__ == "__main__":
    main()
