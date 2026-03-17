from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import requests

from build_wdsi_data import main as build_site_data
from wdsi_pipeline import ChinaMfaRegularPressSource, OpenAIWDSIScorer, UsStateDepartmentSource


ROOT = Path(__file__).resolve().parents[1]
RECORDS_DIR = ROOT / "records"

SUPPORTED_COUNTRIES = {"CN", "US"}


def load_records(path: Path) -> pd.DataFrame:
    if path.exists():
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        if "score" in frame.columns:
            frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
        return frame

    return pd.DataFrame(
        columns=[
            "record_id",
            "country_code",
            "published_at",
            "url",
            "title",
            "speaker",
            "content_chars",
            "score",
            "score_reasoning",
            "war_related",
            "confidence",
            "source_kind",
            "language",
            "model",
            "response_id",
            "scored_at",
            "content_hash",
            "is_legacy",
        ]
    )


def pending_records(existing: pd.DataFrame, fetched: list[dict[str, object]]) -> tuple[list[dict[str, object]], set[str]]:
    existing_by_url = {
        row["url"]: row
        for row in existing.to_dict(orient="records")
        if row.get("url")
    }
    existing_keys = {
        (row.get("published_at"), row.get("content_hash"))
        for row in existing.to_dict(orient="records")
    }

    additions: list[dict[str, object]] = []
    replaced_urls: set[str] = set()

    for record in fetched:
        url = str(record["url"])
        current = existing_by_url.get(url)
        if current:
            if current.get("content_hash") == record["content_hash"]:
                continue
            replaced_urls.add(url)
            additions.append(record)
            continue

        key = (str(record["published_at"]), str(record["content_hash"]))
        if key in existing_keys:
            continue
        additions.append(record)

    return additions, replaced_urls


def make_sources(session: requests.Session, countries: list[str]) -> dict[str, object]:
    sources: dict[str, object] = {}
    if "CN" in countries:
        sources["CN"] = ChinaMfaRegularPressSource(session)
    if "US" in countries:
        sources["US"] = UsStateDepartmentSource(session)
    return sources


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch, score, and store new WDSI records.")
    parser.add_argument("--countries", default="CN,US", help="Comma-separated country codes to update.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and diff records without scoring or writing.")
    parser.add_argument("--max-pages", type=int, default=3, help="How many recent listing pages to inspect per source.")
    args = parser.parse_args()

    countries = [code.strip().upper() for code in args.countries.split(",") if code.strip()]
    unsupported = [code for code in countries if code not in SUPPORTED_COUNTRIES]
    if unsupported:
        raise SystemExit(f"Unsupported automated countries: {', '.join(sorted(unsupported))}")

    RECORDS_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    sources = make_sources(session, countries)

    scorer = None if args.dry_run else OpenAIWDSIScorer()
    changed = False

    for code in countries:
        source = sources[code]
        destination = RECORDS_DIR / f"{code}.csv"
        existing = load_records(destination)

        if code == "CN":
            fetched_records = source.fetch_recent(max_pages=args.max_pages)
        else:
            fetched_records = source.fetch_recent(max_pages=min(args.max_pages, 2))

        fetched_rows = [
            {
                "record_id": record.record_id,
                "country_code": record.country_code,
                "published_at": record.published_at,
                "url": record.url,
                "title": record.title,
                "speaker": record.speaker,
                "content": record.content,
                "content_chars": len(record.content),
                "source_kind": record.source_kind,
                "language": record.language,
                "content_hash": record.content_hash,
                "is_legacy": False,
            }
            for record in fetched_records
        ]

        additions, replaced_urls = pending_records(existing, fetched_rows)
        print(
            f"{code}: fetched {len(fetched_rows)} recent records, "
            f"{len(additions)} new or updated records, {len(replaced_urls)} replacements."
        )

        if args.dry_run:
            for item in additions[:10]:
                preview = f"  - {item['published_at']} | {item['title']}"
                print(preview.encode("ascii", "replace").decode())
            continue

        if not additions:
            continue

        scored_rows: list[dict[str, object]] = []
        for item in additions:
            result = scorer.score_record(SimpleNamespace(**item))  # type: ignore[union-attr]
            scored_rows.append(
                {
                    **item,
                    "score": result["score"],
                    "score_reasoning": result["score_reasoning"],
                    "war_related": result["war_related"],
                    "confidence": result["confidence"],
                    "model": result["model"],
                    "response_id": result["response_id"],
                    "scored_at": result["scored_at"],
                }
            )

        for row in scored_rows:
            row.pop("content", None)

        updated = existing
        if replaced_urls:
            updated = updated.loc[~updated["url"].isin(replaced_urls)].copy()

        updated = pd.concat([updated, pd.DataFrame(scored_rows)], ignore_index=True)
        updated = updated.drop_duplicates(subset=["record_id"], keep="last")
        updated["score"] = pd.to_numeric(updated["score"], errors="coerce")
        updated = updated.sort_values(["published_at", "record_id"]).reset_index(drop=True)
        updated.to_csv(destination, index=False, encoding="utf-8")
        print(f"{code}: wrote {len(updated)} rows to {destination.name}")
        changed = True

    if args.dry_run:
        return

    if changed:
        build_site_data()
        print("Rebuilt site JSON/CSV assets.")
    else:
        print("No record changes detected.")


if __name__ == "__main__":
    main()
