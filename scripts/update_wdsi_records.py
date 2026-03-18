from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import requests

from build_wdsi_data import main as build_site_data
from wdsi_pipeline import (
    ChinaMfaRegularPressSource,
    FranceMfaSpokespersonSource,
    JapanMofaPressReleaseSource,
    KoreaMofaPressReleaseSource,
    OpenAIWDSIScorer,
    RussiaMfaNewsSource,
    UkFcdoNewsSource,
    UsStateDepartmentSource,
)


ROOT = Path(__file__).resolve().parents[1]
RECORDS_DIR = ROOT / "records"

SUPPORTED_COUNTRIES = {"CN", "US", "UK", "JP", "KR", "FR", "RU"}


def load_records(path: Path) -> pd.DataFrame:
    if path.exists():
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        if "score" in frame.columns:
            frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
        if "pipeline_version" not in frame.columns:
            frame["pipeline_version"] = ""
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
            "pipeline_version",
            "response_id",
            "scored_at",
            "content_hash",
            "is_legacy",
        ]
    )


def pending_records(
    existing: pd.DataFrame,
    fetched: list[dict[str, object]],
    pipeline_version: str,
) -> tuple[list[dict[str, object]], set[str]]:
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
            if (
                str(current.get("content_hash", "")) == str(record["content_hash"])
                and str(current.get("pipeline_version", "")) == pipeline_version
            ):
                continue
            replaced_urls.add(url)
            additions.append(record)
            continue

        key = (str(record["published_at"]), str(record["content_hash"]))
        if key in existing_keys:
            continue
        additions.append(record)

    return additions, replaced_urls


def determine_fetch_range(existing: pd.DataFrame, lookback_days: int) -> tuple[str, str]:
    today = datetime.now(timezone.utc).date()
    window_start = today - timedelta(days=max(lookback_days, 1))

    legacy_dates = pd.to_datetime(
        existing.loc[existing["is_legacy"].astype(str).str.lower() == "true", "published_at"],
        errors="coerce",
    ).dropna()
    if len(legacy_dates):
        start_date = max(window_start, (legacy_dates.max().date() + timedelta(days=1)))
    else:
        start_date = window_start

    return start_date.isoformat(), today.isoformat()


def make_sources(session: requests.Session, countries: list[str]) -> dict[str, object]:
    sources: dict[str, object] = {}
    if "CN" in countries:
        sources["CN"] = ChinaMfaRegularPressSource(session)
    if "US" in countries:
        sources["US"] = UsStateDepartmentSource(session)
    if "UK" in countries:
        sources["UK"] = UkFcdoNewsSource(session)
    if "JP" in countries:
        sources["JP"] = JapanMofaPressReleaseSource(session)
    if "KR" in countries:
        sources["KR"] = KoreaMofaPressReleaseSource(session)
    if "FR" in countries:
        sources["FR"] = FranceMfaSpokespersonSource(session)
    if "RU" in countries:
        sources["RU"] = RussiaMfaNewsSource(session)
    return sources


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch, score, and store new WDSI records.")
    parser.add_argument("--countries", default="CN,US", help="Comma-separated country codes to update.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and diff records without scoring or writing.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=200,
        help="How many recent days of source history to refetch and reconcile.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=90,
        help="Maximum pages for source APIs that need explicit pagination.",
    )
    parser.add_argument("--start-date", help="Override fetch start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="Override fetch end date in YYYY-MM-DD format.")
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Update records without rebuilding site data.",
    )
    args = parser.parse_args()

    countries = [code.strip().upper() for code in args.countries.split(",") if code.strip()]
    unsupported = [code for code in countries if code not in SUPPORTED_COUNTRIES]
    if unsupported:
        raise SystemExit(f"Unsupported automated countries: {', '.join(sorted(unsupported))}")

    RECORDS_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    sources = make_sources(session, countries)

    scorer = None if args.dry_run else OpenAIWDSIScorer()
    pipeline_version = OpenAIWDSIScorer.pipeline_version if scorer is None else scorer.pipeline_version
    changed = False

    for code in countries:
        source = sources[code]
        destination = RECORDS_DIR / f"{code}.csv"
        existing = load_records(destination)
        if args.start_date or args.end_date:
            today = datetime.now(timezone.utc).date().isoformat()
            start_date = args.start_date or determine_fetch_range(existing, args.lookback_days)[0]
            end_date = args.end_date or today
        else:
            start_date, end_date = determine_fetch_range(existing, args.lookback_days)

        if code == "CN":
            fetched_records = source.fetch_between(start_date, end_date)
        elif code in {"US", "UK", "KR", "FR", "RU"}:
            fetched_records = source.fetch_between(start_date, end_date, max_pages=args.max_pages)
        else:
            fetched_records = source.fetch_between(start_date, end_date)

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

        additions, replaced_urls = pending_records(existing, fetched_rows, pipeline_version)
        print(
            f"{code}: fetched {len(fetched_rows)} records for {start_date} to {end_date}, "
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
        if code == "CN":
            batched_inputs = [SimpleNamespace(**item) for item in additions]
            batched_results = scorer.score_conference_records(batched_inputs)  # type: ignore[union-attr]
            for item, result in zip(additions, batched_results, strict=False):
                scored_rows.append(
                    {
                        **item,
                        "score": result["score"],
                        "score_reasoning": result["score_reasoning"],
                        "war_related": result["war_related"],
                        "confidence": result["confidence"],
                        "model": result["model"],
                        "pipeline_version": result["pipeline_version"],
                        "response_id": result["response_id"],
                        "scored_at": result["scored_at"],
                    }
                )
        elif code in {"US", "UK", "JP", "KR", "FR", "RU"}:
            batched_inputs = [SimpleNamespace(**item) for item in additions]
            batched_results = scorer.score_flat_records(batched_inputs)  # type: ignore[union-attr]
            for item, result in zip(additions, batched_results, strict=False):
                scored_rows.append(
                    {
                        **item,
                        "score": result["score"],
                        "score_reasoning": result["score_reasoning"],
                        "war_related": result["war_related"],
                        "confidence": result["confidence"],
                        "model": result["model"],
                        "pipeline_version": result["pipeline_version"],
                        "response_id": result["response_id"],
                        "scored_at": result["scored_at"],
                    }
                )
        else:
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
                        "pipeline_version": result["pipeline_version"],
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

    if changed and not args.skip_build:
        build_site_data()
        print("Rebuilt site JSON/CSV assets.")
    elif changed:
        print("Record changes written without rebuilding site data.")
    else:
        print("No record changes detected.")


if __name__ == "__main__":
    main()
