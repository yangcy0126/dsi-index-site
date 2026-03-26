from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import requests

from build_wdsi_data import main as build_site_data
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
    OpenAIWDSIScorer,
    RussiaMfaNewsSource,
    SpainMfaComunicadosSource,
    UkFcdoNewsSource,
    UsStateDepartmentSource,
)


ROOT = Path(__file__).resolve().parents[1]
RECORDS_DIR = ROOT / "records"

SUPPORTED_COUNTRIES = {"CN", "US", "UK", "JP", "KR", "FR", "RU", "DE", "IN", "IT", "CA", "BR", "AU", "MX", "ES"}


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
            metadata_matches = (
                str(current.get("published_at", "")) == str(record.get("published_at", ""))
                and str(current.get("title", "")) == str(record.get("title", ""))
                and str(current.get("speaker", "")) == str(record.get("speaker", ""))
                and str(current.get("source_kind", "")) == str(record.get("source_kind", ""))
                and str(current.get("language", "")) == str(record.get("language", ""))
            )
            if (
                str(current.get("content_hash", "")) == str(record["content_hash"])
                and str(current.get("pipeline_version", "")) == pipeline_version
                and metadata_matches
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


def reuse_scored_rows(
    existing: pd.DataFrame,
    additions: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if existing.empty or not additions:
        return [], additions

    existing_by_url = {
        str(row.get("url", "")): row
        for row in existing.to_dict(orient="records")
        if str(row.get("url", "")).strip()
    }

    reused_rows: list[dict[str, object]] = []
    needs_scoring: list[dict[str, object]] = []

    score_fields = (
        "score",
        "score_reasoning",
        "war_related",
        "confidence",
        "model",
        "pipeline_version",
        "response_id",
        "scored_at",
    )

    for item in additions:
        current = existing_by_url.get(str(item.get("url", "")))
        if current is None:
            needs_scoring.append(item)
            continue

        score_value = current.get("score")
        if pd.isna(score_value):
            needs_scoring.append(item)
            continue

        # Safe reuse path: identical content and title/date, only metadata like source_kind or speaker changed.
        if (
            str(current.get("content_hash", "")) == str(item.get("content_hash", ""))
            and str(current.get("published_at", "")) == str(item.get("published_at", ""))
            and str(current.get("title", "")) == str(item.get("title", ""))
        ):
            reused = {**item}
            for field in score_fields:
                reused[field] = current.get(field, "")
            reused_rows.append(reused)
            continue

        needs_scoring.append(item)

    return reused_rows, needs_scoring


def effective_legacy_mask(existing: pd.DataFrame, source: object | None = None) -> pd.Series:
    if existing.empty:
        return pd.Series(dtype=bool)

    mask = existing.get("is_legacy", pd.Series(dtype=str)).astype(str).str.lower() == "true"
    legacy_source_kinds = {
        str(kind)
        for kind in (getattr(source, "legacy_source_kinds", ()) or ())
        if str(kind).strip()
    }
    if legacy_source_kinds and "source_kind" in existing.columns:
        mask = mask | existing["source_kind"].astype(str).isin(legacy_source_kinds)

    legacy_period_end_date = str(getattr(source, "legacy_period_end_date", "") or "")
    if legacy_period_end_date and "published_at" in existing.columns:
        published_dates = pd.to_datetime(existing["published_at"], errors="coerce")
        cutoff = datetime.fromisoformat(legacy_period_end_date).date()
        mask = mask & published_dates.notna() & (published_dates.dt.date <= cutoff)

    return mask


def determine_fetch_range(existing: pd.DataFrame, lookback_days: int, source: object | None = None) -> tuple[str, str]:
    today = datetime.now(timezone.utc).date()
    window_start = today - timedelta(days=max(lookback_days, 1))

    legacy_dates = pd.to_datetime(
        existing.loc[effective_legacy_mask(existing, source), "published_at"],
        errors="coerce",
    ).dropna()
    if len(legacy_dates):
        start_date = max(window_start, (legacy_dates.max().date() + timedelta(days=1)))
    else:
        start_date = window_start

    return start_date.isoformat(), today.isoformat()


def maybe_expand_history_start(existing: pd.DataFrame, source: object, start_date: str) -> str:
    bootstrap_start_date = str(getattr(source, "bootstrap_history_start_date", "") or "")
    history_start_date = str(getattr(source, "history_start_date", "") or "")
    history_backfill_chunk_days = int(getattr(source, "history_backfill_chunk_days", 0) or 0)
    if not bootstrap_start_date and not history_start_date:
        return start_date

    published_dates = pd.to_datetime(existing.get("published_at"), errors="coerce").dropna()
    if published_dates.empty:
        return bootstrap_start_date or history_start_date

    if not bool(getattr(source, "resume_missing_history", False)):
        return start_date

    earliest_existing = published_dates.min().date().isoformat()
    if earliest_existing > history_start_date and start_date > history_start_date:
        if history_backfill_chunk_days > 0:
            chunk_start = max(
                datetime.fromisoformat(history_start_date).date(),
                published_dates.min().date() - timedelta(days=history_backfill_chunk_days),
            )
            return chunk_start.isoformat()
        return history_start_date
    return start_date


def configure_source_state(code: str, source: object, existing: pd.DataFrame) -> None:
    if code == "RU":
        source.known_urls = {
            str(url)
            for url in existing.get("url", pd.Series(dtype=str)).tolist()
            if str(url).strip()
        }
        source.known_title_keys = {
            (
                str(row.get("published_at", "")),
                source._normalize_compare_text(str(row.get("title", ""))),
            )
            for row in existing.to_dict(orient="records")
            if str(row.get("published_at", "")).strip() and str(row.get("title", "")).strip()
        }
        return

    if code == "US":
        source.known_url_dates = {
            str(row.get("url", "")).strip(): str(row.get("published_at", "")).strip()
            for row in existing.to_dict(orient="records")
            if str(row.get("url", "")).strip() and str(row.get("published_at", "")).strip()
        }


def build_fetch_plan(
    existing: pd.DataFrame,
    source: object,
    lookback_days: int,
    history_backfill_rounds: int,
    start_date_override: str | None,
    end_date_override: str | None,
    history_only: bool,
) -> list[tuple[str, str, str]]:
    if start_date_override or end_date_override:
        today = datetime.now(timezone.utc).date().isoformat()
        recent_start, _ = determine_fetch_range(existing, lookback_days, source)
        return [("requested", start_date_override or recent_start, end_date_override or today)]

    recent_start, recent_end = determine_fetch_range(existing, lookback_days, source)
    published_dates = pd.to_datetime(existing.get("published_at"), errors="coerce").dropna()
    if published_dates.empty:
        bootstrap_start = maybe_expand_history_start(existing, source, recent_start)
        return [("bootstrap", bootstrap_start, recent_end)]

    plan: list[tuple[str, str, str]] = []
    if not history_only:
        plan.append(("recent", recent_start, recent_end))
    history_start_date = str(getattr(source, "history_start_date", "") or "")
    if history_backfill_rounds <= 0 or not history_start_date:
        return plan

    history_floor = datetime.fromisoformat(history_start_date).date()

    chunk_days = int(getattr(source, "history_backfill_chunk_days", 0) or 0)

    legacy_mask = effective_legacy_mask(existing, source)
    legacy_dates = pd.to_datetime(existing.loc[legacy_mask, "published_at"], errors="coerce").dropna()
    modern_dates = pd.to_datetime(existing.loc[~legacy_mask, "published_at"], errors="coerce").dropna()
    if bool(getattr(source, "resume_gap_after_legacy", False)) and len(legacy_dates):
        history_floor = max(history_floor, legacy_dates.max().date() + timedelta(days=1))

    if bool(getattr(source, "resume_scope_history", False)) and "source_kind" in existing.columns:
        scope_start_date = str(getattr(source, "scope_history_start_date", "") or history_start_date)
        scope_floor = max(history_floor, datetime.fromisoformat(scope_start_date).date())
        scope_kinds = {
            str(kind)
            for kind in (getattr(source, "scope_history_source_kinds", ()) or ())
            if str(kind).strip()
        }
        anchor_dates = pd.to_datetime(
            existing.loc[existing["source_kind"].astype(str).isin(scope_kinds), "published_at"],
            errors="coerce",
        ).dropna()
        if len(anchor_dates):
            scope_backfill_end = anchor_dates.min().date() - timedelta(days=1)
        else:
            scope_backfill_end = datetime.fromisoformat(recent_start).date() - timedelta(days=1)

        for round_index in range(history_backfill_rounds):
            if scope_backfill_end < scope_floor:
                break
            if chunk_days > 0:
                backfill_start = max(scope_floor, scope_backfill_end - timedelta(days=chunk_days))
            else:
                backfill_start = scope_floor
            plan.append((f"history-{round_index + 1}", backfill_start.isoformat(), scope_backfill_end.isoformat()))
            if backfill_start <= scope_floor:
                break
            scope_backfill_end = backfill_start - timedelta(days=1)

        if len(plan) > (0 if history_only else 1):
            return plan

    if not bool(getattr(source, "resume_missing_history", False)):
        return plan

    backfill_end = published_dates.min().date() - timedelta(days=1)
    if bool(getattr(source, "resume_gap_after_legacy", False)) and len(legacy_dates) and len(modern_dates):
        gap_floor = max(history_floor, legacy_dates.max().date() + timedelta(days=1))
        gap_end = modern_dates.min().date() - timedelta(days=1)
        if gap_end >= gap_floor:
            history_floor = gap_floor
            backfill_end = gap_end
    if backfill_end < history_floor:
        return plan

    for round_index in range(history_backfill_rounds):
        if backfill_end < history_floor:
            break
        if chunk_days > 0:
            backfill_start = max(history_floor, backfill_end - timedelta(days=chunk_days))
        else:
            backfill_start = history_floor
        plan.append((f"history-{round_index + 1}", backfill_start.isoformat(), backfill_end.isoformat()))
        if backfill_start <= history_floor:
            break
        backfill_end = backfill_start - timedelta(days=1)

    return plan


def fetch_records_for_window(
    code: str,
    source: object,
    start_date: str,
    end_date: str,
    max_pages: int,
) -> list[object]:
    if code == "CN":
        return source.fetch_between(start_date, end_date)
    if code in {"US", "UK", "KR", "DE", "IN", "IT", "CA", "BR", "AU", "MX", "ES", "FR", "RU"}:
        return source.fetch_between(start_date, end_date, max_pages=max_pages)
    return source.fetch_between(start_date, end_date)


def record_is_legacy(source: object, record: object) -> bool:
    if isinstance(record, dict):
        kind = str(record.get("source_kind", "")).strip()
    else:
        kind = str(getattr(record, "source_kind", "")).strip()
    if kind.startswith("legacy_"):
        return True

    legacy_source_kinds = {
        str(item)
        for item in (getattr(source, "legacy_source_kinds", ()) or ())
        if str(item).strip()
    }
    if kind in legacy_source_kinds:
        return True

    return False


def score_pending_rows(
    code: str,
    additions: list[dict[str, object]],
    scorer: OpenAIWDSIScorer,
) -> list[dict[str, object]]:
    def is_recoverable_scoring_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "data_inspection_failed",
                "input data may contain inappropriate content",
                "inappropriate content",
            )
        )

    def score_flat_with_recovery(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        if not rows:
            return []
        batched_inputs = [SimpleNamespace(**item) for item in rows]
        try:
            batched_results = scorer.score_flat_records(batched_inputs)
        except Exception as exc:
            # Isolate records that trip provider-side content inspection without dropping the whole batch.
            if not is_recoverable_scoring_error(exc):
                raise
            if len(rows) == 1:
                item = rows[0]
                print(f"{code}: skipping {item['published_at']} | {item['title']} after scoring error: {exc}")
                return []
            midpoint = len(rows) // 2
            return score_flat_with_recovery(rows[:midpoint]) + score_flat_with_recovery(rows[midpoint:])

        recovered_rows: list[dict[str, object]] = []
        for item, result in zip(rows, batched_results, strict=False):
            recovered_rows.append(
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
        return recovered_rows

    scored_rows: list[dict[str, object]] = []
    if code == "CN":
        batched_inputs = [SimpleNamespace(**item) for item in additions]
        batched_results = scorer.score_conference_records(batched_inputs)
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
        return scored_rows

    if code in {"US", "UK", "JP", "KR", "DE", "IN", "IT", "CA", "BR", "AU", "MX", "ES", "FR", "RU"}:
        return score_flat_with_recovery(additions)

    for item in additions:
        result = scorer.score_record(SimpleNamespace(**item))
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
    return scored_rows


def max_pages_for_window(source: object, default_max_pages: int, window_label: str) -> int:
    if not window_label.startswith("history"):
        return default_max_pages
    history_max_pages = int(getattr(source, "history_max_pages", 0) or 0)
    if history_max_pages <= 0:
        return default_max_pages
    return max(default_max_pages, history_max_pages)


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
    if "DE" in countries:
        sources["DE"] = GermanyForeignOfficeSource(session)
    if "IN" in countries:
        sources["IN"] = IndiaMeaOfficialSource(session)
    if "IT" in countries:
        sources["IT"] = ItalyMfaPressReleaseSource(session)
    if "CA" in countries:
        sources["CA"] = CanadaGlobalAffairsNewsSource(session)
    if "BR" in countries:
        sources["BR"] = BrazilItamaratyPressReleaseSource(session)
    if "AU" in countries:
        sources["AU"] = AustraliaForeignMinisterMediaReleaseSource(session)
    if "MX" in countries:
        sources["MX"] = MexicoSrePressArchiveSource(session)
    if "ES" in countries:
        sources["ES"] = SpainMfaComunicadosSource(session)
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
        "--history-backfill-rounds",
        type=int,
        default=1,
        help="How many additional historical chunks to backfill beyond the recent refresh window.",
    )
    parser.add_argument(
        "--history-only",
        action="store_true",
        help="Skip the recent refresh window and only backfill older history chunks.",
    )
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

    scorer = None
    pipeline_version = OpenAIWDSIScorer.pipeline_version
    changed = False

    for code in countries:
        source = sources[code]
        destination = RECORDS_DIR / f"{code}.csv"
        existing = load_records(destination)
        fetch_plan = build_fetch_plan(
            existing,
            source,
            args.lookback_days,
            args.history_backfill_rounds,
            args.start_date,
            args.end_date,
            args.history_only,
        )

        for window_label, start_date, end_date in fetch_plan:
            configure_source_state(code, source, existing)
            effective_max_pages = max_pages_for_window(source, args.max_pages, window_label)
            fetched_records = fetch_records_for_window(code, source, start_date, end_date, effective_max_pages)
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
                    "is_legacy": record_is_legacy(source, record),
                }
                for record in fetched_records
            ]

            additions, replaced_urls = pending_records(existing, fetched_rows, pipeline_version)
            print(
                f"{code} [{window_label}]: fetched {len(fetched_rows)} records for {start_date} to {end_date}, "
                f"{len(additions)} new or updated records, {len(replaced_urls)} replacements."
            )

            if args.dry_run:
                for item in additions[:10]:
                    preview = f"  - {item['published_at']} | {item['title']}"
                    print(preview.encode("ascii", "replace").decode())
                continue

            if not additions:
                continue

            reused_rows, additions_to_score = reuse_scored_rows(existing, additions)
            if reused_rows:
                print(f"{code} [{window_label}]: reused scores for {len(reused_rows)} metadata-only replacements.")

            scored_rows = list(reused_rows)
            if additions_to_score:
                if scorer is None:
                    scorer = OpenAIWDSIScorer()
                    pipeline_version = scorer.pipeline_version
                scored_rows.extend(score_pending_rows(code, additions_to_score, scorer))

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
            print(f"{code} [{window_label}]: wrote {len(updated)} rows to {destination.name}")
            existing = updated
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
