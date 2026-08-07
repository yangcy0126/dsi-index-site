from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


SITE_ROOT = Path(__file__).resolve().parents[1]
DSI_DATA_ROOT = SITE_ROOT.parent / "DSI-ICF" / "data"
ARCHIVE_ROOT = DSI_DATA_ROOT / "ARCHIVE_OLD_DSI_INDEX_VERSIONS_20260806" / "pre_update_remaining12"
CN_US_KR_ARCHIVE_ROOT = (
    DSI_DATA_ROOT
    / "ARCHIVE_OLD_DSI_INDEX_VERSIONS_20260806"
    / "pre_update_cn_us_kr"
    / "scores"
)
RECORD_ROOT = SITE_ROOT / "records"
RECORD_COLUMNS = [
    "record_id", "country_code", "published_at", "url", "title", "speaker", "content_chars",
    "score", "score_reasoning", "war_related", "confidence", "source_kind", "language", "model",
    "pipeline_version", "response_id", "scored_at", "content_hash", "is_legacy",
]


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)


def normalize_url(value: object) -> str:
    return str(value or "").strip().rstrip("/")


def identity(row: pd.Series) -> str:
    url = normalize_url(row.get("url", ""))
    return f"url::{url}" if url else f"key::{row.get('score_key', '')}"


def find_live_score(code: str) -> Path:
    candidates = [
        path for path in DSI_DATA_ROOT.rglob(f"{code}_scores.csv") if "ARCHIVE_" not in str(path)
    ]
    if len(candidates) != 1:
        raise RuntimeError(f"Expected one live score file for {code}, found {candidates}")
    return candidates[0]


def find_archived_score(code: str) -> Path:
    candidates = [
        ARCHIVE_ROOT / f"{code}_scores.csv",
        CN_US_KR_ARCHIVE_ROOT / f"{code}_scores.csv",
    ]
    matches = [path for path in candidates if path.exists()]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one archived score file for {code}, found {matches}")
    return matches[0]


def changed_accepted_scores(code: str) -> pd.DataFrame:
    current = read_csv(find_live_score(code))
    if "score_status" in current.columns:
        current = current.loc[current["score_status"].eq("accepted")].copy()
    archived = read_csv(find_archived_score(code))
    archived["_identity"] = [identity(row) for _, row in archived.iterrows()]
    old_lookup = archived.drop_duplicates("_identity", keep="last").set_index("_identity")

    keep: list[bool] = []
    for _, row in current.iterrows():
        key = identity(row)
        if key not in old_lookup.index:
            keep.append(True)
            continue
        previous = old_lookup.loc[key]
        current_date = str(row.get("published_at", "") or row.get("time", ""))
        previous_date = str(previous.get("published_at", "") or previous.get("time", ""))
        unchanged = (
            str(row.get("content_hash", "")) == str(previous.get("content_hash", ""))
            and current_date == previous_date
            and str(previous.get("score_status", "accepted")) == "accepted"
            and all(str(row.get(column, "")) == str(previous.get(column, "")) for column in ("c1", "c2", "c3"))
        )
        keep.append(not unchanged)
    return current.loc[keep].copy()


def record_id(code: str, published_at: str, url: str, content_hash: str) -> str:
    seed = f"{code}|{published_at}|{url or content_hash}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def build_record(code: str, row: pd.Series, scored_at: str) -> dict[str, object]:
    published_at = str(row.get("published_at", "") or row.get("time", ""))
    url = normalize_url(row.get("url", ""))
    digest = str(row.get("content_hash", ""))
    score = pd.to_numeric(row.get("c1", ""), errors="coerce")
    score_value = "" if pd.isna(score) else int(score)
    origin = str(row.get("score_origin", ""))
    model = origin.removeprefix("llm::") if origin.startswith("llm::") else origin
    return {
        "record_id": record_id(code, published_at, url, digest),
        "country_code": code,
        "published_at": published_at,
        "url": url,
        "title": row.get("title", ""),
        "speaker": row.get("speaker", "") or row.get("name", ""),
        "content_chars": row.get("content_chars", ""),
        "score": score_value,
        "score_reasoning": row.get("c1_reason", ""),
        "war_related": bool(score_value != 0) if score_value != "" else False,
        "confidence": "",
        "source_kind": row.get("source_kind", ""),
        "language": row.get("language", ""),
        "model": model,
        "pipeline_version": "three-category-dsi-sync-v1",
        "response_id": row.get("response_id", ""),
        "scored_at": scored_at,
        "content_hash": digest,
        "is_legacy": False,
    }


def sync_country(code: str, scored_at: str) -> tuple[int, int]:
    destination = RECORD_ROOT / f"{code}.csv"
    existing = read_csv(destination) if destination.exists() else pd.DataFrame(columns=RECORD_COLUMNS)
    output_columns = list(existing.columns)
    output_columns.extend(column for column in RECORD_COLUMNS if column not in output_columns)
    delta = changed_accepted_scores(code)
    additions = pd.DataFrame([build_record(code, row, scored_at) for _, row in delta.iterrows()])
    if additions.empty:
        return 0, len(existing)

    replacement_urls = {normalize_url(value) for value in additions["url"] if normalize_url(value)}
    if replacement_urls and "url" in existing.columns:
        existing = existing.loc[~existing["url"].map(normalize_url).isin(replacement_urls)].copy()
    updated = pd.concat([existing, additions], ignore_index=True)
    for column in RECORD_COLUMNS:
        if column not in updated.columns:
            updated[column] = ""
    updated = updated[output_columns].drop_duplicates("record_id", keep="last")
    updated = updated.sort_values(["published_at", "record_id"]).reset_index(drop=True)
    updated.to_csv(destination, index=False, encoding="utf-8")
    return len(additions), len(updated)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync refreshed c1 scores to the website record store.")
    parser.add_argument("--countries", required=True, help="Comma-separated country codes.")
    args = parser.parse_args()
    scored_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    for code in [value.strip().upper() for value in args.countries.split(",") if value.strip()]:
        additions, total = sync_country(code, scored_at)
        print(f"{code}: synced={additions} total_records={total}")


if __name__ == "__main__":
    main()
