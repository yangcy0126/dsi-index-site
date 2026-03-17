from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from wdsi_pipeline import clean_text, sha1_text


ROOT = Path(__file__).resolve().parents[1]
RECORDS_DIR = ROOT / "records"
LEGACY_SCORES_DIR = ROOT.parent / "data" / "情绪测度结果数据"
LEGACY_RAW_DIR = ROOT.parent / "data" / "外交部文本数据"

COUNTRIES = [
    {
        "code": "CN",
        "language": "en",
        "source_kind": "legacy_cn_press_conference",
        "score_file": "中国外交部例行记者会情绪测度结果.xlsx",
        "raw_file": "中国外交部例行记者会.xlsx",
    },
    {
        "code": "US",
        "language": "en",
        "source_kind": "legacy_us_spokesperson",
        "score_file": "美国国务院新闻办公室发言稿情绪测度结果.xlsx",
        "raw_file": "美国国务院新闻办公室发言稿.xlsx",
    },
    {
        "code": "UK",
        "language": "en",
        "source_kind": "legacy_uk_news_release",
        "score_file": "英国外交办公室新闻稿情绪测度结果.xlsx",
        "raw_file": "英国外交办公室新闻稿.xlsx",
    },
    {
        "code": "JP",
        "language": "en",
        "source_kind": "legacy_jp_mofa",
        "score_file": "日本外交部数据情绪测度结果.xlsx",
        "raw_file": "日本外交部数据.xlsx",
    },
    {
        "code": "KR",
        "language": "ko",
        "source_kind": "legacy_kr_mofa",
        "score_file": "韩国外交部新闻稿情绪测度结果.xlsx",
        "raw_file": "韩国外交部新闻稿.xlsx",
    },
]


def normalize_text_series(frame: pd.DataFrame, preferred: list[str]) -> pd.Series:
    for candidate in preferred:
        if candidate in frame.columns:
            return frame[candidate].fillna("").astype(str).map(clean_text)
    return pd.Series([""] * len(frame), index=frame.index, dtype="object")


def normalize_date_series(frame: pd.DataFrame) -> pd.Series:
    for candidate in ["time", "date", "发布时间"]:
        if candidate in frame.columns:
            return pd.to_datetime(frame[candidate], errors="coerce").dt.date.astype("string")
    raise ValueError("Could not find a supported date column.")


def normalize_score_frame(score_path: Path) -> pd.DataFrame:
    frame = pd.read_excel(score_path)
    score_col = next(column for column in frame.columns if str(column) == "2")
    content_col = next(column for column in frame.columns if str(column).lower() == "content" or str(column) == "内容")

    normalized = pd.DataFrame(
        {
            "published_at": normalize_date_series(frame),
            "content": frame[content_col].fillna("").astype(str).map(clean_text),
            "score": pd.to_numeric(frame[score_col], errors="coerce"),
            "score_reasoning": normalize_text_series(frame, [3, "3", "reasoning"]),
            "url": normalize_text_series(frame, ["url"]),
            "title": normalize_text_series(frame, ["title", "标题"]),
            "speaker": normalize_text_series(frame, ["name"]),
        }
    )
    normalized = normalized.dropna(subset=["published_at", "score"])
    normalized["published_at"] = normalized["published_at"].astype(str)
    return normalized


def enrich_with_raw(score_frame: pd.DataFrame, raw_path: Path) -> pd.DataFrame:
    if not raw_path.exists():
        return score_frame

    raw = pd.read_excel(raw_path)
    raw_frame = pd.DataFrame(
        {
            "published_at": normalize_date_series(raw).astype(str),
            "content": normalize_text_series(raw, ["content", "内容"]),
            "url_raw": normalize_text_series(raw, ["url"]),
            "title_raw": normalize_text_series(raw, ["title", "标题"]),
            "speaker_raw": normalize_text_series(raw, ["name"]),
        }
    )
    raw_frame = raw_frame.drop_duplicates(subset=["published_at", "content"])

    merged = score_frame.merge(
        raw_frame,
        on=["published_at", "content"],
        how="left",
    )

    for target, source in [("url", "url_raw"), ("title", "title_raw"), ("speaker", "speaker_raw")]:
        merged[target] = merged[target].where(merged[target].astype(bool), merged[source].fillna(""))

    return merged.drop(columns=["url_raw", "title_raw", "speaker_raw"])


def build_country_records(meta: dict[str, str]) -> pd.DataFrame:
    score_path = LEGACY_SCORES_DIR / meta["score_file"]
    raw_path = LEGACY_RAW_DIR / meta["raw_file"]

    normalized = normalize_score_frame(score_path)
    normalized = enrich_with_raw(normalized, raw_path)

    normalized["country_code"] = meta["code"]
    normalized["source_kind"] = meta["source_kind"]
    normalized["language"] = meta["language"]
    normalized["model"] = "legacy_import"
    normalized["pipeline_version"] = "legacy_import"
    normalized["response_id"] = ""
    normalized["confidence"] = ""
    normalized["war_related"] = normalized["score"].astype(float).ne(0)
    normalized["scored_at"] = ""
    normalized["is_legacy"] = True
    normalized["content_chars"] = normalized["content"].astype(str).str.len()
    normalized["content_hash"] = normalized["content"].map(sha1_text)
    normalized["record_id"] = normalized.apply(
        lambda row: sha1_text(
            f"{row['country_code']}|{row['published_at']}|{row['url'] or row['content_hash']}"
        )[:16],
        axis=1,
    )

    normalized = normalized[
        [
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
    ]

    return normalized.drop_duplicates(subset=["record_id"]).sort_values(["published_at", "record_id"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap repository-local scored records.")
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Skip countries that already have a records CSV.",
    )
    args = parser.parse_args()

    RECORDS_DIR.mkdir(parents=True, exist_ok=True)

    for meta in COUNTRIES:
        destination = RECORDS_DIR / f"{meta['code']}.csv"
        if args.only_missing and destination.exists():
            print(f"Skipping {meta['code']} because {destination.name} already exists.")
            continue

        records = build_country_records(meta)
        records.to_csv(destination, index=False, encoding="utf-8")
        print(f"Wrote {len(records)} records to {destination}")


if __name__ == "__main__":
    main()
