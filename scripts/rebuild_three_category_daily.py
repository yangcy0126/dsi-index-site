from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


SITE_ROOT = Path(__file__).resolve().parents[1]
DSI_ROOT = SITE_ROOT.parent / "DSI-ICF"
DATA_ROOT = DSI_ROOT / "data"
ARCHIVE_ROOT = DATA_ROOT / "ARCHIVE_OLD_DSI_INDEX_VERSIONS_20260806" / "pre_update_remaining12"
CN_US_KR_ARCHIVE_ROOT = (
    DATA_ROOT / "ARCHIVE_OLD_DSI_INDEX_VERSIONS_20260806" / "pre_update_cn_us_kr"
)


def find_live_file(filename: str) -> Path:
    candidates = [path for path in DATA_ROOT.rglob(filename) if "ARCHIVE_" not in str(path)]
    if len(candidates) != 1:
        raise RuntimeError(f"Expected one live {filename}, found {len(candidates)}: {candidates}")
    return candidates[0]


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)


def find_archived_file(code: str, kind: str) -> Path:
    candidates = [ARCHIVE_ROOT / f"{code}_{kind}.csv"]
    if code in {"CN", "US", "KR"}:
        candidates.insert(0, CN_US_KR_ARCHIVE_ROOT / kind / f"{code}_{kind}.csv")
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"Missing archived {kind} input for {code}: {candidates}")


def identity(row: pd.Series) -> str:
    url = str(row.get("url", "")).strip().rstrip("/")
    if url:
        return f"url::{url}"
    return f"key::{row.get('score_key', '')}"


def accepted_scores(frame: pd.DataFrame) -> pd.DataFrame:
    if "score_status" in frame.columns:
        frame = frame.loc[frame["score_status"].eq("accepted")].copy()
    for column in ("c1", "c2", "c3"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["published_at"] = pd.to_datetime(frame["published_at"], errors="coerce").dt.strftime("%Y-%m-%d")
    return frame.dropna(subset=["published_at", "c1", "c2", "c3"])


def changed_scores(current: pd.DataFrame, archived: pd.DataFrame) -> pd.DataFrame:
    old = archived.copy()
    old["_identity"] = [identity(row) for _, row in old.iterrows()]
    old_lookup = old.drop_duplicates("_identity", keep="last").set_index("_identity")

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


def rebuild_country(code: str) -> tuple[int, int, str]:
    score_path = find_live_file(f"{code}_scores.csv")
    daily_path = find_live_file(f"{code}_daily.csv")
    old_score_path = find_archived_file(code, "scores")
    old_daily_path = find_archived_file(code, "daily")

    current_scores = accepted_scores(read_csv(score_path))
    archived_scores = read_csv(old_score_path)
    delta = changed_scores(current_scores, archived_scores)
    delta_daily = (
        delta.groupby("published_at")[["c1", "c2", "c3"]]
        .min()
        .rename(columns={"c1": "c1_raw", "c2": "c2_raw", "c3": "c3_raw"})
    )

    old_daily = read_csv(old_daily_path)
    old_daily["time"] = pd.to_datetime(old_daily["time"], errors="raise")
    old_daily = old_daily.set_index("time").sort_index()
    for column in ("c1_raw", "c2_raw", "c3_raw"):
        old_daily[column] = pd.to_numeric(old_daily[column], errors="coerce")
    old_daily["publication"] = old_daily["publication"].astype(str).str.lower().eq("true")

    latest_score_date = pd.to_datetime(current_scores["published_at"], errors="coerce").max()
    end_date = max(old_daily.index.max(), latest_score_date)
    index = pd.date_range(old_daily.index.min(), end_date, freq="D")
    daily = pd.DataFrame(index=index)
    daily.index.name = "time"
    daily[["c1_raw", "c2_raw", "c3_raw"]] = old_daily[["c1_raw", "c2_raw", "c3_raw"]].reindex(index)
    daily["publication"] = old_daily["publication"].reindex(index, fill_value=False)

    for published_at, row in delta_daily.iterrows():
        day = pd.Timestamp(published_at)
        if day not in daily.index:
            continue
        for column in ("c1_raw", "c2_raw", "c3_raw"):
            value = row[column]
            existing = daily.at[day, column]
            daily.at[day, column] = value if pd.isna(existing) else min(existing, value)
        daily.at[day, "publication"] = True

    daily[["c1", "c2", "c3"]] = daily[["c1_raw", "c2_raw", "c3_raw"]].ffill().to_numpy()
    for window in (3, 7, 30):
        for category in ("c1", "c2", "c3"):
            daily[f"{category}_{window}"] = daily[category].rolling(window).mean()
    daily.insert(0, "country_code", code)
    daily = daily.reset_index()
    daily["time"] = daily["time"].dt.strftime("%Y-%m-%d")
    for column in ("c1_raw", "c2_raw", "c3_raw", "c1", "c2", "c3"):
        daily[column] = pd.to_numeric(daily[column], errors="coerce").round().astype("Int64")
    columns = [
        "time", "country_code", "c1_raw", "c2_raw", "c3_raw", "publication",
        "c1", "c2", "c3", "c1_3", "c2_3", "c3_3", "c1_7", "c2_7", "c3_7",
        "c1_30", "c2_30", "c3_30",
    ]
    daily = daily[columns]
    daily.to_csv(daily_path, index=False, encoding="utf-8-sig")
    daily.to_excel(daily_path.with_suffix(".xlsx"), index=False)
    return len(delta), len(daily), daily["time"].iloc[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Overlay refreshed scores on archived DSI daily baselines.")
    parser.add_argument("--countries", required=True, help="Comma-separated country codes.")
    args = parser.parse_args()
    for code in [value.strip().upper() for value in args.countries.split(",") if value.strip()]:
        changed, rows, end_date = rebuild_country(code)
        print(f"{code}: changed_scores={changed} daily_rows={rows} end_date={end_date}")


if __name__ == "__main__":
    main()
