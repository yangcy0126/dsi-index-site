"""Refresh the public sufficient statistics from the reviewed research corpus."""
import argparse
import json

import pandas as pd

from build_dsi_site_data import (
    ALL_COUNTRIES_DAILY_PATH, COUNTRY_ORDER, SCORES_DIR, WORKSPACE_ROOT,
    build_latest_publication_lookup,
)
from wdsi_series import INPUTS


def write_snapshot(frame, name):
    frame.to_csv(INPUTS / name, index=False, lineterminator="\n", float_format="%.0f",
                 compression={"method": "gzip", "mtime": 0})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-paper", action="store_true",
                        help="Also import the November 19 paper's frozen international input.")
    args = parser.parse_args()
    g = pd.read_csv(ALL_COUNTRIES_DAILY_PATH, usecols=[
        "country_code", "time", "publication", "c1_raw", "c2_raw", "c3_raw"
    ]).rename(columns={"time": "date"}).sort_values(["country_code", "date"])
    assert set(g.country_code) == set(COUNTRY_ORDER) and not g.duplicated(["country_code","date"]).any()
    for col in ("c1_raw", "c2_raw", "c3_raw"):
        assert g[col].notna().equals(g.publication) and g[col].dropna().isin(range(-3, 4)).all()
    totals = []
    for code, days in g.groupby("country_code"):
        scores = pd.read_csv(SCORES_DIR / f"{code}_scores.csv", usecols=["time", "c1", "score_status"])
        scored = scores.loc[scores.score_status.eq("accepted")]
        if scored.empty or scored.time.isna().any() or not scored.c1.isin(range(-3, 4)).all():
            raise ValueError(f"{code}: invalid accepted WDSI records")
        if scored.time.max() > days.date.max():
            raise ValueError(f"{code}: reviewed country calendar ends before the last scored document")
        # Use the active score corpus once, on the existing site observation calendar.
        stats = scored.loc[scored.time.ge(days.date.min())].groupby("time").c1.agg(["count", "sum"])
        if not stats.index.isin(days.loc[days.publication, "date"]).all():
            raise ValueError(f"{code}: scored documents on dates absent from the reviewed publication calendar")
        totals.append(stats.rename(columns={"count": "doc_count"}).assign(doc_sum=-stats["sum"], country_code=code)
                      .rename_axis("date").reset_index()[["country_code", "date", "doc_count", "doc_sum"]])
    g = g.merge(pd.concat(totals), on=["country_code", "date"], how="left", validate="one_to_one")
    g[["doc_count", "doc_sum"]] = g[["doc_count", "doc_sum"]].fillna(0).astype(int)
    INPUTS.mkdir(exist_ok=True)
    write_snapshot(g, "reviewed_daily.csv.gz")
    (INPUTS / "latest_publications.json").write_text(
        json.dumps(build_latest_publication_lookup(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if args.include_paper:
        cn = pd.read_csv(SCORES_DIR / "CN_scores.csv", usecols=["time", "c1", "score_status"])
        cn = cn.loc[cn.score_status.eq("accepted") & cn.time.le("2026-04-17")]
        assert cn.time.notna().all() and cn.c1.isin(range(-3, 4)).all()
        stats = cn.groupby("time").c1.agg(["min", "sum", "count"])
        dates = g.loc[g.country_code.eq("CN") & g.date.le("2026-04-17"), ["date","c1_raw"]]
        assert set(stats.index) == set(dates.loc[dates.c1_raw.notna(), "date"])
        assert dates.date.map(stats["min"]).equals(dates.c1_raw)
        documents = dates[["date"]].assign(doc_count=dates.date.map(stats["count"]).fillna(0),
                                           doc_sum=-dates.date.map(stats["sum"]).fillna(0))
        write_snapshot(documents, "paper_document_days.csv.gz")
        audit = WORKSPACE_ROOT / "DSI-ICF/result/review_response_20261014/downstream_corrected_wdsi14_audit"
        shared = pd.read_csv(audit / "shared/daily_wdsi14.csv", usecols=["country_code", "date", "c1"])
        kr = pd.read_csv(audit / "us_jp_kr/kr_daily_same_source_wdsi14.csv", usecols=["date", "k1"])
        frozen = pd.concat([shared.loc[shared.country_code.ne("KR")],
                            kr.rename(columns={"k1": "c1"}).assign(country_code="KR")])
        assert set(frozen.country_code) == set(COUNTRY_ORDER) and frozen.c1.isin(range(-3, 4)).all()
        write_snapshot(frozen.sort_values(["country_code", "date"]).rename(columns={"c1": "legacy_filled"}),
                       "paper_countryday.csv.gz")
    print(f"Imported {len(g):,} reviewed country-days. Included paper inputs: {args.include_paper}.")


if __name__ == "__main__":
    main()
