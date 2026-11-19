"""Verify published artifacts independently of the production rolling/grouping code."""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from build_dsi_site_data import COUNTRY_ORDER, SITE_DATA_DIR
from wdsi_series import INPUTS, daily_indices, paper_documents


def same(a, b, tol=1e-12):
    np.testing.assert_allclose(np.asarray(a, dtype=float), np.asarray(b, dtype=float),
                               atol=tol, rtol=0, equal_nan=True)


def windows(x, n):
    # Prefix sums, not the pandas rolling implementation used by the builder.
    prefix = np.cumsum(np.r_[0., np.asarray(x, dtype=float)])
    return np.r_[np.full(n - 1, np.nan), prefix[n:] - prefix[:-n]]


def document_windows(count, total, n):
    k = windows(count, n)
    avg = windows(total, n) / np.where(k > 0, k, np.nan)
    for i in range(n, len(avg)):
        if k[i] == 0:
            avg[i] = avg[i - 1]
    return avg


def check_edges():
    dates = pd.date_range("2024-01-01", periods=37)
    f = pd.DataFrame({"date": dates, "country_code": "CN", "c1_raw": np.nan,
                      "c2_raw": 0., "c3_raw": 0., "doc_count": 0, "doc_sum": 0})
    f.loc[[0, 4, 33], ["c1_raw", "doc_count", "doc_sum"]] = [
        [-3, 2, 3], [2, 1, -2], [0, 1, 0]]
    g = daily_indices(f)
    docs = paper_documents(f)
    assert g.c1_14[:13].isna().all() and docs.wdsi14_document[:13].isna().all()
    same(g.c1_raw.iloc[[0, 1, 4, 33]], [1.5, np.nan, -2, 0])
    same(g.c1_14.iloc[[13, 14, 18, 32, 33]], [1/3, -2, -2, -2, 0])
    same(docs.wdsi14_document.iloc[[13, 14, 18, 32, 33]], [1/3, -2, -2, -2, 0])


def compare_table(a, b):
    pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True),
                                  check_dtype=False, rtol=0, atol=1e-12)


def check_periods(frame, expected_daily, field):
    dates = pd.to_datetime(expected_daily.date)
    for freq in ("M", "Q"):
        z = frame.loc[frame.frequency.eq(freq)].set_index(["country_code", "period"]).sort_index()
        groups = expected_daily.groupby([expected_daily.country_code, dates.dt.to_period(freq).astype(str)])
        assert list(z.index) == list(groups.size().index)
        values = groups.value
        same(z[field], values.sum(min_count=1).div(values.count().replace(0, np.nan)))
        same(z.signal_days, values.count())
        for row in z.itertuples():
            t = pd.Period(row.Index[1], freq=freq)
            assert row.required_days == (t.end_time.date() - t.start_time.date()).days + 1
            assert row.full_signal_period == (row.required_days == row.signal_days)
            assert row.date == t.start_time.date().isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workbooks", action="store_true")
    parser.add_argument("--research-root", type=Path, help="Optional DSI-ICF directory for source and Figure 2 comparisons")
    parser.add_argument("--paper-reference", type=Path, help="Optional frozen monthly_quarterly_signal14.csv (original sign)")
    args = parser.parse_args()
    check_edges()
    src = pd.read_csv(INPUTS / "reviewed_daily.csv.gz")
    master = pd.read_csv(SITE_DATA_DIR / "dsi_all_countries.csv")
    panel = pd.read_csv(SITE_DATA_DIR / "wdsi_all_countries.csv")
    summary = json.loads((SITE_DATA_DIR / "summary.json").read_text(encoding="utf-8"))
    assert [s["code"] for s in summary["countries"]] == COUNTRY_ORDER
    assert set(src.country_code) == set(COUNTRY_ORDER) and len(src) == len(master) == len(panel)
    branches = {k: pd.read_csv(SITE_DATA_DIR / f"{k}_all_countries.csv") for k in ("edsi","odsi")}
    schema = pd.read_csv(SITE_DATA_DIR / "US.csv", nrows=0).columns
    for s in summary["countries"]:
        code = s["code"]
        a = src.loc[src.country_code.eq(code)].reset_index(drop=True)
        f = pd.read_csv(SITE_DATA_DIR / f"{code}.csv")
        p = panel.loc[panel.country_code.eq(code)].reset_index(drop=True)
        j = pd.DataFrame(json.loads((SITE_DATA_DIR / f"{code}.json").read_text(encoding="utf-8"))["records"])
        assert f.columns.equals(schema) and j.columns.equals(schema)
        for col in ("doc_count", "doc_sum"):
            for out in (f, j, p):
                same(out[col], a[col])
        assert p.publication.equals(a.doc_count.gt(0))
        compare_table(master.loc[master.country_code.eq(code), f.columns], f)
        assert list(a.date) == list(f.date) == list(p.date) == list(j.date)
        assert pd.to_datetime(a.date).diff().dropna().eq(pd.Timedelta(days=1)).all()
        assert a.publication.equals(f.publication) and a.publication.equals(a.c1_raw.notna())
        raw = a.doc_sum.div(a.doc_count.where(a.doc_count.gt(0)))
        stance = raw.ffill()
        same(f.c1_raw, raw)
        same(j.c1_raw, raw)
        same(j.raw, raw)
        same(p.raw, f.c1_raw)
        same(f.raw, f.c1_raw)
        same(p.filled, stance)
        same(j.c1, stance)
        for n in (3, 7, 14, 30):
            expect = document_windows(a.doc_count, a.doc_sum, n)
            for x in (p[f"ma{n}"], f[f"c1_{n}"], j[f"c1_{n}"]):
                same(x, expect)
            if n != 3:
                same(f[f"rolling{n}"], expect)
                same(j[f"rolling{n}"], expect)
        same(s["latest_14d"], f.c1_14.iloc[-1])
        same(s["indicators"]["c1"]["latest_14d"], f.c1_14.iloc[-1])
        wi = s["indicators"]["c1"]
        assert wi["publication_days"] == raw.count() and wi["latest_publication_date"] == a.loc[raw.notna(), "date"].max()
        for key, val in (("latest_raw", raw.dropna().iloc[-1]), ("latest_7d", f.c1_7.iloc[-1]), ("latest_30d", f.c1_30.iloc[-1])):
            same(wi[key], val, 0.0005001)
        assert s["latest_date"] == f.date.iloc[-1] and s["publication_days"] == f.publication.sum()
        for key, slug in (("c2","edsi"), ("c3","odsi")):
            z = branches[slug].loc[branches[slug].country_code.eq(code)]
            assert list(z.date) == list(f.date)
            for field, other in ((key+"_raw","raw"), (key,"filled"), (key+"_3","ma3"),
                                 (key+"_7","ma7"), (key+"_30","ma30")):
                same(f[field], z[other])
                same(j[field], z[other], 0.00050001)
        if args.workbooks:
            compare_table(pd.read_excel(SITE_DATA_DIR / f"{code}.xlsx", sheet_name=code), f)
    live = pd.read_csv(SITE_DATA_DIR / "wdsi_site_periods.csv")
    paper = pd.read_csv(SITE_DATA_DIR / "wdsi_paper_periods.csv")
    check_periods(live, master.rename(columns={"c1_14": "value"}), "wdsi14_document")
    frozen = pd.read_csv(INPUTS / "paper_countryday.csv.gz")
    frozen["value"] = frozen.groupby("country_code").legacy_filled.transform(lambda s: -windows(s, 14) / 14)
    check_periods(paper, frozen, "wdsi14_countryday")
    source_docs = pd.read_csv(INPUTS / "paper_document_days.csv.gz")
    assert pd.to_datetime(source_docs.date).diff().dropna().eq(pd.Timedelta(days=1)).all()
    for n in (7, 14, 30):
        source_docs[f"wdsi{n}_document"] = document_windows(source_docs.doc_count, source_docs.doc_sum, n)
    archive = SITE_DATA_DIR / "wdsi_paper_replication.xlsx"
    doc_export = pd.read_excel(archive, sheet_name="stock_connect_daily")
    compare_table(doc_export, source_docs.loc[source_docs.date.ge("2010-01-01")])
    if args.research_root:
        root = args.research_root
        official = pd.read_csv(root / "data/三类指数结果_扩展15国/all_countries_daily.csv")
        official = official.rename(columns={"time": "date"}).set_index(["country_code", "date"]).sort_index()
        ordered = master.set_index(["country_code", "date"]).sort_index()
        assert official.index.equals(ordered.index)
        for k in ("c2", "c3"):
            same(official[k], ordered[k])
        for code, a in ordered.groupby(level="country_code"):
            docs = pd.read_csv(root / f"data/情绪测度结果数据_扩展15国/{code}_scores.csv", usecols=["time","score_status","c1"])
            grouped = docs.loc[docs.score_status.eq("accepted")].groupby("time").c1.agg(["count", "sum"])
            bydate = grouped.reindex(a.index.get_level_values("date"), fill_value=0)
            same(a.doc_count, bydate["count"])
            same(a.doc_sum, -bydate["sum"])
        docs = pd.read_csv(root / "data/情绪测度结果数据_扩展15国/CN_scores.csv", usecols=["time","c1","score_status"])
        counts = docs.loc[docs.score_status.eq("accepted")].groupby("time").c1.agg(["sum","count"])
        same(source_docs.doc_count, counts["count"].reindex(source_docs.date, fill_value=0))
        same(source_docs.doc_sum, -counts["sum"].reindex(source_docs.date, fill_value=0))
        ref = pd.read_csv(root / "result/updated_signal_aggregation_search_20261002/docroll_14_audit/docroll_14_signal_series.csv")
        assert list(doc_export.date) == list(ref.date)
        same(doc_export.wdsi14_document, -ref.docroll_14)
        same(ordered.loc["CN"].loc[ref.date, "c1_14"], -ref.docroll_14)
        print(f"Verified all 15 active score sources ({int(ordered.doc_count.sum()):,} in-calendar documents) and {len(ref):,} Figure 2 days.")
    if args.paper_reference:
        ref = pd.read_csv(args.paper_reference)
        ref.wdsi14_countryday *= -1
        compare_table(paper, ref)
        print(f"Verified all {len(ref):,} paper-vintage country-periods against the regression archive.")
    if args.workbooks:
        book = SITE_DATA_DIR / "wdsi_all_countries.xlsx"
        for sheet, table in (("wdsi_panel", panel), ("site_periods", live)):
            compare_table(pd.read_excel(book, sheet_name=sheet), table)
        compare_table(pd.read_excel(archive, sheet_name="international_periods"), paper)
        archived_daily = pd.read_excel(archive, sheet_name="international_daily")
        assert list(archived_daily.date) == list(frozen.date) and list(archived_daily.country_code) == list(frozen.country_code)
        same(archived_daily.c1_14, frozen.value)
        same(archived_daily.filled, -frozen.legacy_filled)
        compare_table(pd.read_excel(SITE_DATA_DIR / "dsi_all_countries.xlsx", sheet_name="daily_panel"), master)
        s = pd.read_excel(SITE_DATA_DIR / "summary.xlsx")[["code", "latest_date", "latest_14d"]]
        compare_table(s, pd.read_csv(SITE_DATA_DIR / "summary.csv")[s.columns])
    print(f"Passed: {len(summary['countries'])} countries, {len(master):,} calendar days, "
          f"{panel.ma14.count():,} valid comparative WDSI values, {len(live):,} site and {len(paper):,} frozen periods.")


if __name__ == "__main__":
    main()
