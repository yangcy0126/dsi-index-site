"""November 2026 WDSI: public scores increase with adversity."""
from pathlib import Path

import pandas as pd


INPUTS = Path(__file__).resolve().parents[1] / "inputs"


def full_calendar(dates):
    days = pd.DatetimeIndex(dates)
    if not days.equals(pd.date_range(days.min(), days.max(), freq="D")):
        raise ValueError("Country input must have one row per consecutive calendar day")


def daily_indices(group):
    g = group.sort_values("date").copy()
    full_calendar(g.date)
    g["c1_raw"] = g.doc_sum.div(g.doc_count.where(g.doc_count.gt(0)))
    g["c1"] = g.c1_raw.ffill()
    for n in (3, 7, 14, 30):
        g[f"c1_{n}"] = document_mean(g, n)
    for key in ("c2", "c3"):
        g[key] = g[f"{key}_raw"].ffill()
        for n in (3, 7, 30):
            g[f"{key}_{n}"] = g[key].rolling(n, min_periods=n).mean()
    return g


def document_mean(g, n):
    count = g.doc_count.rolling(n, min_periods=n).sum()
    total = g.doc_sum.rolling(n, min_periods=n).sum()
    return total.div(count.where(count.gt(0))).ffill()


def paper_documents(frame):
    g = frame.sort_values("date").copy()
    full_calendar(g.date)
    for n in (7, 14, 30):
        g[f"wdsi{n}_document"] = document_mean(g, n)
    return g


def period_means(daily, field):
    frame = daily[["country_code", "date", "c1_14"]].copy()
    dates = pd.to_datetime(frame.date)
    parts = []
    for freq in ("M", "Q"):
        frame["period"] = dates.dt.to_period(freq)
        z = frame.groupby(["country_code", "period"]).c1_14.agg(["mean", "count"]).reset_index()
        z = z.rename(columns={"mean": field, "count": "signal_days"})
        start = z.period.dt.start_time
        z["required_days"] = (z.period.dt.end_time.dt.normalize() - start).dt.days + 1
        z["full_signal_period"] = z.signal_days.eq(z.required_days)
        z["date"] = start.dt.strftime("%Y-%m-%d")
        z["frequency"] = freq
        z["period"] = z.period.astype(str)
        parts.append(z)
    return pd.concat(parts, ignore_index=True)


def frozen_countrydays():
    frame = pd.read_csv(INPUTS / "paper_countryday.csv.gz")
    for _, g in frame.groupby("country_code"):
        full_calendar(g.date)
    frame["c1_14"] = frame.groupby("country_code").legacy_filled.transform(
        lambda s: -s.rolling(14, min_periods=14).mean()
    )
    frame["filled"] = -frame.pop("legacy_filled")
    return frame[["country_code", "date", "filled", "c1_14"]]
