from __future__ import annotations

import pandas as pd

from build_wdsi_data import build_filled_daily_series, collapse_to_daily_minimum


def main() -> None:
    sample = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2026-01-01",
                    "2026-01-01",
                    "2026-01-01",
                    "2026-01-03",
                    "2026-01-07",
                    "2026-01-10",
                ]
            ),
            "raw": [2, -3, 0, -1, 1, -2],
            "title": ["a", "b", "c", "d", "e", "f"],
            "url": ["u1", "u2", "u3", "u4", "u5", "u6"],
        }
    )

    daily = collapse_to_daily_minimum(sample)
    expected_daily_raw = [-3, -1, 1, -2]
    if daily["raw"].tolist() != expected_daily_raw:
        raise AssertionError(f"Expected daily minima {expected_daily_raw}, got {daily['raw'].tolist()}")

    series = build_filled_daily_series(daily, pd.Timestamp("2026-01-31"))
    first_seven = series.loc[:6, "rolling7"].tolist()
    if first_seven[:6] != [None] * 6 and any(pd.notna(value) for value in first_seven[:6]):
        raise AssertionError("7-day rolling values must stay empty before the 7th calendar day.")

    day_seven_value = series.loc[6, "rolling7"]
    if round(float(day_seven_value), 6) != round((-3 - 3 - 1 - 1 - 1 - 1 + 1) / 7, 6):
        raise AssertionError("7-day rolling value does not match the original forward-filled path.")

    if pd.notna(series.loc[28, "rolling30"]):
        raise AssertionError("30-day rolling values must stay empty before the 30th calendar day.")

    print("Method lock check passed.")


if __name__ == "__main__":
    main()
