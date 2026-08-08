from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_wdsi_data import (  # noqa: E402
    build_visitor_display_countries,
    normalize_visitor_countries,
)


class VisitorRegionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = [
            {"code": "CN", "country": "China", "visitors": 55},
            {"code": "TW", "country": "Taiwan", "visitors": 35},
            {"code": "HK", "country": "Hong Kong", "visitors": 7},
            {"code": "GB", "country": "United Kingdom", "visitors": 34},
            {"code": "SG", "country": "Singapore", "visitors": 33},
        ]

    def test_regions_remain_separate(self) -> None:
        countries = normalize_visitor_countries(self.raw)
        by_code = {country["code"]: country for country in countries}

        self.assertEqual(by_code["CN"]["country"], "Mainland China")
        self.assertEqual(by_code["CN"]["visitors"], 55)
        self.assertEqual(by_code["TW"]["visitors"], 35)
        self.assertEqual(by_code["HK"]["visitors"], 7)
        self.assertNotIn("MO", by_code)

    def test_display_is_ranked_positive_and_limited(self) -> None:
        raw = self.raw + [
            {"code": "MO", "country": "Macau", "visitors": 0},
        ] + [
            {"code": f"X{index:02}", "country": f"Country {index:02}", "visitors": index}
            for index in range(1, 14)
        ]
        countries = normalize_visitor_countries(raw)
        display = build_visitor_display_countries(countries)

        self.assertEqual(len(display), 12)
        self.assertNotIn("MO", [row["code"] for row in display])
        self.assertTrue(all(int(row["visitors"]) > 0 for row in display))
        self.assertEqual(
            [row["visitors"] for row in display],
            sorted((row["visitors"] for row in display), reverse=True),
        )
        self.assertTrue(next(row for row in display if row["code"] == "CN")["is_focus_region"])
        self.assertFalse(next(row for row in display if row["code"] == "GB")["is_focus_region"])


if __name__ == "__main__":
    unittest.main()
