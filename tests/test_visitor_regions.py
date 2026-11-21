from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_wdsi_data import (  # noqa: E402
    build_visitor_display_countries,
    build_visitor_snapshot,
    normalize_visitor_countries,
    parse_visitor_countries,
    parse_visitor_history,
    parse_visitor_overview,
)
import update_visitor_snapshot  # noqa: E402


OVERVIEW = """2 different countries have visited this site. 4 flags collected.
Visitors Yesterday: 5 30 day average: 2 Record: 59 on August 9, 2026
Flag Counter Views Yesterday: 8 30 day average: 3 Record: 1,108 on March 22, 2026"""
HISTORY = "This counter has been viewed <b>1,234,567</b> times by 1,245 visitors!"
COUNTRIES = """<table>
<tr><td>1.</td><td></td><td></td><td><a href="https://en.wikipedia.org/wiki/China">China</a></td>
<td><font>1,200</font></td><td><a href="/detail30/cn/DVgZ">Chart</a></td><td></td><td>Today</td></tr>
<tr><td>1.1.</td><td>Beijing</td><td>500</td><td><a href="/detail30/cn_bj/DVgZ">Chart</a></td></tr>
<tr><td>2.</td><td></td><td></td><td>Hong Kong</td><td>45</td>
<td><a href="/detail30/hk/DVgZ">Chart</a></td><td></td><td>Yesterday</td></tr></table>"""


class VisitorSnapshotTests(unittest.TestCase):
    def test_grouped_counters_and_dates(self) -> None:
        self.assertEqual(parse_visitor_history(HISTORY), {"total_views": 1234567, "total_visitors": 1245})
        overview = parse_visitor_overview(OVERVIEW)
        self.assertEqual(overview["views_record"], 1108)
        self.assertEqual(overview["views_record_date"], "March 22, 2026")

    def test_current_country_rows_exclude_subdivisions(self) -> None:
        rows = parse_visitor_countries(COUNTRIES)
        self.assertEqual([(r["code"], r["country"], r["visitors"]) for r in rows],
                         [("CN", "China", 1200), ("HK", "Hong Kong", 45)])

    def test_incomplete_list_does_not_publish(self) -> None:
        with patch("build_wdsi_data.fetch_url_text", side_effect=[OVERVIEW, COUNTRIES.replace("/detail30/hk", "/other/hk"), HISTORY]):
            with self.assertRaisesRegex(ValueError, "country rows"):
                build_visitor_snapshot()

    def test_failed_refresh_keeps_previous_snapshot_and_signals_failure(self) -> None:
        with TemporaryDirectory() as tmp, patch.object(update_visitor_snapshot, "OUTPUT_DIR", Path(tmp)):
            path = Path(tmp) / "visitor_stats.json"
            previous = b'{"available": true, "total_views": 999}'
            path.write_bytes(previous)
            with patch.object(update_visitor_snapshot, "build_visitor_snapshot", side_effect=ValueError("bad response")):
                with self.assertRaisesRegex(SystemExit, "bad response"):
                    update_visitor_snapshot.main()
            self.assertEqual(path.read_bytes(), previous)

    def test_check_writes_only_real_changes(self) -> None:
        before = {"available": True, "total_views": 10, "generated_at": "first"}
        same = {**before, "generated_at": "second"}
        changed = {**same, "total_views": 11}
        with TemporaryDirectory() as tmp, patch.object(update_visitor_snapshot, "OUTPUT_DIR", Path(tmp)):
            path = Path(tmp) / "visitor_stats.json"
            original = json.dumps(before).encode()
            path.write_bytes(original)
            with patch.object(update_visitor_snapshot, "build_visitor_snapshot", side_effect=[same, changed]):
                update_visitor_snapshot.main()
                self.assertEqual(path.read_bytes(), original)
                update_visitor_snapshot.main()
                self.assertEqual(json.loads(path.read_bytes()), changed)

    def test_stale_public_copy_is_redeployed_without_another_commit(self) -> None:
        expected = {"available": True, "total_views": 11, "generated_at": "current"}
        for total, stale in [(11, False), (10, True)]:
            live = json.dumps({**expected, "total_views": total, "generated_at": "other"}).encode()
            with self.subTest(total=total), patch.object(update_visitor_snapshot, "urlopen", return_value=io.BytesIO(live)):
                self.assertEqual(update_visitor_snapshot.published_snapshot_is_stale(expected), stale)
        for status in (404, 503):
            error = HTTPError("https://dsi-index.org/", status, "test", None, None)
            with self.subTest(status=status), patch.object(update_visitor_snapshot, "urlopen", side_effect=error):
                if status == 404:
                    self.assertTrue(update_visitor_snapshot.published_snapshot_is_stale(expected))
                else:
                    with self.assertRaises(HTTPError):
                        update_visitor_snapshot.published_snapshot_is_stale(expected)


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
