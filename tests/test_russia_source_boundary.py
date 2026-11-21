import os
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from wdsi_pipeline import RussiaMfaNewsSource


class CentralOriginals(unittest.TestCase):
    def setUp(self):
        self.src = RussiaMfaNewsSource(None)
        self.sec = next(s for s in self.src.sections if s["source_kind"] == "ru_mfa_press_release")

    def test_channel_switch_cannot_replace_new_vintage(self):
        with patch.dict(os.environ, {"WDSI_RU_OFFICIAL_TELEGRAM_ONLY": "1"}), patch.object(self.src, "_fetch_official_telegram_between") as channel:
            with self.assertRaisesRegex(ValueError, "central ministry English originals"):
                self.src.fetch_between("2026-08-07", "2026-11-20")
            channel.assert_not_called()
            self.src.fetch_between("2026-07-01", "2026-08-06")
            channel.assert_called_once()

    def test_archive_must_cross_boundary(self):
        def one(day):
            return [{"url": "https://mid.ru/en/foreign_policy/news/2164048/", "title": "A release", "published_at": day}]
        args = (object(), self.sec, "2026-08-07", "2026-11-20", date(2026, 8, 7))
        with patch.object(self.src, "_load_mid_list_page"), patch.object(self.src, "_extract_list_candidates", side_effect=[one("2026-11-20"), []]):
            with self.assertRaisesRegex(ValueError, "empty central listing"):
                self.src._collect_candidates(*args, 2)
        with patch.object(self.src, "_load_mid_list_page"), patch.object(self.src, "_extract_list_candidates", side_effect=[one("2026-08-07")]):
            with self.assertRaisesRegex(ValueError, "has not reached"):
                self.src._collect_candidates(*args, 1)
        with patch.object(self.src, "_load_mid_list_page"), patch.object(self.src, "_extract_list_candidates", side_effect=[one("2026-08-07"), one("2026-08-06")]):
            self.assertEqual(len(self.src._collect_candidates(*args, 2)), 1)

    def test_original_and_announcement_use_publication_not_event_date(self):
        q = {"title": "Briefing on November 20", "published_at": "2026-11-16"}
        regular = """<h1 class="photo-content__title">Briefing on November 20</h1>
        <span class="photo-content__date">16 November 2026</span><div class="photo-content__desc">
        <div class="article-content"><p>Official text.</p></div></div><aside>Unrelated link</aside>"""
        notice = """<h2 class="announcement__date">November 2026</h2><div>20</div>
        <div class="announcement__text"><span class="announcement__doc-num">1888-16-11-2026</span>
        <h3 class="announcement__title">Briefing on November 20</h3><p>Official text.</p></div>"""
        for html in (regular, notice):
            self.assertEqual(self.src._central_original(html, q), "Official text.")
            with self.assertRaisesRegex(ValueError, "conflicting publication date"):
                self.src._central_original(html, {**q, "published_at": "2026-11-20"})
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.src._central_original("<p>Human verification required</p>", q)

if __name__ == "__main__":
    unittest.main()
