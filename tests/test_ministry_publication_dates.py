import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from wdsi_pipeline import FranceMfaSpokespersonSource, GermanyForeignOfficeSource


class PublicationDates(unittest.TestCase):
    def test_german_article_overrides_stale_index_date_and_excludes_share_links(self):
        html = """<main><span class="heading__meta">19.11.2026 - Press release</span>
        <p class="rte__paragraph">The official statement.</p>
        <p class="quote__paragraph">The quoted paragraph.</p><li>Share page</li></main>"""
        rec = GermanyForeignOfficeSource(None)._make_record_from_html(
            html, "https://www.auswaertiges-amt.de/en/newsroom/news/2811580-2811580",
            "Joint statement", "2026-11-04", "de_aa_press_release",
        )
        self.assertEqual(rec.published_at, "2026-11-19")
        self.assertEqual(rec.content.splitlines(), ["The official statement.", "The quoted paragraph."])

    def test_french_publication_precedes_dates_in_title_and_body(self):
        html = """<article><h1>Meeting of 26 octobre 2026</h1>
        <span class="diplomatie--hdp-date--validated">Le : 27 octobre 2026</span>
        <div class="fr-mt-6w"><div class="fr-col-md-7"><p>Meeting on 25 octobre 2026.</p>
        </div></div></article>"""
        rec = FranceMfaSpokespersonSource(None)._make_record_from_html(html, "https://www.diplomatie.gouv.fr/fr/meeting")
        self.assertEqual(rec.published_at, "2026-10-27")
        self.assertEqual(rec.content, "Meeting on 25 octobre 2026.")

    def test_french_news_keeps_lead_and_article_once(self):
        html = """<article class="node--type-actualite"><h1>Announcement</h1>
        <span class="diplomatie--hdp-date--created">Publié le : 13 novembre 2026</span>
        <p class="fr-text--lead">The lead.</p><div class="fr-prose"><p>The full text.</p></div></article>"""
        rec = FranceMfaSpokespersonSource(None)._make_record_from_html(html, "https://www.diplomatie.gouv.fr/fr/announcement")
        self.assertEqual(rec.published_at, "2026-11-13")
        self.assertEqual(rec.content.splitlines(), ["The lead.", "The full text."])


if __name__ == "__main__":
    unittest.main()
