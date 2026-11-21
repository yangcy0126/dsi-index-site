import csv
import math
import sys
import unittest
from pathlib import Path

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import build_index_sources as sources


class IndexDownloadTests(unittest.TestCase):
    def assert_csv_sheet(self, sheet, path, numeric, boolean=()):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            rows = csv.reader(stream)
            columns = next(rows)
            cells = iter(sheet.values)
            self.assertEqual(tuple(columns), next(cells))
            for i, (row, values) in enumerate(zip(rows, cells, strict=True), 2):
                for key, raw, value in zip(columns, row, values, strict=True):
                    field = f"{sheet.title} row {i}, {key}"
                    if not raw:
                        self.assertIsNone(value, field)
                    elif key in numeric:
                        self.assertIsInstance(value, (int, float), field)
                        self.assertTrue(math.isclose(value, float(raw), rel_tol=1e-14, abs_tol=1e-14), field)
                    elif key in boolean:
                        self.assertIs(value, {"True": True, "False": False}[raw], field)
                    else:
                        self.assertEqual(raw, value, field)

    def test_public_periods(self):
        book = load_workbook(sources.DATA / "dsi_all_countries.xlsx", read_only=True)
        try:
            self.assertEqual(book.sheetnames, ["daily_panel", "variable_definitions", "site_periods"])
            self.assert_csv_sheet(book["site_periods"], sources.DATA / "wdsi_site_periods.csv",
                                  {"wdsi14_document", "signal_days", "required_days"}, {"full_signal_period"})
        finally:
            book.close()

    def test_source_workbook(self):
        files = sources.sources(sources.DATA)
        path = sources.DATA / "index_sources.xlsx"
        self.assertTrue(sources.current(path, sources.fingerprint(files)))
        book = load_workbook(path, read_only=True)
        try:
            self.assertEqual(book.sheetnames, list(files))
            for name, source in files.items():
                self.assert_csv_sheet(book[name], source, sources.NUMERIC)
        finally:
            book.close()
