"""Package the published index coverage and revision tables as one Excel download."""

import argparse
import csv
import hashlib
from pathlib import Path
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

DATA = Path(__file__).resolve().parents[1] / "data"
SHEETS = {
    "Coverage": "source_coverage.csv",
    "Exceptions": "source_review_exceptions.csv",
    "Document revisions": "multicountry_source_audit_*.csv",
    "India source audit": "india_source_audit_*.csv",
    "Russia catalogue": "russia_central_catalogue_*.csv",
    "Russia WDSI revisions": "russia_wdsi_revision_*.csv",
}
NUMERIC = {
    "accepted_new_documents", "unscored_documents", "added_calendar_days", "wdsi_score",
    "in_old_channel", "official_fulltext", "full_text_chars", "uses_original_40000_char_limit",
    "doc_count_previous_channel", "doc_sum_previous_channel", "c1_14_previous_channel",
    "doc_count_central_originals", "doc_sum_central_originals", "c1_14_central_originals",
    "wdsi14_change",
}


def sources(data):
    files = {}
    for sheet, pattern in SHEETS.items():
        matches = sorted(data.glob(pattern))
        if not matches:
            raise FileNotFoundError(data / pattern)
        files[sheet] = matches[-1]
    return files


def fingerprint(files):
    digest = hashlib.sha256()
    for path in (Path(__file__), *files.values()):
        digest.update(path.name.encode() + b"\0" + path.read_bytes().replace(b"\r\n", b"\n") + b"\0")
    return digest.hexdigest()


def current(book, expected):
    try:
        with ZipFile(book) as archive:
            metadata = ElementTree.fromstring(archive.read("docProps/core.xml"))
        return metadata.findtext("{http://purl.org/dc/elements/1.1/}identifier") == expected
    except (FileNotFoundError, BadZipFile, KeyError, ElementTree.ParseError):
        return False


def build(book, files, signature):
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    workbook = Workbook()
    workbook.remove(workbook.active)
    workbook.properties.title = "Diplomatic Sentiment Index: sources and revisions"
    workbook.properties.identifier = signature
    workbook.properties.description = "\n".join(f"{name}: {path.name}" for name, path in files.items())
    for name, path in files.items():
        sheet = workbook.create_sheet(name)
        with path.open(encoding="utf-8-sig", newline="") as stream:
            rows = csv.reader(stream)
            header = next(rows)
            sheet.append(header)
            widths = [len(field) for field in header]
            for i, row in enumerate(rows, 2):
                for j, (key, value) in enumerate(zip(header, row, strict=True), 1):
                    if not value:
                        continue
                    cell = sheet.cell(i, j)
                    if key in NUMERIC:
                        cell.value = float(value)
                    else:
                        cell.value = value
                        cell.data_type = "s"
                    widths[j - 1] = max(widths[j - 1], len(value))
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
        for j, width in enumerate(widths, 1):
            sheet.cell(1, j).font = Font(bold=True, color="173B5C")
            sheet.column_dimensions[get_column_letter(j)].width = min(65, max(13, width + 3))
    workbook.save(book)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check freshness without Excel dependencies")
    args = parser.parse_args()
    files = sources(DATA)
    signature = fingerprint(files)
    book = DATA / "index_sources.xlsx"
    if not current(book, signature):
        if args.check:
            raise SystemExit("Index sources workbook needs rebuilding.")
        build(book, files, signature)
    print("Index sources workbook is up to date.")
