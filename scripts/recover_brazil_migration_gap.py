from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import json
import re
import time
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup
from googlenewsdecoder import gnewsdecoder

from update_three_category_sources import (
    COUNTRY_NAMES,
    dedupe,
    load_local,
    save_local,
    standardize,
)
from wdsi_pipeline import BrazilItamaratyPressReleaseSource, clean_text, normalize_generic_url


GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
OFFICIAL_PREFIX = "https://www.gov.br/mre/pt-br/canais_atendimento/imprensa/notas-a-imprensa/"
COMMON_CRAWL_INDEXES = (
    "CC-MAIN-2026-17",
    "CC-MAIN-2026-21",
    "CC-MAIN-2026-25",
    "CC-MAIN-2026-30",
)


def canonical_official_url(value: str) -> str:
    url = normalize_generic_url(value)
    return url.replace("/notas-a-imprensa-backup/", "/notas-a-imprensa/")


def fetch_rss(query: str) -> list[dict[str, str]]:
    params = {"q": query, "hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-419"}
    response = requests.get(GOOGLE_NEWS_RSS, params=params, timeout=45)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "xml")
    return [
        {
            "title": clean_text(item.title.get_text()),
            "link": clean_text(item.link.get_text()),
            "pub_date": clean_text(item.pubDate.get_text()),
        }
        for item in soup.select("item")
    ]


def discover_google_news(start_date: str, end_date: str, note_min: int, note_max: int) -> list[dict[str, str]]:
    note_phrase = "NOTA À IMPRENSA Nº"
    base = f"site:gov.br/mre/pt-br/canais_atendimento/imprensa/notas-a-imprensa"
    queries = [f'{base} "{note_phrase} {number}"' for number in range(note_min, note_max + 1)]
    queries.extend(
        f"{base} {term} after:{start_date} before:{pd.Timestamp(end_date) + pd.Timedelta(days=1):%Y-%m-%d}"
        for term in ("Brasil", "MRE", "visita", "mercado", "declaração", "comunicado", "eleição")
    )

    items: dict[str, dict[str, str]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for result in executor.map(fetch_rss, queries):
            for item in result:
                try:
                    published_at = parsedate_to_datetime(item["pub_date"]).date().isoformat()
                except (TypeError, ValueError):
                    continue
                if start_date <= published_at <= end_date:
                    item["published_at"] = published_at
                    items[item["link"]] = item

    def decode(item: dict[str, str]) -> dict[str, str] | None:
        for attempt in range(3):
            try:
                decoded = gnewsdecoder(item["link"])
                url = decoded.get("decoded_url", "") if isinstance(decoded, dict) else ""
                if OFFICIAL_PREFIX in url or "/notas-a-imprensa-backup/" in url:
                    return {**item, "url": canonical_official_url(url)}
            except Exception:
                time.sleep(attempt + 1)
        return None

    decoded_items: list[dict[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for item in executor.map(decode, items.values()):
            if item is not None:
                decoded_items.append(item)
    return decoded_items


def common_crawl_captures() -> list[dict[str, object]]:
    captures: dict[str, dict[str, object]] = {}
    for index_name in COMMON_CRAWL_INDEXES:
        endpoint = f"https://index.commoncrawl.org/{index_name}-index"
        params = {"url": f"{OFFICIAL_PREFIX}*", "output": "json", "filter": "status:200"}
        response: requests.Response | None = None
        for attempt in range(3):
            try:
                response = requests.get(endpoint, params=params, timeout=120)
                if response.status_code == 200:
                    break
            except requests.RequestException:
                response = None
            time.sleep(2 * (attempt + 1))
        if response is None or response.status_code != 200:
            continue
        for line in response.text.splitlines():
            try:
                capture = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = canonical_official_url(str(capture.get("url", "")))
            if url == OFFICIAL_PREFIX.rstrip("/") or not url.startswith(OFFICIAL_PREFIX):
                continue
            capture["url"] = url
            previous = captures.get(url)
            if previous is None or str(capture.get("timestamp", "")) > str(previous.get("timestamp", "")):
                captures[url] = capture
    return list(captures.values())


def fetch_common_crawl_html(capture: dict[str, object]) -> str:
    offset = int(capture["offset"])
    length = int(capture["length"])
    response = requests.get(
        f"https://data.commoncrawl.org/{capture['filename']}",
        headers={"Range": f"bytes={offset}-{offset + length - 1}"},
        timeout=90,
    )
    response.raise_for_status()
    payload = gzip.decompress(response.content)
    first = payload.find(b"\r\n\r\n")
    second = payload.find(b"\r\n\r\n", first + 4)
    if second < 0:
        raise ValueError("Common Crawl record did not contain an HTTP payload")
    return payload[second + 4 :].decode("utf-8", errors="replace")


def parse_official_html(url: str, html_text: str) -> dict[str, str] | None:
    soup = BeautifulSoup(html_text, "html.parser")
    title_node = soup.select_one(".documentFirstHeading, h1")
    byline = soup.select_one(".documentByLine")
    content_root = soup.select_one("#content-core")
    if title_node is None or byline is None or content_root is None:
        return None
    date_match = re.search(
        r"(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{4})",
        clean_text(byline.get_text(" ", strip=True)),
    )
    if date_match is None:
        return None
    content = clean_text(content_root.get_text("\n", strip=True))
    if not content:
        return None
    return {
        "url": canonical_official_url(url),
        "published_at": f"{date_match.group('year')}-{date_match.group('month')}-{date_match.group('day')}",
        "title": clean_text(title_node.get_text(" ", strip=True)),
        "content": content,
    }


def discover_common_crawl(start_date: str, end_date: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    def recover(capture: dict[str, object]) -> dict[str, str] | None:
        try:
            return parse_official_html(str(capture["url"]), fetch_common_crawl_html(capture))
        except Exception:
            return None

    captures = common_crawl_captures()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for record in executor.map(recover, captures):
            if record is not None and start_date <= record["published_at"] <= end_date:
                records.append(record)
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Recover Brazil MRE releases hidden during the 2026 election migration.")
    parser.add_argument("--start-date", default="2026-04-20")
    parser.add_argument("--end-date", default="2026-07-02")
    parser.add_argument("--note-min", type=int, default=140)
    parser.add_argument("--note-max", type=int, default=229)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    google_items = discover_google_news(args.start_date, args.end_date, args.note_min, args.note_max)
    crawl_items = discover_common_crawl(args.start_date, args.end_date)
    recovered = {item["url"]: item for item in google_items}
    for item in crawl_items:
        previous = recovered.get(item["url"])
        if previous is None or len(item.get("content", "")) > len(previous.get("content", "")):
            recovered[item["url"]] = item

    rows = []
    for item in recovered.values():
        title = clean_text(item["title"].rsplit(" - www.gov.br", 1)[0])
        content = clean_text(item.get("content", "")) or title
        rows.append(
            {
                "published_at": item["published_at"],
                "title": title,
                "name": "Ministério das Relações Exteriores",
                "speaker": "Ministério das Relações Exteriores",
                "url": item["url"],
                "content": content,
                "source_kind": BrazilItamaratyPressReleaseSource._source_kind(title, content),
                "language": "pt",
                "country_code": "BR",
                "country": COUNTRY_NAMES["BR"],
                "origin": "official_archive_migration_recovery_20260806",
            }
        )

    fetched = standardize(pd.DataFrame(rows))
    existing = load_local("BR")
    merged = dedupe(pd.concat([existing, fetched], ignore_index=True))
    print(
        f"BR migration recovery: google={len(google_items)} common_crawl={len(crawl_items)} "
        f"recovered={len(fetched)} rows={len(existing)}->{len(merged)} "
        f"title_only={(fetched['content_chars'].astype(int) <= fetched['title'].str.len() + 2).sum()}"
    )
    if not args.dry_run:
        save_local("BR", merged)


if __name__ == "__main__":
    main()
