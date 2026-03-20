from __future__ import annotations

import hashlib
import html
import json
import os
import re
import subprocess
import time
from email.utils import parsedate_to_datetime
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup


BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

STATE_HEADERS = {
    **BROWSER_HEADERS,
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.state.gov/",
}

CN_SEARCH_REFERER = (
    "https://www.mfa.gov.cn/irs-c-web/search_eng.shtml"
    "?code=18fe7c6489d&searchBy=title&searchWord=Regular%20Press%20Conference"
)

JSON_BLOCK_RE = re.compile(r"\{.*\}", re.S)
WHITESPACE_RE = re.compile(r"\s+")
QUESTION_LABEL_RE = re.compile(r"^(?P<label>[A-Z][A-Za-z0-9 .&/'()\\-]{0,80}|Q)\s*:\s*(?P<body>.*)$")
SPEAKER_TITLE_RE = re.compile(r"Foreign Ministry Spokesperson (.+?)'s Regular Press Conference", re.I)
ISO_LIKE_DATE_RE = re.compile(r"(\d{4}[./-]\d{2}[./-]\d{2})")
MONTH_DAY_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})$"
)
MONTH_NAME_PATTERN = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
MARKDOWN_LINK_RE = re.compile(r"\[(?P<label>[^\]]+)\]\((?P<url>https?://[^)\s]+)\)")
JP_LINE_DATE_RE = re.compile(rf"^{MONTH_NAME_PATTERN}\s+\d{{1,2}}(?:,\s+\d{{4}})?$")
JP_TITLE_DATE_RE = re.compile(rf"\((?P<date>{MONTH_NAME_PATTERN}\s+\d{{1,2}},\s+\d{{4}})(?:,[^)]+)?\)")
JP_CONFERENCE_HEADER_RE = re.compile(
    rf"^###\s+(?:[A-Za-z]+,\s+)?(?P<date>{MONTH_NAME_PATTERN}\s+\d{{1,2}},\s+\d{{4}})(?:,[^.]+)?"
)
FR_MONTH_NAME_PATTERN = (
    r"(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|"
    r"septembre|octobre|novembre|décembre|decembre)"
)
FR_SHORT_DATE_RE = re.compile(r"(?P<day>\d{2})[./-](?P<month>\d{2})[./-](?P<year>\d{2,4})")
FR_TEXTUAL_DATE_RE = re.compile(
    rf"(?P<day>\d{{1,2}})\s+(?P<month>{FR_MONTH_NAME_PATTERN})\s+(?P<year>\d{{4}})",
    re.I,
)
FR_TITLE_TEXTUAL_DATE_RE = re.compile(
    rf"\((?:[^()]*,\s*)?(?P<date>\d{{1,2}}\s+{FR_MONTH_NAME_PATTERN}\s+\d{{4}})\)",
    re.I,
)
FR_SLASH_DATE_RE = re.compile(r"(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{4})")
RU_LIST_TIMESTAMP_RE = re.compile(
    rf"(?P<date>\d{{1,2}}\s+{MONTH_NAME_PATTERN}\s+\d{{4}}\s+\d{{2}}:\d{{2}})"
)
RU_ARTICLE_ID_RE = re.compile(r"^\d{2,4}-\d{2}-\d{2}-\d{4}$")
DE_DOTTED_DATE_RE = re.compile(r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})")
INDIA_PAGE_UPDATED_RE = re.compile(r"Page last updated on:\s*(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})")
INDIA_SLASH_DATE_RE = re.compile(r"(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})")
ITALY_LISTING_DATE_RE = re.compile(rf"^(?P<date>\d{{1,2}}\s+{MONTH_NAME_PATTERN}\s+\d{{4}})$")
ITALY_HEADING_LINK_RE = re.compile(r"^#{5}\s+\[(?P<title>.+?)\]\((?P<url>https://www\.esteri\.it/[^\s)]+)")
ITALY_SITE_TITLE_SUFFIX_RE = re.compile(
    r"\s*(?:-|–|—|每)\s*Ministero degli Affari Esteri e della Cooperazione Internazionale.*$",
    re.I,
)
AU_LISTING_ENTRY_RE = re.compile(
    r"^\*\s+\[(?P<title>.+?)\]\((?P<url>https://www\.foreignminister\.gov\.au/[^\s)]+)\)\s+(?P<date>\d{1,2}\s+[A-Za-z]+\s+\d{4})$"
)
AU_INLINE_DATE_RE = re.compile(r"(?P<date>\d{1,2}\s+[A-Za-z]+\s+\d{4})$")
MX_LISTING_DATE_RE = re.compile(rf"^(?P<date>{MONTH_NAME_PATTERN}\s+\d{{1,2}},\s+\d{{4}})\s+Fecha de publicación")
MX_DETAIL_DATE_RE = re.compile(rf"\|\s*(?P<date>{MONTH_NAME_PATTERN}\s+\d{{1,2}},\s+\d{{4}})\s*\|")
ES_LISTING_ENTRY_RE = re.compile(
    r"^\*\s+(?P<date>\d{1,2}\s+[A-Za-z]{3}\s+\d{2})\s+##\s+\[(?P<title>.+?)\]\((?P<url>https://www\.exteriores\.gob\.es/en/Comunicacion/Comunicados/Paginas/[^\s)]+)\)"
)
BR_LISTING_TITLE_RE = re.compile(
    r'^##\s+\[(?P<title>.+?)\]\((?P<url>https://www\.gov\.br/mre/en/contact-us/press-area/press-releases/[^\s)]+)'
)
BR_LISTING_DATE_RE = re.compile(
    r"^published\s+(?P<date>[A-Z][a-z]{2}\s+\d{2},\s+\d{4})\s+\d{2}:\d{2}\s+[AP]M\s+News$"
)
JINA_CACHE: dict[str, str] = {}

FR_MONTHS = {
    "janvier": 1,
    "fevrier": 2,
    "février": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "août": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
    "décembre": 12,
}

RELEVANCE_VARIANTS = (
    "Apply a literal reading and do not infer military relevance unless the text itself supports it.",
    "Use a conservative topic screen and separate war-security content from trade, protocol, and culture.",
    "Focus on conflict, force, deterrence, sanctions tied to conflict, terrorism, ceasefire, and de-escalation.",
)
CATEGORY_VARIANTS = (
    "Classify sentiment strictly with respect to war and the use of force.",
    "Prefer neutral when the unit is descriptive and not evaluative.",
    "Pay attention to condemnation, threats, mediation, ceasefire, reconciliation, and peace language.",
)
SCORE_VARIANTS = (
    "Map intensity carefully across the full -3 to 3 range.",
    "Reserve -3 and +3 for exceptionally strong tone or breakthrough de-escalation.",
    "Differentiate mild concern from firm opposition and explicit threats or countermeasures.",
)


@dataclass(slots=True)
class ScrapedRecord:
    country_code: str
    published_at: str
    url: str
    title: str
    content: str
    source_kind: str
    language: str
    speaker: str = ""

    @property
    def content_hash(self) -> str:
        return sha1_text(self.content)

    @property
    def record_id(self) -> str:
        seed = self.url or f"{self.country_code}|{self.published_at}|{self.content_hash}"
        return sha1_text(seed)[:16]


@dataclass(slots=True)
class TextUnit:
    unit_id: str
    label: str
    text: str


def sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def clean_text(value: str) -> str:
    text = html.unescape(value or "")
    text = text.replace("\xa0", " ")
    text = text.replace("\u3000", " ")
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [WHITESPACE_RE.sub(" ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def normalize_compare_text(value: str) -> str:
    text = clean_text(value)
    text = text.replace("’", "'").replace("–", "-").replace("—", "-")
    return text.casefold()


def strip_html(value: str) -> str:
    soup = BeautifulSoup(value, "html.parser")
    return clean_text(soup.get_text("\n"))


def normalize_cn_article_url(value: str) -> str:
    url = clean_text(value)
    url = url.replace("http://www.mfa.gov.cn", "https://www.mfa.gov.cn")
    url = url.replace("http://www.fmprc.gov.cn", "https://www.fmprc.gov.cn")
    url = url.replace("https://www.fmprc.gov.cn/eng/", "https://www.mfa.gov.cn/eng/")
    return url


def normalize_generic_url(value: str) -> str:
    parts = urlsplit(clean_text(value))
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme, parts.netloc.lower(), path, parts.query, ""))


def parse_iso_like_date(value: str) -> str:
    text = clean_text(value)
    for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return parse_us_date(text)


def month_day_with_year_to_iso(value: str, year: int) -> str | None:
    text = clean_text(value)
    match = MONTH_DAY_RE.fullmatch(text)
    if not match:
        return None
    month_name, day_number = match.groups()
    return datetime.strptime(f"{month_name} {day_number} {year}", "%B %d %Y").date().isoformat()


def iter_months(start_date: str, end_date: str) -> list[tuple[int, int]]:
    start = iso_to_date(start_date).replace(day=1)
    end = iso_to_date(end_date).replace(day=1)
    months: list[tuple[int, int]] = []
    cursor = start
    while cursor <= end:
        months.append((cursor.year, cursor.month))
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)
    return months


def parse_us_date(value: str) -> str:
    text = clean_text(value)
    for fmt in (
        "%B %d, %Y %H:%M",
        "%B %d, %Y",
        "%b %d, %Y %H:%M",
        "%b %d, %Y",
        "%B %d %Y",
        "%d %B %Y %H:%M",
        "%d %B %Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Unsupported date format: {value}") from exc


def parse_en_short_date(value: str) -> str:
    text = clean_text(value)
    try:
        return datetime.strptime(text, "%d %b %y").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Unsupported short English date format: {value}") from exc


def parse_fr_date(value: str) -> str:
    text = clean_text(value)

    match = FR_TEXTUAL_DATE_RE.search(text)
    if match:
        month_name = clean_text(match.group("month")).lower()
        month_number = FR_MONTHS.get(month_name)
        if month_number is None:
            raise ValueError(f"Unsupported French month: {match.group('month')}")
        return date(int(match.group("year")), month_number, int(match.group("day"))).isoformat()

    for pattern in (FR_SLASH_DATE_RE, FR_SHORT_DATE_RE):
        match = pattern.search(text)
        if not match:
            continue
        year = int(match.group("year"))
        if year < 100:
            year += 2000
        return date(year, int(match.group("month")), int(match.group("day"))).isoformat()

    raise ValueError(f"Unsupported French date format: {value}")


def parse_de_date(value: str) -> str:
    text = clean_text(value)
    match = DE_DOTTED_DATE_RE.search(text)
    if not match:
        raise ValueError(f"Unsupported German date format: {value}")
    return date(int(match.group("year")), int(match.group("month")), int(match.group("day"))).isoformat()


def parse_india_page_updated(value: str) -> str:
    text = clean_text(value)
    match = INDIA_PAGE_UPDATED_RE.search(text)
    if match:
        return date(int(match.group("year")), int(match.group("month")), int(match.group("day"))).isoformat()

    month_match = re.search(rf"{MONTH_NAME_PATTERN}\s+\d{{1,2}},\s+\d{{4}}", text)
    if month_match:
        return parse_us_date(month_match.group(0))

    answered_match = re.search(r"ANSWERED ON[-:\s]*(?P<date>\d{1,2}/\d{1,2}/\d{4})", text, re.I)
    if answered_match:
        slash_match = INDIA_SLASH_DATE_RE.search(answered_match.group("date"))
        assert slash_match is not None
        return date(
            int(slash_match.group("year")),
            int(slash_match.group("month")),
            int(slash_match.group("day")),
        ).isoformat()

    raise ValueError(f"Could not determine India MEA publication date from: {value[:160]}")


def request_json(
    session: requests.Session,
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> object:
    merged_headers = {
        **BROWSER_HEADERS,
        "Accept": "application/json,text/plain,*/*",
    }
    if headers:
        merged_headers.update(headers)

    last_error: Exception | None = None
    for timeout_seconds in (30, 45, 60):
        try:
            response = session.get(url, headers=merged_headers, timeout=timeout_seconds)
            response.raise_for_status()
            content_type = (response.headers.get("content-type") or "").lower()
            if "json" not in content_type:
                preview = response.text[:200].replace("\n", " ")
                raise RuntimeError(f"Expected JSON from {url}, got {content_type or 'unknown'}: {preview}")
            return response.json()
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            time.sleep(1.5)
    if _supports_curl_fallback(url):
        try:
            return request_json_with_curl(url, headers=merged_headers)
        except Exception as curl_exc:  # pragma: no cover - external fallback
            last_error = curl_exc
    assert last_error is not None
    raise last_error


def request_json_post(
    session: requests.Session,
    url: str,
    payload: dict[str, object],
    *,
    headers: dict[str, str] | None = None,
) -> object:
    merged_headers = {
        **BROWSER_HEADERS,
        "Accept": "application/json,text/plain,*/*",
        "Content-Type": "application/json",
    }
    if headers:
        merged_headers.update(headers)
    last_error: Exception | None = None
    for timeout_seconds in (30, 45, 60):
        try:
            response = session.post(url, headers=merged_headers, json=payload, timeout=timeout_seconds)
            response.raise_for_status()
            content_type = (response.headers.get("content-type") or "").lower()
            if "json" not in content_type:
                preview = response.text[:200].replace("\n", " ")
                raise RuntimeError(f"Expected JSON from {url}, got {content_type or 'unknown'}: {preview}")
            return response.json()
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            time.sleep(1.5)
    assert last_error is not None
    raise last_error


def request_html(session: requests.Session, url: str) -> str:
    last_error: Exception | None = None
    for timeout_seconds in (30, 45, 60):
        try:
            response = session.get(url, headers=BROWSER_HEADERS, timeout=timeout_seconds)
            response.raise_for_status()
            if response.encoding == "ISO-8859-1" and response.apparent_encoding:
                response.encoding = response.apparent_encoding
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(1.5)
    if _supports_curl_fallback(url):
        try:
            return request_html_with_curl(url)
        except Exception as curl_exc:  # pragma: no cover - external fallback
            last_error = curl_exc
    if _supports_browser_fallback(url):
        try:
            return request_html_with_playwright(url)
        except Exception as browser_exc:  # pragma: no cover - external fallback
            last_error = browser_exc
    assert last_error is not None
    raise last_error


def request_html_with_curl(url: str) -> str:
    command = [
        "curl",
        "-fsSL",
        "--compressed",
        "--http1.1",
        "-A",
        BROWSER_HEADERS["User-Agent"],
        "-H",
        f"Accept: {BROWSER_HEADERS['Accept']}",
        "-H",
        f"Accept-Language: {BROWSER_HEADERS['Accept-Language']}",
        "-H",
        "Cache-Control: max-age=0",
        "-H",
        "Upgrade-Insecure-Requests: 1",
        url,
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        message = clean_text(result.stderr or result.stdout or f"curl failed for {url}")
        raise RuntimeError(message)
    return result.stdout


def request_json_with_curl(url: str, *, headers: dict[str, str] | None = None) -> object:
    merged_headers = {
        **BROWSER_HEADERS,
        "Accept": "application/json,text/plain,*/*",
    }
    if headers:
        merged_headers.update(headers)

    command = [
        "curl",
        "-fsSL",
        "--compressed",
        "--http1.1",
        "-A",
        merged_headers["User-Agent"],
    ]
    for name, value in merged_headers.items():
        if name == "User-Agent":
            continue
        command.extend(["-H", f"{name}: {value}"])
    command.append(url)

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        message = clean_text(result.stderr or result.stdout or f"curl failed for {url}")
        raise RuntimeError(message)
    return json.loads(result.stdout)


def _supports_curl_fallback(url: str) -> bool:
    return any(
        domain in url
        for domain in (
            "mofa.go.jp",
            "gov.uk",
            "diplomatie.gouv.fr",
            "auswaertiges-amt.de",
            "mea.gov.in",
        )
    )


def _supports_browser_fallback(url: str) -> bool:
    return any(domain in url for domain in ("mofa.go.jp", "gov.uk", "esteri.it"))


def request_html_with_playwright(url: str) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - optional dependency in CI fallback
        raise RuntimeError("Playwright fallback is not installed.") from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=BROWSER_HEADERS["User-Agent"],
            locale="en-US",
            extra_http_headers={
                "Accept": BROWSER_HEADERS["Accept"],
                "Accept-Language": BROWSER_HEADERS["Accept-Language"],
                "Cache-Control": "max-age=0",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        page = context.new_page()
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=90_000)
            page.wait_for_load_state("networkidle", timeout=15_000)
            if response is not None and response.status >= 400:
                raise RuntimeError(f"Playwright got HTTP {response.status} for {url}")
            content = page.content()
            if not clean_text(content):
                raise RuntimeError(f"Playwright returned empty content for {url}")
            return content
        finally:
            context.close()
            browser.close()


def request_markdown_via_jina(url: str) -> str:
    cached = JINA_CACHE.get(url)
    if cached is not None:
        return cached

    gateway_url = f"https://r.jina.ai/http://{url}"
    last_error: Exception | None = None
    for attempt, timeout_seconds in enumerate((30, 45, 60, 60, 60), start=1):
        try:
            response = requests.get(gateway_url, headers=BROWSER_HEADERS, timeout=timeout_seconds)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                sleep_seconds = float(retry_after) if retry_after else min(20, 4 * attempt)
                last_error = RuntimeError(f"Jina rate limited {url}")
                time.sleep(sleep_seconds)
                continue
            response.raise_for_status()
            if response.encoding == "ISO-8859-1" and response.apparent_encoding:
                response.encoding = response.apparent_encoding
            JINA_CACHE[url] = response.text
            time.sleep(0.25)
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(1.5)
    assert last_error is not None
    raise last_error


def extract_jina_markdown_body(markdown: str) -> str:
    lines = markdown.splitlines()
    for index, line in enumerate(lines):
        if clean_text(line) == "Markdown Content:":
            return "\n".join(lines[index + 1 :]).strip()
    return markdown.strip()


def markdown_links(line: str) -> list[tuple[str, str]]:
    return [(clean_text(match.group("label")), clean_text(match.group("url"))) for match in MARKDOWN_LINK_RE.finditer(line)]


def extract_json_object(text: str) -> dict[str, object]:
    match = JSON_BLOCK_RE.search(text.strip())
    if not match:
        raise ValueError(f"Model response did not contain JSON: {text[:200]}")
    return json.loads(match.group(0))


def iso_to_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def truncate_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + "\n[Truncated]"


def normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return clean_text(str(value)).lower() in {"1", "true", "yes", "y"}


def normalize_category(value: object) -> str:
    text = clean_text(str(value)).lower()
    if text not in {"negative", "neutral", "positive"}:
        raise ValueError(f"Unsupported category: {value}")
    return text


class ChinaMfaRegularPressSource:
    country_code = "CN"
    search_url = "https://www.mfa.gov.cn/irs/front/search"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 4) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 40, 30))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), page_size=10)

    def fetch_between(self, start_date: str, end_date: str, page_size: int = 10) -> list[ScrapedRecord]:
        begin = iso_to_date(start_date)
        finish = iso_to_date(end_date)
        payload = {
            "code": "18fe7c6489d",
            "configCode": "",
            "codes": "",
            "searchWord": "Regular Press Conference",
            "dataTypeId": "2076",
            "orderBy": "time",
            "searchBy": "title",
            "appendixType": "",
            "granularity": "CUSTOM",
            "beginDateTime": int(datetime.combine(begin, datetime.min.time()).timestamp() * 1000),
            "endDateTime": int((datetime.combine(finish + timedelta(days=1), datetime.min.time()).timestamp() * 1000) - 1),
            "isSearchForced": 0,
            "filters": [],
            "pageNo": 1,
            "pageSize": page_size,
            "isDefaultAdvanced": 0,
            "advancedFilters": None,
        }

        page_count = 1
        article_urls: list[str] = []
        while int(payload["pageNo"]) <= page_count:
            result = request_json_post(
                self.session,
                self.search_url,
                payload,
                headers={"Referer": CN_SEARCH_REFERER},
            )
            if not isinstance(result, dict) or not result.get("success"):
                raise RuntimeError(f"CN search failed for {start_date} to {end_date}: {result}")

            data = result.get("data") or {}
            pager = data.get("pager") if isinstance(data, dict) else {}
            page_count = int((pager or {}).get("pageCount") or 1)
            middle = data.get("middle") if isinstance(data, dict) else {}
            items = middle.get("listAndBox") if isinstance(middle, dict) else []
            if not isinstance(items, list):
                break

            for item in items:
                entry = item.get("data") if isinstance(item, dict) else {}
                url = normalize_cn_article_url(str(entry.get("url", "")).strip())
                if "/xw/fyrbt/lxjzh/" not in url:
                    continue
                article_urls.append(url)

            payload["pageNo"] = int(payload["pageNo"]) + 1

        unique_urls = list(dict.fromkeys(article_urls))
        return [self._parse_article(url) for url in unique_urls]

    def _parse_article(self, url: str) -> ScrapedRecord:
        url = normalize_cn_article_url(url)
        html_text = request_html(self.session, url)
        soup = BeautifulSoup(html_text, "html.parser")
        title = clean_text(self._select_text(soup, [".news_header_title", "meta[name='ArticleTitle']", "title"]))
        date_text = self._select_text(soup, [".xltime", "meta[name='PubDate']"])
        content_node = soup.select_one(".content_text") or soup.select_one(".news_content")
        if content_node is None:
            raise ValueError(f"Could not find article body for {url}")

        for node in content_node.select("img, script, style"):
            node.decompose()

        content = clean_text(content_node.get_text("\n"))
        if not title or not content:
            raise ValueError(f"Missing title or content for {url}")

        speaker = ""
        title_match = SPEAKER_TITLE_RE.search(title.replace("\u2019", "'"))
        if title_match:
            speaker = title_match.group(1).strip()

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=parse_us_date(date_text),
            url=url,
            title=title,
            content=content,
            source_kind="mfa_regular_press_conference",
            language="en",
            speaker=speaker,
        )

    @staticmethod
    def _select_text(soup: BeautifulSoup, selectors: Iterable[str]) -> str:
        for selector in selectors:
            if selector.startswith("meta["):
                node = soup.select_one(selector)
                if node and node.get("content"):
                    return str(node.get("content", "")).strip()
            else:
                node = soup.select_one(selector)
                if node:
                    return node.get_text(" ", strip=True)
        return ""


class UsStateDepartmentSource:
    country_code = "US"
    press_archive_url = "https://www.state.gov/press-releases/"
    briefing_endpoint = (
        "https://www.state.gov/wp-json/wp/v2/state_briefing"
        "?state_briefing_type=393&per_page=100&page={page}"
    )

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 2) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 50, 30))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 8) -> list[ScrapedRecord]:
        start = start_date[:10]
        end = end_date[:10]
        records: list[ScrapedRecord] = []
        records.extend(self._fetch_press_releases(start, end, max_pages=max_pages))
        records.extend(self._fetch_press_briefings(start, end, max_pages=max_pages))
        return list({record.url: record for record in records}.values())

    def _fetch_press_releases(
        self,
        start_date: str,
        end_date: str,
        *,
        max_pages: int,
    ) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str]] = []
        for page in range(1, max_pages + 1):
            url = self.press_archive_url if page == 1 else f"{self.press_archive_url}page/{page}/"
            soup = BeautifulSoup(request_html(self.session, url), "html.parser")
            items = soup.select("li.collection-result")
            if not items:
                break

            oldest_on_page: str | None = None
            for item in items:
                link_node = item.select_one("a.collection-result__link[href]")
                if link_node is None:
                    continue

                link = str(link_node.get("href", "")).strip()
                if "/releases/office-of-the-spokesperson/" not in link:
                    continue

                published_at = self._extract_collection_date(item)
                if not published_at:
                    continue

                if oldest_on_page is None or published_at < oldest_on_page:
                    oldest_on_page = published_at

                if not (start_date <= published_at <= end_date):
                    continue

                title = clean_text(link_node.get_text(" ", strip=True))
                candidates.append((link, title, published_at))

            if oldest_on_page is not None and oldest_on_page < start_date:
                break

        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._parse_press_release_article_threadsafe, link, title, published_at): (
                    link,
                    title,
                    published_at,
                )
                for link, title, published_at in candidates
            }
            for future in as_completed(futures):
                records.append(future.result())

        return sorted(records, key=lambda record: (record.published_at, record.url), reverse=True)

    def _fetch_press_briefings(self, start_date: str, end_date: str, max_pages: int = 8) -> list[ScrapedRecord]:
        records: list[ScrapedRecord] = []
        for page in range(1, max_pages + 1):
            payload = request_json(self.session, self.briefing_endpoint.format(page=page), headers=STATE_HEADERS)
            if not isinstance(payload, list):
                return records

            matched = 0
            for item in payload:
                link = str(item.get("link", ""))
                published_at = parse_us_date(str(item.get("date", "")))
                if "/briefings/department-press-briefing" not in link:
                    continue
                matched += 1
                if not (start_date <= published_at <= end_date):
                    continue
                records.append(self._make_record(item, "state_department_press_briefing"))

            if matched == 0:
                break
        return records

    def _make_record(self, item: dict[str, object], source_kind: str) -> ScrapedRecord:
        raw_html = str(
            ((item.get("content") or {}) if isinstance(item.get("content"), dict) else {}).get("rendered", "")
        )
        soup = BeautifulSoup(raw_html, "html.parser")

        speaker = ""
        speaker_node = soup.select_one(".article-meta__author-bureau")
        if speaker_node:
            speaker = clean_text(speaker_node.get_text(" ", strip=True))

        for selector in [
            ".wp-block-default-hero-container",
            "#breadcrumb__wrapper",
            ".article-meta",
            ".wp-block-summary-article-index",
            ".wp-block-buttons",
            ".wp-block-tags-block",
            ".related-content",
            ".share-this-page",
            "script",
            "style",
        ]:
            for node in soup.select(selector):
                node.decompose()

        content = clean_text(soup.get_text("\n"))
        title = clean_text(
            strip_html(
                str(((item.get("title") or {}) if isinstance(item.get("title"), dict) else {}).get("rendered", ""))
            )
        )
        if not title or not content:
            raise ValueError(f"Missing title or content for {item.get('link')}")

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=parse_us_date(str(item.get("date", ""))),
            url=str(item.get("link", "")),
            title=title,
            content=content,
            source_kind=source_kind,
            language="en",
            speaker=speaker,
        )

    def _parse_press_release_article(self, url: str, title: str, published_at: str) -> ScrapedRecord:
        html_text = request_html(self.session, url)
        return self._make_press_release_record_from_html(html_text, url, title, published_at)

    def _parse_press_release_article_threadsafe(self, url: str, title: str, published_at: str) -> ScrapedRecord:
        with requests.Session() as session:
            html_text = request_html(session, url)
        return self._make_press_release_record_from_html(html_text, url, title, published_at)

    def _make_press_release_record_from_html(
        self,
        html_text: str,
        url: str,
        title: str,
        published_at: str,
    ) -> ScrapedRecord:
        soup = BeautifulSoup(html_text, "html.parser")
        content_node = soup.select_one(".entry-content")
        if content_node is None:
            raise ValueError(f"Missing press-release body for {url}")

        speaker = ""
        speaker_node = soup.select_one(".article-meta__author-bureau")
        if speaker_node:
            speaker = clean_text(speaker_node.get_text(" ", strip=True))

        for selector in [
            ".wp-block-breadcrumbs",
            ".featured-content__headline",
            ".article-meta",
            ".wp-block-tags-block",
            ".tags-block",
            ".share-this-page",
            ".social-share",
            ".copy-to-clipboard",
            "button",
            "script",
            "style",
        ]:
            for node in content_node.select(selector):
                node.decompose()

        content = clean_text(content_node.get_text("\n"))
        if not content:
            raise ValueError(f"Missing parsed content for {url}")

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=url,
            title=title,
            content=content,
            source_kind="state_press_release",
            language="en",
            speaker=speaker,
        )

    @staticmethod
    def _extract_collection_date(item: BeautifulSoup) -> str:
        meta = item.select_one(".collection-result-meta")
        if meta is None:
            return ""
        for span in meta.select("span"):
            text = clean_text(span.get_text(" ", strip=True))
            if re.fullmatch(r"[A-Za-z]+ \d{1,2}, \d{4}", text):
                return parse_us_date(text)
        return ""


class UkFcdoNewsSource:
    country_code = "UK"
    site_root = "https://www.gov.uk"
    search_api_url = "https://www.gov.uk/api/search.json"
    allowed_document_types = {
        "press_release": "fcdo_press_release",
        "speech": "fcdo_speech",
        "oral_statement": "fcdo_oral_statement_to_parliament",
        "written_statement": "fcdo_written_statement_to_parliament",
        "news_article": "fcdo_news_story",
        "world_news_story": "fcdo_world_news_story",
        "world_location_news_article": "fcdo_world_news_story",
        "authored_article": "fcdo_authored_article",
    }
    search_fields = (
        "title",
        "description",
        "link",
        "public_timestamp",
        "content_store_document_type",
        "format",
    )

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 25, 45))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 20) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str, str]] = []
        page_size = 150
        for page_index in range(max_pages):
            page_url = f"{self.search_api_url}?{urlencode(self._search_params(start_date, end_date, page_size, page_index * page_size), doseq=True)}"
            payload = request_json(self.session, page_url)
            if not isinstance(payload, dict):
                break
            items = payload.get("results", [])
            if not isinstance(items, list) or not items:
                break

            for item in items:
                if not isinstance(item, dict):
                    continue
                href = clean_text(str(item.get("link", "")))
                if not href.startswith("/government/"):
                    continue

                title = clean_text(str(item.get("title", "")))
                public_timestamp = clean_text(str(item.get("public_timestamp", "")))
                if not title or not public_timestamp:
                    continue
                published_at = parse_us_date(public_timestamp)
                if not published_at:
                    continue

                source_kind = self._extract_result_format(item)
                if not source_kind:
                    continue
                candidates.append((urljoin(self.site_root, href), title, published_at, source_kind))

            if len(items) < page_size:
                break

        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._parse_article_threadsafe, url, title, published_at, source_kind): (
                    url,
                    title,
                    published_at,
                    source_kind,
                )
                for url, title, published_at, source_kind in candidates
            }
            for future in as_completed(futures):
                records.append(future.result())

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    @staticmethod
    def _search_params(start_date: str, end_date: str, page_size: int, start: int) -> list[tuple[str, str]]:
        params = [
            ("filter_organisations", "foreign-commonwealth-development-office"),
            ("filter_content_purpose_supergroup", "news_and_communications"),
            ("filter_public_timestamp", f"from:{start_date},to:{end_date}"),
            ("order", "-public_timestamp"),
            ("count", str(page_size)),
            ("start", str(start)),
        ]
        params.extend(("fields", field) for field in UkFcdoNewsSource.search_fields)
        return params

    @staticmethod
    def _extract_result_format(item: dict[str, object]) -> str:
        document_type = clean_text(str(item.get("content_store_document_type", ""))).lower()
        return UkFcdoNewsSource.allowed_document_types.get(document_type, "")

    def _parse_article_threadsafe(
        self,
        url: str,
        title: str,
        published_at: str,
        source_kind: str,
    ) -> ScrapedRecord:
        with requests.Session() as session:
            html_text = request_html(session, url)
        return self._make_record_from_html(html_text, url, title, published_at, source_kind)

    def _make_record_from_html(
        self,
        html_text: str,
        url: str,
        title: str,
        published_at: str,
        source_kind: str,
    ) -> ScrapedRecord:
        soup = BeautifulSoup(html_text, "html.parser")
        main = soup.select_one("main") or soup.body
        if main is None:
            raise ValueError(f"Missing main article body for {url}")

        for selector in [
            "nav",
            "header",
            "footer",
            "aside",
            "form",
            ".gem-c-share-links",
            ".govuk-related-navigation",
            ".gem-c-contextual-footer",
            ".gem-c-contextual-sidebar",
            "script",
            "style",
        ]:
            for node in main.select(selector):
                node.decompose()

        content_parts: list[str] = []
        lead = main.select_one(".gem-c-lead-paragraph, .govuk-lead-paragraph")
        if lead is not None:
            lead_text = clean_text(lead.get_text(" ", strip=True))
            if lead_text:
                content_parts.append(lead_text)

        body = main.select_one(".gem-c-govspeak, .govuk-govspeak")
        if body is not None:
            for node in body.select("p, li"):
                text = clean_text(node.get_text(" ", strip=True))
                if text:
                    content_parts.append(text)

        if not content_parts:
            content = clean_text(main.get_text("\n"))
        else:
            content = clean_text("\n".join(dict.fromkeys(content_parts)))

        if not content:
            raise ValueError(f"Missing parsed content for {url}")

        speaker_nodes = main.select(".gem-c-metadata a[href*='/government/people/'], .gem-c-metadata a[href*='/government/organisations/']")
        speaker = clean_text("; ".join(node.get_text(" ", strip=True) for node in speaker_nodes))

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=source_kind,
            language="en",
            speaker=speaker,
        )


class JapanMofaPressReleaseSource:
    country_code = "JP"
    current_release_url = "https://www.mofa.go.jp/press/release/"
    monthly_release_template = "https://www.mofa.go.jp/press/release/{year:04d}{month:02d}_index.html"
    current_conference_url = "https://www.mofa.go.jp/press/kaiken/"
    monthly_conference_template = "https://www.mofa.go.jp/press/kaiken/pc_{year:04d}{month:02d}_index.html"
    statement_pages = (
        "https://www.mofa.go.jp/press/statement/fm.html",
        "https://www.mofa.go.jp/press/statement/fm_archives.html",
        "https://www.mofa.go.jp/press/statement/ps.html",
        "https://www.mofa.go.jp/press/statement/ps_bn.html",
    )

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 4) -> list[ScrapedRecord]:
        del max_pages
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=120)
        return self.fetch_between(start_date.isoformat(), end_date.isoformat())

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 0) -> list[ScrapedRecord]:
        del max_pages
        candidates: list[tuple[str, str, str, str]] = []
        today = datetime.now(timezone.utc).date()
        for year, month in iter_months(start_date, end_date):
            release_index_url = (
                self.current_release_url
                if (year, month) == (today.year, today.month)
                else self.monthly_release_template.format(year=year, month=month)
            )
            release_markdown = request_markdown_via_jina(release_index_url)
            candidates.extend(self._extract_release_candidates(release_markdown, year, month, start_date, end_date))

            conference_index_url = (
                self.current_conference_url
                if (year, month) == (today.year, today.month)
                else self.monthly_conference_template.format(year=year, month=month)
            )
            conference_markdown = request_markdown_via_jina(conference_index_url)
            candidates.extend(self._extract_conference_candidates(conference_markdown, start_date, end_date))

        for statement_page in self.statement_pages:
            statement_markdown = request_markdown_via_jina(statement_page)
            candidates.extend(self._extract_statement_candidates(statement_markdown, start_date, end_date))

        deduped_candidates: dict[str, tuple[str, str, str, str]] = {}
        for url, title, published_at, source_kind in candidates:
            deduped_candidates[url] = (url, title, published_at, source_kind)

        if not deduped_candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(self._parse_article_threadsafe, url, title, published_at, source_kind): (
                    url,
                    title,
                    published_at,
                    source_kind,
                )
                for url, title, published_at, source_kind in deduped_candidates.values()
            }
            for future in as_completed(futures):
                try:
                    records.append(future.result())
                except Exception:
                    continue

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _extract_release_candidates(
        self,
        markdown: str,
        year: int,
        month: int,
        start_date: str,
        end_date: str,
    ) -> list[tuple[str, str, str, str]]:
        candidates: list[tuple[str, str, str, str]] = []
        current_date: str | None = None
        seen_urls: set[str] = set()

        for raw_line in markdown.splitlines():
            line = clean_text(raw_line)
            if not line:
                continue
            if line.startswith("### Archives") or line.startswith("Archives List|"):
                break
            if line.startswith("[Page Top]") or line.startswith("[Back to Press Releases]"):
                break
            parsed_date = month_day_with_year_to_iso(line, year)
            if parsed_date:
                current_date = parsed_date
                continue
            if current_date is None or not (start_date <= current_date <= end_date):
                continue
            if not raw_line.lstrip().startswith("*"):
                continue

            for title, url in markdown_links(raw_line):
                if not self._is_valid_document_url(url):
                    continue
                normalized = normalize_generic_url(url)
                if normalized in seen_urls:
                    continue
                seen_urls.add(normalized)
                candidates.append((normalized, title, current_date, "jp_mofa_written_statement"))

        return candidates

    def _extract_conference_candidates(
        self,
        markdown: str,
        start_date: str,
        end_date: str,
    ) -> list[tuple[str, str, str, str]]:
        candidates: list[tuple[str, str, str, str]] = []
        seen_urls: set[str] = set()

        for raw_line in markdown.splitlines():
            for title, url in markdown_links(raw_line):
                if "/press/kaiken/" not in url or "_index.html" in url or "#topic" in url:
                    continue
                if "Press Conference" not in title:
                    continue
                match = JP_TITLE_DATE_RE.search(title)
                if not match:
                    continue
                published_at = parse_us_date(match.group("date"))
                if not (start_date <= published_at <= end_date):
                    continue
                normalized = normalize_generic_url(url)
                if normalized in seen_urls:
                    continue
                seen_urls.add(normalized)
                candidates.append((normalized, title, published_at, "jp_mofa_press_conference"))

        return candidates

    def _extract_statement_candidates(
        self,
        markdown: str,
        start_date: str,
        end_date: str,
    ) -> list[tuple[str, str, str, str]]:
        candidates: list[tuple[str, str, str, str]] = []
        seen_urls: set[str] = set()

        for raw_line in markdown.splitlines():
            if not raw_line.lstrip().startswith("*"):
                continue
            for title, url in markdown_links(raw_line):
                if not self._is_valid_document_url(url):
                    continue
                match = JP_TITLE_DATE_RE.search(title)
                if not match:
                    continue
                published_at = parse_us_date(match.group("date"))
                if not (start_date <= published_at <= end_date):
                    continue
                normalized = normalize_generic_url(url)
                if normalized in seen_urls:
                    continue
                seen_urls.add(normalized)
                candidates.append((normalized, title, published_at, "jp_mofa_written_statement"))

        return candidates

    def _parse_article_threadsafe(self, url: str, title: str, published_at: str, source_kind: str) -> ScrapedRecord:
        markdown = request_markdown_via_jina(url)
        return self._make_record_from_markdown(markdown, url, title, published_at, source_kind)

    def _make_record_from_markdown(
        self,
        markdown: str,
        url: str,
        title: str,
        published_at: str,
        source_kind: str,
    ) -> ScrapedRecord:
        lines = [line.rstrip() for line in markdown.splitlines()]
        content_parts: list[str] = []
        started = False
        stop_headings = {"Related Links", "Page Top"}

        for raw_line in lines:
            line = raw_line.strip()
            text = clean_text(line)
            if not text:
                continue
            if text == "The text will be coming soon.":
                raise ValueError(f"Text not yet available for {url}")

            if not started:
                header_match = JP_CONFERENCE_HEADER_RE.match(text)
                if header_match:
                    published_at = parse_us_date(header_match.group("date"))
                    started = True
                    continue
                if JP_LINE_DATE_RE.fullmatch(text) and "," in text:
                    published_at = parse_us_date(text)
                    started = True
                    continue
                continue

            if text in stop_headings or text.startswith("Copyright "):
                break
            if text.startswith("Back to ") or text.startswith("About Us") or text.startswith("News"):
                break
            if line.startswith("[") or line.startswith("![") or line.startswith("*   ["):
                continue
            if text.startswith("This is a provisional translation"):
                continue
            if text == "Japanese" or text == title:
                continue
            content_parts.append(text)

        content = clean_text("\n".join(content_parts))
        if not content:
            raise ValueError(f"Missing parsed content for {url}")

        speaker = ""
        if "Foreign Minister" in title:
            speaker = "Foreign Minister"
        elif "Press Secretary" in title:
            speaker = "Press Secretary"
        elif "Foreign Press Secretary" in title:
            speaker = "Foreign Press Secretary"

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=source_kind,
            language="en",
            speaker=speaker,
        )

    @staticmethod
    def _is_valid_document_url(url: str) -> bool:
        normalized = clean_text(url)
        if not normalized.startswith("https://www.mofa.go.jp/"):
            return False
        if "/mofaj/" in normalized:
            return False
        if normalized.endswith("index.html") or normalized.endswith("_index.html"):
            return False
        if "#" in normalized or normalized.startswith("mailto:"):
            return False
        return normalized.endswith(".html")


class KoreaMofaPressReleaseSource:
    country_code = "KR"
    list_url = "https://www.mofa.go.kr/eng/brd/m_5676/list.do"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 20, 45))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 20) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str]] = []
        for page in range(1, max_pages + 1):
            page_url = f"{self.list_url}?page={page}"
            soup = BeautifulSoup(request_html(self.session, page_url), "html.parser")
            rows = soup.select("tbody tr")
            if not rows:
                rows = soup.select("tr")
            page_candidates = self._extract_list_candidates(rows, page)
            if not page_candidates:
                break

            oldest_on_page = min(published_at for _, _, published_at in page_candidates)
            for url, title, published_at in page_candidates:
                if start_date <= published_at <= end_date:
                    candidates.append((url, title, published_at))

            if oldest_on_page < start_date:
                break

        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._parse_article_threadsafe, url, title, published_at): (
                    url,
                    title,
                    published_at,
                )
                for url, title, published_at in candidates
            }
            for future in as_completed(futures):
                records.append(future.result())

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _extract_list_candidates(self, rows: list[BeautifulSoup], page: int) -> list[tuple[str, str, str]]:
        candidates: list[tuple[str, str, str]] = []
        for row in rows:
            link_node = row.select_one("a[href]")
            if link_node is None:
                continue

            title = clean_text(link_node.get_text(" ", strip=True))
            row_text = clean_text(row.get_text(" ", strip=True))
            match = ISO_LIKE_DATE_RE.search(row_text)
            if not match:
                continue
            published_at = parse_iso_like_date(match.group(1))
            href = str(link_node.get("href", "")).strip()
            onclick = str(link_node.get("onclick", "")).strip()
            seq_match = re.search(r"f_view\('(?P<seq>\d+)'\)", onclick)
            if seq_match:
                href = f"./view.do?seq={seq_match.group('seq')}&page={page}"
            if href == "#" or not href:
                continue
            candidates.append((normalize_generic_url(urljoin(self.list_url, href)), title, published_at))
        return candidates

    def _parse_article_threadsafe(self, url: str, title: str, published_at: str) -> ScrapedRecord:
        with requests.Session() as session:
            html_text = request_html(session, url)
        return self._make_record_from_html(html_text, url, title, published_at)

    def _make_record_from_html(self, html_text: str, url: str, title: str, published_at: str) -> ScrapedRecord:
        soup = BeautifulSoup(html_text, "html.parser")
        main = soup.select_one("#contents") or soup.select_one("main") or soup.body
        if main is None:
            raise ValueError(f"Missing main article body for {url}")

        title_candidate = ""
        for selector in [".board_detail .bo_head h2", ".bo_head h2", ".board_view_tit", ".view_tit", ".title", "h3", "h2", "h1", "title"]:
            node = soup.select_one(selector)
            if node is not None:
                title_candidate = clean_text(node.get_text(" ", strip=True))
                if title_candidate:
                    break
        if title_candidate:
            title = title_candidate

        whole_text = clean_text(main.get_text("\n"))
        match = ISO_LIKE_DATE_RE.search(whole_text)
        if match:
            published_at = parse_iso_like_date(match.group(1))

        for selector in [
            "nav",
            "header",
            "footer",
            "aside",
            ".bo_head",
            ".board_navi",
            ".board_btn",
            ".article_btn",
            ".view_file",
            ".attach",
            ".sns",
            "script",
            "style",
        ]:
            for node in main.select(selector):
                node.decompose()

        candidate_nodes = [
            ".board_detail .bo_con",
            ".bo_con",
            ".se-contents",
            ".board_view_cont",
            ".board_view_con",
            ".board_view_body",
            ".view_cont",
            ".cont_view",
            ".editor_view",
            ".board_view",
            ".bbs_view",
            ".content",
        ]
        content = ""
        for selector in candidate_nodes:
            node = main.select_one(selector)
            if node is None:
                continue
            text = clean_text(node.get_text("\n"))
            if len(text) > len(content):
                content = text

        if not content:
            paragraphs = [clean_text(node.get_text(" ", strip=True)) for node in main.select("p, li")]
            content = clean_text("\n".join(text for text in paragraphs if text))

        if not content:
            raise ValueError(f"Missing parsed content for {url}")

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind="kr_mofa_press_release",
            language="en",
            speaker="",
        )


class GermanyForeignOfficeSource:
    country_code = "DE"
    history_start_date = "2023-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 365
    history_max_pages = 180
    site_root = "https://www.auswaertiges-amt.de"
    archive_url = "https://www.auswaertiges-amt.de/ajax/json-filterlist/en/newsroom/news/609204-609204"
    page_size = 20
    source_kind_by_name = {
        "press release": "de_aa_press_release",
        "speech": "de_aa_speech",
        "interview": "de_aa_interview",
        "article": "de_aa_article",
    }

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 30, 150))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 90) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str, str]] = []
        for page_index in range(max_pages):
            offset = page_index * self.page_size
            page_url = self.archive_url if offset == 0 else f"{self.archive_url}?offset={offset}"
            payload = request_json(self.session, page_url)
            if not isinstance(payload, dict):
                break

            items = payload.get("items", [])
            if not isinstance(items, list) or not items:
                break

            oldest_on_page: str | None = None
            for item in items:
                if not isinstance(item, dict):
                    continue

                published_at = parse_de_date(str(item.get("date", "")))
                headline = clean_text(str(item.get("headline", "")))
                href = clean_text(str(item.get("link", "")))
                result_name = clean_text(str(item.get("name", ""))).lower()
                source_kind = self.source_kind_by_name.get(result_name, "")
                if not headline or not href or not source_kind:
                    continue

                if oldest_on_page is None or published_at < oldest_on_page:
                    oldest_on_page = published_at

                if not (start_date <= published_at <= end_date):
                    continue

                candidates.append((urljoin(self.site_root, href), headline, published_at, source_kind))

            if oldest_on_page is not None and oldest_on_page < start_date:
                break

        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._parse_article_threadsafe, url, title, published_at, source_kind): (
                    url,
                    title,
                    published_at,
                    source_kind,
                )
                for url, title, published_at, source_kind in candidates
            }
            failures = 0
            for future in as_completed(futures):
                url, _, _, _ = futures[future]
                try:
                    records.append(future.result())
                except Exception as exc:
                    failures += 1
                    if failures <= 3:
                        print(f"DE: skipping {url} after fetch/parse error: {exc}")

        if not records:
            raise RuntimeError("Germany Foreign Office fetch produced no parseable records.")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _parse_article_threadsafe(
        self,
        url: str,
        title: str,
        published_at: str,
        source_kind: str,
    ) -> ScrapedRecord:
        with requests.Session() as session:
            html_text = request_html(session, url)
        return self._make_record_from_html(html_text, url, title, published_at, source_kind)

    def _make_record_from_html(
        self,
        html_text: str,
        url: str,
        title: str,
        published_at: str,
        source_kind: str,
    ) -> ScrapedRecord:
        soup = BeautifulSoup(html_text, "html.parser")
        main = soup.select_one("main") or soup.body
        if main is None:
            raise ValueError(f"Missing German Foreign Office article body for {url}")

        for selector in [
            "nav",
            "header",
            "footer",
            "aside",
            "form",
            "script",
            "style",
            ".search__helper-text-wrapper",
            ".modul-sidebar",
            ".modul-list",
            ".share",
            ".socialmedia",
        ]:
            for node in main.select(selector):
                node.decompose()

        content_parts: list[str] = []
        for node in main.select("p, li, h2, h3, blockquote"):
            text = clean_text(node.get_text(" ", strip=True))
            if not text:
                continue
            if text == title or text in {"Print page", "Share page", "Top of page", "Keywords"}:
                continue
            if text.startswith("Overview ") or text.startswith("Overview “") or text.startswith("Overview \""):
                break
            content_parts.append(text)

        content = clean_text("\n".join(dict.fromkeys(content_parts)))
        if not content:
            content = clean_text(main.get_text("\n"))
            for marker in ("Overview \"Newsroom\"", "Overview “Newsroom”", "Keywords", "Print page", "Share page"):
                if marker in content:
                    content = content.split(marker, 1)[0].strip()
                    break

        if not content:
            raise ValueError(f"Missing parsed German Foreign Office content for {url}")

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=source_kind,
            language="en",
            speaker=self._speaker(title),
        )

    @staticmethod
    def _speaker(title: str) -> str:
        cleaned = clean_text(title)
        match = re.search(r"(?:Speech|Statement|Interview)\s+(?:by|with)\s+(.+?)(?:\s+at\s+|:|$)", cleaned, re.I)
        if match:
            return clean_text(match.group(1))
        if "Johann Wadephul" in cleaned:
            return "Johann Wadephul"
        if "Annalena Baerbock" in cleaned:
            return "Annalena Baerbock"
        return "Federal Foreign Office"


class ItalyMfaPressReleaseSource:
    country_code = "IT"
    history_start_date = "2022-01-01"
    bootstrap_history_start_date = "2026-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 150
    archive_url = "https://www.esteri.it/en/sala_stampa/archivionotizie/comunicati/"
    month_page_size = 10

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 30, 150))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 20) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str, str]] = []
        seen_urls: set[str] = set()

        for year, month in reversed(iter_months(start_date, end_date)):
            for page_number in range(1, max_pages + 1):
                page_markdown = request_markdown_via_jina(self._month_url(year, month, page_number))
                page_candidates, has_next = self._extract_listing_candidates(page_markdown)
                if not page_candidates:
                    break

                oldest_on_page: str | None = None
                for url, title, published_at, excerpt in page_candidates:
                    if oldest_on_page is None or published_at < oldest_on_page:
                        oldest_on_page = published_at
                    if not (start_date <= published_at <= end_date):
                        continue
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    candidates.append((url, title, published_at, excerpt))

                if not has_next or (oldest_on_page is not None and oldest_on_page < start_date):
                    break

        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(self._fetch_detail_record, url, title, published_at, excerpt): (
                    url,
                    title,
                    published_at,
                    excerpt,
                )
                for url, title, published_at, excerpt in candidates
            }
            for future in as_completed(futures):
                url, title, published_at, excerpt = futures[future]
                try:
                    records.append(future.result())
                except Exception as exc:
                    print(f"IT: skipping {url} after fetch/parse error: {exc}")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _month_url(self, year: int, month: int, page_number: int) -> str:
        params = {
            "lang": "en",
            "anno_pub": str(year),
            "mese_pub": str(month),
        }
        if page_number > 1:
            params["pag"] = str(page_number)
        return f"{self.archive_url}?{urlencode(params)}"

    def _extract_listing_candidates(self, markdown: str) -> tuple[list[tuple[str, str, str, str]], bool]:
        lines = [clean_text(line) for line in markdown.splitlines()]
        candidates: list[tuple[str, str, str, str]] = []
        current_date: str | None = None
        pending: dict[str, str | list[str]] | None = None

        def flush_pending() -> None:
            nonlocal pending
            if pending is None:
                return
            url = str(pending["url"])
            title = str(pending["title"])
            published_at = str(pending["published_at"])
            excerpt_lines = pending.get("excerpt_lines") or []
            excerpt = clean_text(" ".join(str(line) for line in excerpt_lines))
            excerpt = excerpt.removesuffix("[...]").removesuffix("[…]").strip()
            candidates.append((url, title, published_at, excerpt))
            pending = None

        for line in lines:
            if not line:
                continue
            date_match = ITALY_LISTING_DATE_RE.fullmatch(line)
            if date_match:
                flush_pending()
                current_date = parse_us_date(date_match.group("date"))
                continue

            title_match = ITALY_HEADING_LINK_RE.match(line)
            if title_match and current_date is not None:
                flush_pending()
                title = self._normalize_title(title_match.group("title"))
                url = normalize_generic_url(title_match.group("url"))
                if not title or "/en/sala_stampa/archivionotizie/comunicati/" not in url:
                    pending = None
                    continue
                pending = {
                    "url": url,
                    "title": title,
                    "published_at": current_date,
                    "excerpt_lines": [],
                }
                continue

            if pending is None:
                continue
            if line.startswith("[Read more]("):
                flush_pending()
                continue
            if line.startswith("## Pagination") or line.startswith("#### Browse section"):
                flush_pending()
                break
            excerpt_lines = pending["excerpt_lines"]
            assert isinstance(excerpt_lines, list)
            excerpt_lines.append(line)

        flush_pending()

        return candidates, "[Next page](" in markdown

    def _fetch_detail_record(
        self,
        url: str,
        fallback_title: str,
        fallback_published_at: str,
        fallback_excerpt: str,
    ) -> ScrapedRecord:
        try:
            markdown = request_markdown_via_jina(url)
        except Exception:
            return self._fetch_detail_record_from_html(url, fallback_title, fallback_published_at, fallback_excerpt)
        if self._is_blocked_markdown(markdown):
            return self._fetch_detail_record_from_html(url, fallback_title, fallback_published_at, fallback_excerpt)
        title = self._extract_title(markdown) or fallback_title
        title = self._normalize_title(title) or fallback_title
        published_at = self._extract_published_at(markdown) or fallback_published_at
        content = self._extract_content(markdown, title)
        if self._is_blocked_title_or_content(title, content):
            return self._fetch_detail_record_from_html(url, fallback_title, fallback_published_at, fallback_excerpt)

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=self._source_kind(title),
            language="en",
            speaker=self._speaker(title),
        )

    def _fetch_detail_record_from_html(
        self,
        url: str,
        fallback_title: str,
        fallback_published_at: str,
        fallback_excerpt: str,
    ) -> ScrapedRecord:
        last_error: Exception | None = None
        html_text = ""
        for attempt in range(3):
            try:
                html_text = request_html_with_playwright(url)
                if not self._is_blocked_markdown(html_text):
                    break
            except Exception as exc:
                last_error = exc
                time.sleep(1.5 * (attempt + 1))
        if not html_text:
            if fallback_excerpt:
                return self._build_listing_fallback_record(
                    url,
                    fallback_title,
                    fallback_published_at,
                    fallback_excerpt,
                )
            assert last_error is not None
            raise last_error
        try:
            soup = BeautifulSoup(html_text, "html.parser")
            article = soup.select_one("article") or soup.select_one("main") or soup.body
            if article is None:
                raise ValueError(f"Missing Italy MAECI article body for {url}")

            title_node = article.select_one("h1")
            title = self._normalize_title(title_node.get_text(" ", strip=True)) if title_node else fallback_title
            if not title:
                title = fallback_title

            for selector in [
                "nav",
                "header",
                "footer",
                "aside",
                "script",
                "style",
                "form",
                ".share",
                ".social",
                ".related-posts",
                ".related-news",
            ]:
                for node in article.select(selector):
                    node.decompose()

            article_text = clean_text(article.get_text("\n"))
            published_match = re.search(r"Publication date:\s*(?P<date>[A-Za-z]+\s+\d{1,2}\s+\d{4})", article_text)
            published_at = parse_us_date(published_match.group("date")) if published_match else fallback_published_at

            content_root = article.select_one(".entry-content") or article
            content_parts = [clean_text(node.get_text(" ", strip=True)) for node in content_root.select("p, li, h2, h3")]
            content = clean_text("\n".join(text for text in content_parts if text))
            if not content:
                content = article_text
                for marker in ("Browse section", "You might also be interested in.."):
                    if marker in content:
                        content = content.split(marker, 1)[0].strip()
                        break
                for marker in ("Publication date:", "Tipology:", title):
                    content = content.replace(marker, "").strip()
            if not content:
                raise ValueError(f"Missing parsed Italy MAECI content for {url}")
            if self._is_blocked_title_or_content(title, content):
                raise ValueError(f"Italy MAECI returned a blocked page for {url}")
        except Exception as exc:
            if fallback_excerpt:
                return self._build_listing_fallback_record(
                    url,
                    fallback_title,
                    fallback_published_at,
                    fallback_excerpt,
                )
            raise exc

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=self._source_kind(title),
            language="en",
            speaker=self._speaker(title),
        )

    def _build_listing_fallback_record(
        self,
        url: str,
        fallback_title: str,
        fallback_published_at: str,
        fallback_excerpt: str,
    ) -> ScrapedRecord:
        content = clean_text(fallback_excerpt).removesuffix("[...]").removesuffix("[…]").strip()
        if not content:
            raise ValueError(f"Missing Italy MAECI listing excerpt for {url}")
        if self._is_blocked_title_or_content(fallback_title, content):
            raise ValueError(f"Italy MAECI listing excerpt looks blocked for {url}")
        return ScrapedRecord(
            country_code=self.country_code,
            published_at=fallback_published_at,
            url=normalize_generic_url(url),
            title=fallback_title,
            content=content,
            source_kind=self._source_kind(fallback_title),
            language="en",
            speaker=self._speaker(fallback_title),
        )

    @staticmethod
    def _extract_title(markdown: str) -> str:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Title: "):
                return cleaned.removeprefix("Title: ").strip()
        return ""

    @staticmethod
    def _extract_published_at(markdown: str) -> str | None:
        match = re.search(r"Publication date:\s*(?P<date>[A-Za-z]+\s+\d{1,2}\s+\d{4})", markdown)
        if not match:
            return None
        return parse_us_date(match.group("date"))

    @staticmethod
    def _extract_content(markdown: str, title: str) -> str:
        lines = [clean_text(line) for line in markdown.splitlines()]
        normalized_title = ItalyMfaPressReleaseSource._normalize_title(title)
        start_index = 0
        for index, line in enumerate(lines):
            if not line.startswith("# "):
                continue
            heading = ItalyMfaPressReleaseSource._normalize_title(line.removeprefix("# ").strip())
            if heading == normalized_title:
                start_index = index + 1

        content_lines: list[str] = []
        for line in lines[start_index:]:
            if not line:
                continue
            if line.startswith("* **Tag:**"):
                break
            if line.startswith("#### Browse section") or line.startswith("### You might also be interested in.."):
                break
            if line.startswith("* **Publication date:**") or line.startswith("* **Tipology:**"):
                continue
            if line == title or ItalyMfaPressReleaseSource._normalize_title(line) == normalized_title:
                continue
            content_lines.append(line)

        content = clean_text("\n".join(content_lines))
        if not content:
            raise ValueError(f"Missing parsed Italy MAECI content for {title}")
        return content

    @staticmethod
    def _normalize_title(title: str) -> str:
        cleaned = clean_text(title)
        cleaned = ITALY_SITE_TITLE_SUFFIX_RE.sub("", cleaned).strip()
        return cleaned.replace("＊", "'").replace("※", '"').replace("§", '"')

    @staticmethod
    def _is_blocked_markdown(markdown: str) -> bool:
        lowered = clean_text(markdown).lower()
        return (
            "radware bot manager captcha" in lowered
            or "completa il captcha" in lowered
            or "validate.perfdrive.com" in lowered
            or "accesso temporaneamente limitato" in lowered
            or "temporarily limited access" in lowered
        )

    @staticmethod
    def _is_blocked_title_or_content(title: str, content: str) -> bool:
        lowered = clean_text(f"{title}\n{content[:400]}").lower()
        return (
            "radware bot manager captcha" in lowered
            or "accesso temporaneamente limitato" in lowered
            or "temporarily limited access" in lowered
        )

    @staticmethod
    def _source_kind(title: str) -> str:
        lowered = clean_text(title).lower()
        if "joint statement" in lowered or lowered.startswith("statement"):
            return "it_maeci_statement"
        if "interview" in lowered:
            return "it_maeci_interview"
        return "it_maeci_press_release"

    @staticmethod
    def _speaker(title: str) -> str:
        cleaned = clean_text(title)
        speaker_map = {
            "Tajani": "Antonio Tajani",
            "Tripodi": "Maria Tripodi",
            "Cirielli": "Edmondo Cirielli",
            "Terzi": "Giulio Terzi",
        }
        for marker, speaker in speaker_map.items():
            if marker in cleaned:
                return speaker
        if cleaned.startswith("Minister "):
            return "Minister of Foreign Affairs"
        if cleaned.startswith("Undersecretary "):
            return "Undersecretary of State"
        if cleaned.startswith("Deputy Minister "):
            return "Deputy Minister"
        return "MAECI"


class AustraliaForeignMinisterMediaReleaseSource:
    country_code = "AU"
    history_start_date = "2022-05-23"
    bootstrap_history_start_date = "2026-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 150
    archive_url = "https://www.foreignminister.gov.au/minister/penny-wong/media-releases"
    rss_url = "https://www.foreignminister.gov.au/rss.xml"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 30, 180))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 24) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str]] = []
        seen_urls: set[str] = set()

        for page_index in range(max_pages):
            page_markdown = request_markdown_via_jina(self._page_url(page_index))
            page_candidates, has_next = self._extract_listing_candidates(page_markdown)
            if not page_candidates:
                break

            oldest_on_page: str | None = None
            for url, title, published_at in page_candidates:
                if oldest_on_page is None or published_at < oldest_on_page:
                    oldest_on_page = published_at
                if not (start_date <= published_at <= end_date):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                candidates.append((url, title, published_at))

            if not has_next or (oldest_on_page is not None and oldest_on_page < start_date):
                break

        if not candidates:
            candidates = self._fetch_rss_candidates(start_date, end_date)

        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self._fetch_detail_record, url, title, published_at): (url, title, published_at)
                for url, title, published_at in candidates
            }
            for future in as_completed(futures):
                url, title, published_at = futures[future]
                try:
                    records.append(future.result())
                except Exception as exc:
                    print(f"AU: skipping {url} after fetch/parse error: {exc}")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _page_url(self, page_index: int) -> str:
        if page_index <= 0:
            return self.archive_url
        return f"{self.archive_url}?page={page_index}"

    def _extract_listing_candidates(self, markdown: str) -> tuple[list[tuple[str, str, str]], bool]:
        candidates: list[tuple[str, str, str]] = []
        for raw_line in extract_jina_markdown_body(markdown).splitlines():
            line = clean_text(raw_line)
            if not line:
                continue
            match = AU_LISTING_ENTRY_RE.match(line)
            if not match:
                generic = self._extract_listing_candidate_generic(line)
                if generic is None:
                    continue
                candidates.append(generic)
                continue
            candidates.append(
                (
                    normalize_generic_url(match.group("url")),
                    clean_text(match.group("title")),
                    parse_us_date(match.group("date")),
                )
            )
        return candidates, "[Next page" in markdown

    def _extract_listing_candidate_generic(self, line: str) -> tuple[str, str, str] | None:
        if not line.startswith("*"):
            return None
        links = markdown_links(line)
        if len(links) != 1:
            return None
        title, url = links[0]
        if "foreignminister.gov.au" not in url or "/minister/penny-wong/media-release/" not in url:
            return None
        date_match = AU_INLINE_DATE_RE.search(line)
        if not date_match:
            return None
        return normalize_generic_url(url), clean_text(title), parse_us_date(date_match.group("date"))

    def _fetch_rss_candidates(self, start_date: str, end_date: str) -> list[tuple[str, str, str]]:
        try:
            response = self.session.get(self.rss_url, headers=BROWSER_HEADERS, timeout=45)
            response.raise_for_status()
        except requests.RequestException:
            return []

        soup = BeautifulSoup(response.text, "xml")
        candidates: list[tuple[str, str, str]] = []
        seen_urls: set[str] = set()
        for item in soup.select("item"):
            link_node = item.select_one("link")
            title_node = item.select_one("title")
            pubdate_node = item.select_one("pubDate")
            if link_node is None or title_node is None or pubdate_node is None:
                continue
            url = normalize_generic_url(clean_text(link_node.get_text(" ", strip=True)))
            if "/minister/penny-wong/media-release/" not in url:
                continue
            try:
                published_at = parsedate_to_datetime(clean_text(pubdate_node.get_text(" ", strip=True))).date().isoformat()
            except Exception:
                continue
            if not (start_date <= published_at <= end_date):
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            candidates.append((url, clean_text(title_node.get_text(" ", strip=True)), published_at))
        return sorted(candidates, key=lambda item: (item[2], item[0]))

    def _fetch_detail_record(self, url: str, fallback_title: str, fallback_published_at: str) -> ScrapedRecord:
        markdown = request_markdown_via_jina(url)
        title = self._extract_title(markdown) or fallback_title
        body = extract_jina_markdown_body(markdown)
        content = self._extract_content(body, title)
        return ScrapedRecord(
            country_code=self.country_code,
            published_at=fallback_published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=self._source_kind(title),
            language="en",
            speaker=self._speaker(title),
        )

    @staticmethod
    def _extract_title(markdown: str) -> str:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Title: "):
                return cleaned.removeprefix("Title: ").strip()
        return ""

    @staticmethod
    def _extract_content(body: str, title: str) -> str:
        lowered_body = body.lower()
        if "page not found" in lowered_body or "requested page could not be found" in lowered_body:
            raise ValueError("Australia FM detail page not found")

        content_lines: list[str] = []
        for raw_line in body.splitlines():
            line = clean_text(raw_line)
            if not line or normalize_compare_text(line) == normalize_compare_text(title):
                continue
            if line.startswith("[Back to top]") or line == "Back to top":
                break
            content_lines.append(line)

        content = clean_text("\n".join(content_lines))
        if not content:
            raise ValueError(f"Missing Australian FM content for {title}")
        return content

    @staticmethod
    def _source_kind(title: str) -> str:
        lowered = normalize_compare_text(title)
        if "joint statement" in lowered or lowered.startswith("statement"):
            return "au_fm_statement"
        return "au_fm_media_release"

    @staticmethod
    def _speaker(title: str) -> str:
        cleaned = clean_text(title)
        if "Deputy" in cleaned:
            return "Deputy Foreign Minister"
        return "Penny Wong"


class CanadaGlobalAffairsNewsSource:
    country_code = "CA"
    history_start_date = "2017-03-31"
    bootstrap_history_start_date = "2026-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 120
    api_url = "https://api.io.canada.ca/io-server/gc/news/en/v2"
    department = "departmentofforeignaffairstradeanddevelopment"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 2) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 90, 180))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 8) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str, str]] = []
        seen_urls: set[str] = set()
        cursor = f"{start_date}T00:00:00-05:00"

        for _ in range(max_pages):
            entries = self._fetch_batch(cursor)
            if not entries:
                break

            newest_timestamp: datetime | None = None
            for entry in entries:
                if not isinstance(entry, dict):
                    continue

                url = normalize_generic_url(str(entry.get("link", "")).strip())
                title = clean_text(str(entry.get("title", "")).strip())
                teaser = clean_text(str(entry.get("teaser", "")).strip())
                published_timestamp = clean_text(str(entry.get("publishedDate", "")).strip())
                if not url or not title or not published_timestamp:
                    continue

                published_at = parse_us_date(published_timestamp)
                try:
                    published_dt = datetime.fromisoformat(published_timestamp.replace("Z", "+00:00"))
                except ValueError:
                    published_dt = datetime.combine(iso_to_date(published_at), datetime.min.time())

                if newest_timestamp is None or published_dt > newest_timestamp:
                    newest_timestamp = published_dt

                if not (start_date <= published_at <= end_date):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                candidates.append((url, title, published_at, teaser))

            if newest_timestamp is None:
                break

            if len(entries) < 1000 or newest_timestamp.date().isoformat() >= end_date:
                break

            cursor = (newest_timestamp + timedelta(seconds=1)).isoformat()

        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._fetch_detail_record, url, title, published_at, teaser): (
                    url,
                    title,
                    published_at,
                    teaser,
                )
                for url, title, published_at, teaser in candidates
            }
            for future in as_completed(futures):
                url, title, published_at, teaser = futures[future]
                try:
                    records.append(future.result())
                except Exception as exc:
                    print(f"CA: skipping {url} after fetch/parse error: {exc}")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _fetch_batch(self, cursor: str) -> list[dict[str, object]]:
        query = (
            f"dept={self.department}"
            f"&sort=publishedDate"
            f"&orderBy=asc"
            f"&pick=1000"
            f"&publishedDate>={cursor}"
        )
        payload = request_json(self.session, f"{self.api_url}?{query}")
        if not isinstance(payload, dict):
            raise RuntimeError("Canada news API returned a non-dict payload")
        feed = payload.get("feed")
        if not isinstance(feed, dict):
            return []
        entries = feed.get("entry", [])
        if isinstance(entries, dict):
            return [entries]
        if not isinstance(entries, list):
            return []
        return [entry for entry in entries if isinstance(entry, dict)]

    def _fetch_detail_record(self, url: str, fallback_title: str, fallback_published_at: str, teaser: str) -> ScrapedRecord:
        content = ""
        title = fallback_title
        published_at = fallback_published_at
        source_kind = "ca_gac_news"

        try:
            markdown = request_markdown_via_jina(url)
            title = self._extract_title(markdown) or fallback_title
            published_at = self._extract_published_at(markdown) or fallback_published_at
            body = extract_jina_markdown_body(markdown)
            content = self._extract_content(body, title)
            source_kind = self._source_kind(body, title)
        except Exception:
            content = ""

        if not content:
            content = teaser or title

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=source_kind,
            language="en",
            speaker="Global Affairs Canada",
        )

    @staticmethod
    def _extract_title(markdown: str) -> str:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Title: "):
                return cleaned.removeprefix("Title: ").strip()
        return ""

    @staticmethod
    def _extract_published_at(markdown: str) -> str | None:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Published Time: "):
                return parse_us_date(cleaned.removeprefix("Published Time: ").strip())
        return None

    @staticmethod
    def _extract_content(body: str, title: str) -> str:
        lines = [clean_text(line) for line in body.splitlines()]
        normalized_title = normalize_compare_text(title)
        start_index = 0

        for index, line in enumerate(lines):
            if line.startswith("From: "):
                start_index = index + 1
                break

        if start_index == 0:
            for index, line in enumerate(lines):
                if normalize_compare_text(line) == normalized_title:
                    start_index = index + 1
                    break

        content_lines: list[str] = []
        stop_headings = {
            "Associated links",
            "Contacts",
            "Page details",
            "Related products",
            "Related links",
            "Features",
            "About this site",
            "On this page",
            "Services and information",
        }
        type_headings = {
            "News release",
            "Statement",
            "Readout",
            "Media advisory",
            "Speech",
            "Backgrounder",
        }

        for line in lines[start_index:]:
            if not line or normalize_compare_text(line) == normalized_title:
                continue
            if line == "Report a problem on this page":
                break
            if line.startswith("## "):
                heading = clean_text(line.removeprefix("## ").strip())
                if heading in stop_headings:
                    break
                if heading in type_headings:
                    continue
            if line.startswith("From: "):
                continue
            if line.startswith("*   ["):
                continue
            if line.endswith("- Canada.ca"):
                continue
            content_lines.append(line.removeprefix("# ").strip() if line.startswith("# ") else line)

        return clean_text("\n".join(content_lines))

    @staticmethod
    def _source_kind(body: str, title: str) -> str:
        merged = normalize_compare_text(f"{title}\n{body[:240]}")
        if "backgrounder" in merged:
            return "ca_gac_backgrounder"
        if "media advisory" in merged:
            return "ca_gac_media_advisory"
        if "speech" in merged or "remarks" in merged:
            return "ca_gac_speech"
        if "statement" in merged:
            return "ca_gac_statement"
        if "readout" in merged:
            return "ca_gac_readout"
        return "ca_gac_news_release"


class MexicoSrePressArchiveSource:
    country_code = "MX"
    history_start_date = "2022-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 180
    history_max_pages = 150
    archive_url = "https://www.gob.mx/sre/es/archivo/prensa?idiom=en"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 20, 180))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 90) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str]] = []
        seen_urls: set[str] = set()

        for page_number in range(1, max_pages + 1):
            page_markdown = request_markdown_via_jina(self._page_url(page_number))
            page_candidates = self._extract_listing_candidates(page_markdown)
            if not page_candidates:
                break

            oldest_on_page: str | None = None
            for url, title, published_at in page_candidates:
                if oldest_on_page is None or published_at < oldest_on_page:
                    oldest_on_page = published_at
                if not (start_date <= published_at <= end_date):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                candidates.append((url, title, published_at))

            if oldest_on_page is not None and oldest_on_page < start_date:
                break

        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(self._fetch_detail_record, url, title, published_at): (
                    url,
                    title,
                    published_at,
                )
                for url, title, published_at in candidates
            }
            for future in as_completed(futures):
                url, title, published_at = futures[future]
                try:
                    records.append(future.result())
                except Exception as exc:
                    print(f"MX: skipping {url} after fetch/parse error: {exc}")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return self.archive_url
        return f"{self.archive_url}&page={page_number}"

    def _extract_listing_candidates(self, markdown: str) -> list[tuple[str, str, str]]:
        lines = [clean_text(line) for line in extract_jina_markdown_body(markdown).splitlines()]
        candidates: list[tuple[str, str, str]] = []
        current_date: str | None = None
        pending_title: str | None = None

        for line in lines:
            if not line:
                continue
            date_match = MX_LISTING_DATE_RE.match(line)
            if date_match:
                current_date = parse_us_date(date_match.group("date"))
                pending_title = None
                continue

            if line.startswith("## ") and current_date:
                pending_title = clean_text(line.removeprefix("## ").strip())
                continue

            if line.startswith("[continue reading](") and current_date and pending_title:
                links = markdown_links(line)
                if not links:
                    continue
                _, url = links[0]
                candidates.append((normalize_generic_url(url), pending_title, current_date))
                pending_title = None

        return candidates

    def _fetch_detail_record(self, url: str, fallback_title: str, fallback_published_at: str) -> ScrapedRecord:
        markdown = request_markdown_via_jina(url)
        body = extract_jina_markdown_body(markdown)
        title = self._extract_title(markdown) or fallback_title
        published_at = self._extract_published_at(body) or fallback_published_at
        content = self._extract_content(body, title)
        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=self._source_kind(title, content),
            language="es",
            speaker=self._speaker(title, content),
        )

    @staticmethod
    def _extract_title(markdown: str) -> str:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Title: "):
                return cleaned.removeprefix("Title: ").strip()
        return ""

    @staticmethod
    def _extract_published_at(body: str) -> str | None:
        for raw_line in body.splitlines():
            line = clean_text(raw_line)
            match = MX_DETAIL_DATE_RE.search(line)
            if match:
                return parse_us_date(match.group("date"))
        return None

    @staticmethod
    def _extract_content(body: str, title: str) -> str:
        lines = [clean_text(line) for line in body.splitlines()]
        normalized_title = normalize_compare_text(title)
        title_hits = 0
        start_index = 0

        for index, line in enumerate(lines):
            if not line.startswith("# "):
                continue
            heading = clean_text(line.removeprefix("# ").split(" | ", 1)[0])
            if normalize_compare_text(heading) != normalized_title:
                continue
            title_hits += 1
            if title_hits >= 2:
                start_index = index + 1
                break

        if start_index == 0:
            for index, line in enumerate(lines):
                if normalize_compare_text(line) == normalized_title:
                    start_index = index + 1
                    break

        content_lines: list[str] = []
        for line in lines[start_index:]:
            if not line or normalize_compare_text(line) == normalized_title:
                continue
            if line == "* * *" or line.startswith("[Imprime la página completa]") or line.startswith("#### Links"):
                break
            if line.startswith("La legalidad, veracidad"):
                break
            if line.startswith("Secretaría de Relaciones Exteriores | "):
                continue
            if line.startswith("![Image") or line == "Aa+Aa-":
                continue
            content_lines.append(line.removeprefix("## ").strip() if line.startswith("## ") else line)

        content = clean_text("\n".join(content_lines))
        if not content:
            raise ValueError(f"Missing Mexico SRE content for {title}")
        return content

    @staticmethod
    def _source_kind(title: str, content: str) -> str:
        lowered = normalize_compare_text(f"{title}\n{content[:200]}")
        if "comunicado conjunto" in lowered or "joint statement" in lowered:
            return "mx_sre_joint_statement"
        return "mx_sre_press_release"

    @staticmethod
    def _speaker(title: str, content: str) -> str:
        merged = clean_text(f"{title}\n{content[:240]}")
        if "Juan Ramón de la Fuente" in merged or "De la Fuente" in merged:
            return "Juan Ramon de la Fuente"
        return "Secretaria de Relaciones Exteriores"


class SpainMfaComunicadosSource:
    country_code = "ES"
    history_start_date = "2022-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 240
    history_max_pages = 150
    archive_url = "https://www.exteriores.gob.es/en/Comunicacion/Comunicados/Paginas/index.aspx"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 20, 180))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 90) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str]] = []
        seen_urls: set[str] = set()

        for page_number in range(1, max_pages + 1):
            page_markdown = request_markdown_via_jina(self._page_url(page_number))
            page_candidates, has_next = self._extract_listing_candidates(page_markdown)
            if not page_candidates:
                break

            oldest_on_page: str | None = None
            for url, title, published_at in page_candidates:
                if oldest_on_page is None or published_at < oldest_on_page:
                    oldest_on_page = published_at
                if not (start_date <= published_at <= end_date):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                candidates.append((url, title, published_at))

            if not has_next or (oldest_on_page is not None and oldest_on_page < start_date):
                break

        records: list[ScrapedRecord] = []
        for url, title, published_at in candidates:
            try:
                records.append(self._fetch_detail_record(url, title, published_at))
            except Exception as exc:
                print(f"ES: skipping {url} after fetch/parse error: {exc}")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return self.archive_url
        return f"{self.archive_url}?p={page_number}"

    def _extract_listing_candidates(self, markdown: str) -> tuple[list[tuple[str, str, str]], bool]:
        candidates: list[tuple[str, str, str]] = []
        for raw_line in extract_jina_markdown_body(markdown).splitlines():
            line = clean_text(raw_line)
            if not line:
                continue
            match = ES_LISTING_ENTRY_RE.match(line)
            if not match:
                continue
            candidates.append(
                (
                    normalize_generic_url(match.group("url")),
                    clean_text(match.group("title")),
                    parse_en_short_date(match.group("date")),
                )
            )
        return candidates, "Go to the next page" in markdown

    def _fetch_detail_record(self, url: str, fallback_title: str, fallback_published_at: str) -> ScrapedRecord:
        markdown = request_markdown_via_jina(url)
        title = self._extract_title(markdown) or fallback_title
        body = extract_jina_markdown_body(markdown)
        content = self._extract_content(body, title)
        return ScrapedRecord(
            country_code=self.country_code,
            published_at=fallback_published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=self._source_kind(title),
            language="en",
            speaker=self._speaker(title),
        )

    @staticmethod
    def _extract_title(markdown: str) -> str:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Title: "):
                return cleaned.removeprefix("Title: ").strip()
        return ""

    @staticmethod
    def _extract_content(body: str, title: str) -> str:
        lines = [clean_text(line) for line in body.splitlines()]
        normalized_title = normalize_compare_text(title)
        title_hits = 0
        start_index = 0

        for index, line in enumerate(lines):
            if not line.startswith("# "):
                continue
            if normalize_compare_text(line.removeprefix("# ").strip()) != normalized_title:
                continue
            title_hits += 1
            if title_hits >= 2:
                start_index = index + 1
                break

        if start_index == 0:
            raise ValueError(f"Could not locate Spain MFA body for {title}")

        content_lines: list[str] = []
        for line in lines[start_index:]:
            if not line or line == "Today" or normalize_compare_text(line) == normalized_title:
                continue
            if line == "_-NON OFFICIAL TRANSLATION-_" or line.startswith("## More information") or line == "Banners":
                break
            content_lines.append(line.removeprefix("# ").strip() if line.startswith("# ") else line)

        content = clean_text("\n".join(content_lines))
        if not content:
            raise ValueError(f"Missing Spain MFA content for {title}")
        return content

    @staticmethod
    def _source_kind(title: str) -> str:
        lowered = normalize_compare_text(title)
        if "joint statement" in lowered or "statement" in lowered or "communiqué" in lowered:
            return "es_mfa_statement"
        return "es_mfa_comunicado"

    @staticmethod
    def _speaker(title: str) -> str:
        cleaned = clean_text(title)
        if cleaned.startswith("Spanish Government"):
            return "Spanish Government"
        return "Spanish Ministry of Foreign Affairs"


class BrazilItamaratyPressReleaseSource:
    country_code = "BR"
    history_start_date = "2022-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 150
    history_max_pages = 120
    archive_url = "https://www.gov.br/mre/en/en/contact-us/press-area/press-releases/press-releases"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 30, 180))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 30) -> list[ScrapedRecord]:
        candidates: list[tuple[str, str, str, str]] = []
        seen_urls: set[str] = set()

        for page_index in range(max_pages):
            page_markdown = request_markdown_via_jina(self._page_url(page_index))
            page_candidates, has_next = self._extract_listing_candidates(page_markdown)
            if not page_candidates:
                break

            oldest_on_page: str | None = None
            for url, title, published_at, excerpt in page_candidates:
                if oldest_on_page is None or published_at < oldest_on_page:
                    oldest_on_page = published_at
                if not (start_date <= published_at <= end_date):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                candidates.append((url, title, published_at, excerpt))

            if not has_next or (oldest_on_page is not None and oldest_on_page < start_date):
                break

        records: list[ScrapedRecord] = []
        for url, title, published_at, excerpt in candidates:
            try:
                records.append(self._fetch_detail_record(url, title, published_at, excerpt))
            except Exception as exc:
                print(f"BR: skipping {url} after fetch/parse error: {exc}")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _page_url(self, page_index: int) -> str:
        if page_index <= 0:
            return self.archive_url
        return f"{self.archive_url}?b_start:int={page_index * 30}"

    def _extract_listing_candidates(self, markdown: str) -> tuple[list[tuple[str, str, str, str]], bool]:
        lines = [clean_text(line) for line in extract_jina_markdown_body(markdown).splitlines()]
        candidates: list[tuple[str, str, str, str]] = []
        pending_title = ""
        pending_url = ""
        pending_excerpt: list[str] = []

        for line in lines:
            if not line:
                continue

            title_match = BR_LISTING_TITLE_RE.match(line)
            if title_match:
                pending_title = clean_text(title_match.group("title"))
                pending_url = normalize_generic_url(title_match.group("url"))
                pending_excerpt = []
                continue

            if not pending_title:
                continue

            date_match = BR_LISTING_DATE_RE.match(line)
            if date_match:
                candidates.append(
                    (
                        pending_url,
                        pending_title,
                        parse_us_date(date_match.group("date")),
                        clean_text(" ".join(pending_excerpt)),
                    )
                )
                pending_title = ""
                pending_url = ""
                pending_excerpt = []
                continue

            if line.startswith("PRESS RELEASE") or line.startswith("# ") or line == "Info":
                continue
            if line.startswith("Published in ") or line.startswith("published "):
                continue
            pending_excerpt.append(line)

        return candidates, "Next »" in markdown

    def _fetch_detail_record(self, url: str, fallback_title: str, fallback_published_at: str, excerpt: str) -> ScrapedRecord:
        title = fallback_title
        published_at = fallback_published_at
        content = ""
        fetch_url = self._detail_fetch_url(url)

        try:
            markdown = request_markdown_via_jina(fetch_url)
            title = self._extract_title(markdown) or fallback_title
            published_at = self._extract_published_at(markdown) or fallback_published_at
            content = self._extract_content(extract_jina_markdown_body(markdown), title)
        except Exception:
            content = ""

        if not content:
            content = excerpt or title

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=self._source_kind(title, content),
            language="en",
            speaker="Brazilian Ministry of Foreign Affairs",
        )

    @staticmethod
    def _detail_fetch_url(url: str) -> str:
        return url.replace("/mre/en/contact-us/", "/mre/en/en/contact-us/")

    @staticmethod
    def _extract_title(markdown: str) -> str:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Title: "):
                return cleaned.removeprefix("Title: ").strip()
        return ""

    @staticmethod
    def _extract_published_at(markdown: str) -> str | None:
        for line in markdown.splitlines():
            cleaned = clean_text(line)
            if cleaned.startswith("Published Time: "):
                return parse_us_date(cleaned.removeprefix("Published Time: ").strip())
            if cleaned.startswith("Published in "):
                return parse_us_date(cleaned.removeprefix("Published in ").split(" Updated in ", 1)[0].strip())
        return None

    @staticmethod
    def _extract_content(body: str, title: str) -> str:
        if "Advanced cookie settings" in body and "Strictly necessary cookies" in body:
            return ""

        lines = [clean_text(line) for line in body.splitlines()]
        normalized_title = normalize_compare_text(title)
        start_index = 0

        for index, line in enumerate(lines):
            if line.startswith("Published in "):
                start_index = index + 1
                break

        content_lines: list[str] = []
        for line in lines[start_index:]:
            if not line or normalize_compare_text(line) == normalized_title:
                continue
            if line in {"Category", "Editor", "Location", "Subjects"}:
                break
            if line.startswith("Cookies") or line.startswith("Due date"):
                break
            if line.startswith("Published in "):
                continue
            if line.startswith("# ") or line.startswith("## "):
                continue
            content_lines.append(line)

        return clean_text("\n".join(content_lines))

    @staticmethod
    def _source_kind(title: str, content: str) -> str:
        lowered = normalize_compare_text(f"{title}\n{content[:200]}")
        if "joint statement" in lowered or "joint communiqu" in lowered or "joint press release" in lowered:
            return "br_mre_joint_statement"
        if "statement" in lowered:
            return "br_mre_statement"
        return "br_mre_press_release"


class IndiaMeaOfficialSource:
    country_code = "IN"
    history_start_date = "2025-01-01"
    bootstrap_history_start_date = "2025-10-01"
    history_scan_limit = 2200
    history_backfill_chunk_days = 120
    history_probe_step = 100
    history_probe_buffer = 40
    history_fetch_workers = 3
    history_batch_size = 6
    history_retry_delay_seconds = 1.0
    recent_fetch_workers = 3
    resume_missing_history = True
    recent_listing_start_date = "2026-02-18"
    recent_listing_url = "https://www.mea.gov.in/whats-new.htm"
    recent_sections = ("Press Releases", "Media Briefings", "Lok Sabha", "Rajya Sabha")
    recent_index_urls = (
        "https://www.mea.gov.in/whats-new.htm",
        "https://www.mea.gov.in/press-releases.htm?51/Press_Releases",
        "https://www.mea.gov.in/media-briefings.htm?49/Media_Briefings",
        "https://www.mea.gov.in/lok-sabha.htm?61/Lok_Sabha",
        "https://www.mea.gov.in/rajya-sabha.htm?62/Rajya_Sabha",
    )
    detail_url_template = "https://www.mea.gov.in/press-releases.htm?dtl/{dtl_id}/"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = max(end_date - timedelta(days=max(max_pages * 10, 20)), iso_to_date(self.recent_listing_start_date))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 90) -> list[ScrapedRecord]:
        if start_date >= self.recent_listing_start_date:
            return self._fetch_recent_listing_between(start_date, end_date)

        return self._fetch_archive_between(start_date, end_date, max_pages=max_pages)

    def _fetch_archive_between(self, start_date: str, end_date: str, max_pages: int = 90) -> list[ScrapedRecord]:
        latest_id = self._latest_detail_id()
        # The ASP.NET archive does not expose stable pagination controls in this environment,
        # so we walk recent detail ids backwards and stop once we are safely before the window.
        scan_limit = max(240, min(max_pages * 8, 720))
        if start_date < self.recent_listing_start_date:
            scan_limit = max(scan_limit, self.history_scan_limit)
        lower_bound = max(1, latest_id - scan_limit)
        probed_records: dict[int, ScrapedRecord | None] = {}
        if start_date < self.recent_listing_start_date:
            lower_bound = max(
                lower_bound,
                self._estimate_archive_lower_bound_id(start_date, latest_id, lower_bound, probed_records),
            )
        upper_bound = latest_id
        if end_date < self.recent_listing_start_date:
            upper_bound = min(
                upper_bound,
                self._estimate_archive_upper_bound_id(end_date, latest_id, lower_bound, probed_records),
            )

        records: list[ScrapedRecord] = []
        seen_urls: set[str] = set()
        stale_hits = 0

        dtl_ids = list(range(upper_bound, lower_bound - 1, -1))
        history_fetch_workers = max(1, int(getattr(self, "history_fetch_workers", 3) or 3))
        history_batch_size = max(1, int(getattr(self, "history_batch_size", 6) or 6))
        history_retry_delay_seconds = float(
            getattr(self, "history_retry_delay_seconds", 1.0) or 1.0
        )
        with ThreadPoolExecutor(max_workers=history_fetch_workers) as executor:
            for batch_start in range(0, len(dtl_ids), history_batch_size):
                batch_ids = dtl_ids[batch_start : batch_start + history_batch_size]
                batch_results: dict[int, ScrapedRecord] = {}
                failed_ids: list[int] = []
                futures = {
                    executor.submit(self._fetch_detail_record, dtl_id): dtl_id
                    for dtl_id in batch_ids
                    if dtl_id not in probed_records
                }

                for dtl_id, record in probed_records.items():
                    if dtl_id in batch_ids and record is not None:
                        batch_results[dtl_id] = record

                for future in as_completed(futures):
                    dtl_id = futures[future]
                    try:
                        batch_results[dtl_id] = future.result()
                    except Exception:
                        failed_ids.append(dtl_id)

                for dtl_id in failed_ids:
                    try:
                        time.sleep(history_retry_delay_seconds)
                        batch_results[dtl_id] = self._fetch_detail_record(dtl_id)
                    except Exception:
                        continue

                for dtl_id in batch_ids:
                    record = batch_results.get(dtl_id)
                    if record is None:
                        continue
                    if record.url in seen_urls:
                        continue
                    seen_urls.add(record.url)

                    if record.published_at < start_date:
                        stale_hits += 1
                        if stale_hits >= 30:
                            break
                        continue

                    stale_hits = 0
                    if record.published_at > end_date:
                        continue
                    records.append(record)

                if stale_hits >= 30:
                    break

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _estimate_archive_lower_bound_id(
        self,
        start_date: str,
        latest_id: int,
        hard_lower_bound: int,
        probed_records: dict[int, ScrapedRecord | None],
    ) -> int:
        probe_step = int(getattr(self, "history_probe_step", 100) or 100)
        probe_buffer = int(getattr(self, "history_probe_buffer", 40) or 40)
        previous_probe: tuple[int, str] | None = None

        for dtl_id in range(latest_id, hard_lower_bound - 1, -probe_step):
            record = self._probe_detail_record(dtl_id, probed_records)
            if record is None:
                continue
            if record.published_at < start_date:
                if previous_probe is not None:
                    return max(
                        hard_lower_bound,
                        self._estimate_boundary_id(
                            start_date,
                            previous_probe,
                            (dtl_id, record.published_at),
                            direction="lower",
                        ),
                    )
                return max(hard_lower_bound, dtl_id - probe_buffer)
            previous_probe = (dtl_id, record.published_at)

        if previous_probe is None:
            return hard_lower_bound
        return max(hard_lower_bound, previous_probe[0] - probe_buffer)

    def _estimate_archive_upper_bound_id(
        self,
        end_date: str,
        latest_id: int,
        hard_lower_bound: int,
        probed_records: dict[int, ScrapedRecord | None],
    ) -> int:
        probe_step = int(getattr(self, "history_probe_step", 100) or 100)
        probe_buffer = int(getattr(self, "history_probe_buffer", 40) or 40)
        previous_probe: tuple[int, str] | None = None

        for dtl_id in range(latest_id, hard_lower_bound - 1, -probe_step):
            record = self._probe_detail_record(dtl_id, probed_records)
            if record is None:
                continue
            if record.published_at <= end_date:
                if previous_probe is not None:
                    return min(
                        latest_id,
                        self._estimate_boundary_id(
                            end_date,
                            previous_probe,
                            (dtl_id, record.published_at),
                            direction="upper",
                        ),
                    )
                return min(latest_id, dtl_id + probe_buffer)
            previous_probe = (dtl_id, record.published_at)

        return latest_id

    def _estimate_boundary_id(
        self,
        target_date: str,
        newer_probe: tuple[int, str],
        older_probe: tuple[int, str],
        direction: str,
    ) -> int:
        probe_buffer = int(getattr(self, "history_probe_buffer", 40) or 40)
        newer_id, newer_date = newer_probe
        older_id, older_date = older_probe
        newer_ordinal = iso_to_date(newer_date).toordinal()
        older_ordinal = iso_to_date(older_date).toordinal()
        target_ordinal = iso_to_date(target_date).toordinal()

        if newer_ordinal <= older_ordinal:
            estimate = older_id if direction == "lower" else newer_id
        else:
            ratio = (newer_ordinal - target_ordinal) / (newer_ordinal - older_ordinal)
            ratio = min(max(ratio, 0.0), 1.0)
            estimate = round(newer_id - ((newer_id - older_id) * ratio))

        if direction == "lower":
            return max(1, estimate - probe_buffer)
        return estimate + probe_buffer

    def _probe_detail_record(
        self,
        dtl_id: int,
        probed_records: dict[int, ScrapedRecord | None],
    ) -> ScrapedRecord | None:
        if dtl_id in probed_records:
            return probed_records[dtl_id]
        try:
            record = self._fetch_detail_record(dtl_id)
        except Exception:
            probed_records[dtl_id] = None
            return None
        probed_records[dtl_id] = record
        return record

    def _fetch_recent_listing_between(self, start_date: str, end_date: str) -> list[ScrapedRecord]:
        markdown = request_markdown_via_jina(self.recent_listing_url)
        items = self._parse_recent_listing(markdown)
        if not items:
            return []

        candidates = [
            item for item in items if start_date <= item["published_at"] <= end_date  # type: ignore[index]
        ]
        if not candidates:
            return []

        records: list[ScrapedRecord] = []
        recent_fetch_workers = max(1, int(getattr(self, "recent_fetch_workers", 3) or 3))
        with ThreadPoolExecutor(max_workers=recent_fetch_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_recent_listing_record,
                    str(item["url"]),
                    str(item["title"]),
                    str(item["published_at"]),
                ): (
                    str(item["url"]),
                    str(item["title"]),
                    str(item["published_at"]),
                )
                for item in candidates
            }
            for future in as_completed(futures):
                url, title, published_at = futures[future]
                try:
                    records.append(future.result())
                except Exception as exc:
                    try:
                        time.sleep(4)
                        records.append(self._fetch_recent_listing_record(url, title, published_at))
                    except Exception as retry_exc:
                        print(f"IN: skipping {url} from what's-new after fetch/parse error: {retry_exc}")

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _parse_recent_listing(self, markdown: str) -> list[dict[str, str]]:
        lines = markdown.splitlines()
        date_pattern = re.compile(r"^[A-Z][a-z]+\s+\d{1,2},\s+\d{4}$")
        item_pattern = re.compile(
            r"^\*\s+(Press Releases|Media Briefings|Lok Sabha|Rajya Sabha)\[(.+?)\]\((https://www\.mea\.gov\.in/[^\s)]+)"
        )

        items: list[dict[str, str]] = []
        seen_urls: set[str] = set()
        for index, raw_line in enumerate(lines):
            line = clean_text(raw_line)
            match = item_pattern.match(line)
            if not match:
                continue

            section, title, url = match.groups()
            if section not in self.recent_sections:
                continue

            date_line = ""
            pointer = index + 1
            while pointer < len(lines):
                candidate = clean_text(lines[pointer])
                if candidate:
                    date_line = candidate
                    break
                pointer += 1

            if not date_pattern.match(date_line):
                continue

            normalized_url = normalize_generic_url(url)
            if normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)

            items.append(
                {
                    "section": section,
                    "title": title,
                    "url": normalized_url,
                    "published_at": parse_us_date(date_line),
                }
            )

        return items

    def _fetch_recent_listing_record(
        self,
        url: str,
        fallback_title: str,
        fallback_published_at: str,
    ) -> ScrapedRecord:
        markdown = request_markdown_via_jina(url)
        if self._is_unavailable_markdown(markdown):
            raise ValueError(f"India MEA listing detail page is unavailable in English: {url}")

        title = self._extract_title(markdown) or clean_text(fallback_title)
        try:
            published_at = parse_india_page_updated(markdown)
        except ValueError:
            published_at = fallback_published_at
        content = self._extract_content(markdown, title)
        source_kind = self._source_kind(title, content)

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=source_kind,
            language="en",
            speaker=self._speaker(title, content),
        )

    def _latest_detail_id(self) -> int:
        recent_listing_markdown = request_markdown_via_jina(self.recent_listing_url)
        recent_ids = [int(match) for match in re.findall(r"\?dtl/(\d+)\b", recent_listing_markdown)]
        if recent_ids:
            return max(recent_ids)

        latest_id = 0
        for index_url in self.recent_index_urls:
            markdown = request_markdown_via_jina(index_url)
            ids = [int(match) for match in re.findall(r"\?dtl/(\d+)\b", markdown)]
            if ids:
                latest_id = max(latest_id, max(ids))
        if latest_id <= 0:
            raise ValueError("Could not determine the latest India MEA detail id.")
        return latest_id

    def _fetch_detail_record(self, dtl_id: int) -> ScrapedRecord:
        url = self.detail_url_template.format(dtl_id=dtl_id)
        markdown = request_markdown_via_jina(url)
        if markdown.startswith("Title: Sorry for the inconvenience."):
            raise ValueError(f"Missing India MEA detail page for {dtl_id}")
        if self._is_unavailable_markdown(markdown):
            raise ValueError(f"India MEA detail page {dtl_id} is unavailable in English.")

        title = self._extract_title(markdown)
        published_at = parse_india_page_updated(markdown)
        content = self._extract_content(markdown, title)
        source_kind = self._source_kind(title, content)

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=source_kind,
            language="en",
            speaker=self._speaker(title, content),
        )

    @staticmethod
    def _extract_title(markdown: str) -> str:
        match = re.search(r"^Title:\s*(.+)$", markdown, re.M)
        if not match:
            raise ValueError("Missing India MEA title.")
        return clean_text(match.group(1))

    @staticmethod
    def _extract_content(markdown: str, title: str) -> str:
        lines = [clean_text(line) for line in markdown.splitlines()]
        start = 0
        for index, line in enumerate(lines):
            if line == "Markdown Content:":
                start = index + 1
                break

        body_lines = lines[start:]
        heading_candidates = {f"# {title}", f"## {title}"}
        heading_indexes = [idx for idx, line in enumerate(body_lines) if line in heading_candidates]
        if heading_indexes:
            body_lines = body_lines[heading_indexes[-1] + 1 :]

        content_lines: list[str] = []
        for line in body_lines:
            if not line:
                continue
            if line.startswith("[Write a Comment]") or line in {"Comments", "Post A Comment"}:
                break
            if line.startswith("[Click here for ") and " version" in line:
                break
            if line.startswith("[]("):
                continue
            if line.startswith("[![Image") or line.startswith("!["):
                continue
            if line.startswith("*   Name *(required)") or line.startswith("*   Write Your Comment"):
                break
            if line.startswith("Visitors:"):
                break
            if line.startswith("Ministry of External Affairs") and not content_lines:
                continue
            if line in heading_candidates:
                continue
            content_lines.append(line)

        content = clean_text("\n".join(content_lines))
        if not content:
            raise ValueError(f"Missing parsed India MEA content for {title}")
        return content

    @staticmethod
    def _source_kind(title: str, content: str) -> str:
        lowered_title = clean_text(title).lower()
        lowered_content = clean_text(content[:800]).lower()
        if lowered_title.startswith("question no") or "lok sabha" in lowered_content or "rajya sabha" in lowered_content:
            return "in_mea_parliament_answer"
        if "briefing" in lowered_title or lowered_title.startswith("transcript"):
            return "in_mea_media_briefing"
        if "official spokesperson" in lowered_title:
            return "in_mea_statement"
        if "interview" in lowered_title:
            return "in_mea_interview"
        if lowered_title.startswith("speech") or lowered_title.startswith("statement"):
            return "in_mea_statement"
        return "in_mea_press_release"

    @staticmethod
    def _speaker(title: str, content: str) -> str:
        lowered_title = clean_text(title).lower()
        if "official spokesperson" in lowered_title:
            return "Official Spokesperson"
        first_line = clean_text(content.splitlines()[0]) if content else ""
        match = re.match(r"(?P<speaker>(?:Shri|Dr\.?|Mr\.?|Ms\.?|Mrs\.?|Ambassador)[^:]{1,120}):", first_line)
        if match:
            return clean_text(match.group("speaker"))
        if "minister of external affairs" in lowered_title:
            return "Ministry of External Affairs"
        return ""

    @staticmethod
    def _is_unavailable_markdown(markdown: str) -> bool:
        lowered = clean_text(markdown).lower()
        return "the page you are refering is not available in selected language" in lowered


class FranceMfaSpokespersonSource:
    country_code = "FR"
    history_start_date = "2022-01-01"
    resume_missing_history = True
    sitemap_url = "https://www.diplomatie.gouv.fr/sitemap.xml"
    relevant_patterns = (
        re.compile(r"/fr/salle-de-presse/point-de-presse-live-du-porte-parole-du-meae/article/"),
        re.compile(r"/fr/les-ministres/[^/]+/presse-et-medias/article/"),
        re.compile(r"/fr/les-ministres/[^/]+/discours/article/"),
        re.compile(r"/fr/les-ministres/[^/]+/interventions-a-l-assemblee-nationale-et-au-senat/article/"),
        re.compile(r"/fr/dossiers-pays/.+/evenements(?:/.+)?/article/"),
        re.compile(r"/fr/dossiers-pays/.+/actualites-et-evenements(?:/.+)?/article/"),
        re.compile(r"/fr/politique-etrangere-de-la-france/.+/actualites[^/]*(?:/.+)?/article/"),
        re.compile(r"/fr/politique-etrangere-de-la-france/.+/evenements[^/]*(?:/.+)?/article/"),
    )

    def __init__(self, session: requests.Session) -> None:
        self.session = session
        self._candidate_urls: list[str] | None = None

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 35, 120))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 20) -> list[ScrapedRecord]:
        records: list[ScrapedRecord] = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._parse_article_threadsafe, url): url for url in self._load_candidate_urls()}
            for future in as_completed(futures):
                try:
                    record = future.result()
                except Exception:
                    continue
                if start_date <= record.published_at <= end_date:
                    records.append(record)

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _load_candidate_urls(self) -> list[str]:
        if self._candidate_urls is not None:
            return self._candidate_urls

        sitemap_xml = request_html(self.session, self.sitemap_url)
        urls = re.findall(r"<loc>(.*?)</loc>", sitemap_xml)
        filtered: list[str] = []
        for url in urls:
            normalized = normalize_generic_url(clean_text(url))
            if not normalized.startswith("https://www.diplomatie.gouv.fr/fr/"):
                continue
            if any(pattern.search(normalized) for pattern in self.relevant_patterns):
                filtered.append(normalized)

        self._candidate_urls = list(dict.fromkeys(filtered))
        return self._candidate_urls

    def _parse_article_threadsafe(self, url: str) -> ScrapedRecord:
        with requests.Session() as session:
            html_text = request_html(session, url)
        return self._make_record_from_html(html_text, url)

    def _make_record_from_html(self, html_text: str, url: str) -> ScrapedRecord:
        soup = BeautifulSoup(html_text, "html.parser")
        article = soup.select_one("article") or soup.select_one("#main") or soup.body
        if article is None:
            raise ValueError(f"Missing article body for {url}")

        title_node = article.select_one("h1")
        title = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        if not title:
            raise ValueError(f"Missing French MFA title for {url}")

        for selector in [
            "nav",
            "aside",
            "button",
            "script",
            "style",
            ".aria",
            ".social",
            ".partager",
            ".bloc_rss",
            ".toolbar",
        ]:
            for node in article.select(selector):
                node.decompose()

        content_root = article.select_one(".texte") or article
        paragraphs = [clean_text(node.get_text(" ", strip=True)) for node in content_root.select("p, li, h2, h3")]
        content = clean_text("\n".join(text for text in paragraphs if text))
        if not content:
            content = clean_text(content_root.get_text("\n"))
        if not content:
            raise ValueError(f"Missing parsed content for {url}")

        published_at = self._extract_published_at(url, title, html_text, content)

        return ScrapedRecord(
            country_code=self.country_code,
            published_at=published_at,
            url=normalize_generic_url(url),
            title=title,
            content=content,
            source_kind=self._source_kind(url),
            language="fr",
            speaker=self._speaker(url, title),
        )

    @staticmethod
    def _extract_published_at(url: str, title: str, html_text: str, content: str) -> str:
        for candidate in (title, url, html_text[:3000], content[:2000]):
            try:
                return parse_fr_date(candidate)
            except ValueError:
                continue
        raise ValueError(f"Could not determine French MFA publication date for {url}")

    @staticmethod
    def _source_kind(url: str) -> str:
        normalized = normalize_generic_url(url)
        if "/point-de-presse-live-du-porte-parole-du-meae/article/" in normalized:
            return "fr_meae_live_qa"
        if "/presse-et-medias/article/" in normalized:
            return "fr_meae_press_statement"
        if "/discours/article/" in normalized:
            return "fr_meae_speech"
        if "/interventions-a-l-assemblee-nationale-et-au-senat/article/" in normalized:
            return "fr_meae_parliament_intervention"
        if "/fr/politique-etrangere-de-la-france/" in normalized:
            return "fr_meae_policy_event"
        if "/dossiers-pays/" in normalized:
            return "fr_meae_country_event"
        return "fr_meae_statement"

    @staticmethod
    def _speaker(url: str, title: str) -> str:
        normalized = normalize_generic_url(url)
        lowered_title = clean_text(title).lower()
        if "/point-de-presse-live-du-porte-parole-du-meae/article/" in normalized or "point de presse" in lowered_title:
            return "MEAE spokesperson"
        if "jean-noel barrot" in lowered_title:
            return "Jean-Noel Barrot"
        if "nicolas forissier" in lowered_title:
            return "Nicolas Forissier"
        if "benjamin haddad" in lowered_title:
            return "Benjamin Haddad"
        return "France Diplomatie"


class RussiaMfaNewsSource:
    country_code = "RU"
    history_start_date = "2010-01-01"
    resume_missing_history = True
    history_backfill_chunk_days = 365
    sections = (
        {
            "list_url": "https://mid.ru/en/press_service/spokesman/official_statement/",
            "item_url_fragment": "/en/press_service/spokesman/official_statement/",
            "source_kind": "ru_mfa_statement",
            "speaker": "",
            "max_pages": 50,
        },
        {
            "list_url": "https://mid.ru/en/press_service/spokesman/briefings/",
            "item_url_fragment": "/en/press_service/spokesman/briefings/",
            "source_kind": "ru_mfa_briefing",
            "speaker": "MFA spokesperson",
            "max_pages": 30,
        },
        {
            "list_url": "https://mid.ru/en/foreign_policy/news/",
            "item_url_fragment": "/en/foreign_policy/news/",
            "source_kind": "ru_mfa_press_release",
            "speaker": "",
            "max_pages": 15,
            "section_start_date": "2026-01-01",
        },
    )

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 6) -> list[ScrapedRecord]:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(max_pages * 20, 120))
        return self.fetch_between(start_date.isoformat(), end_date.isoformat(), max_pages=max_pages)

    def fetch_between(self, start_date: str, end_date: str, max_pages: int = 20) -> list[ScrapedRecord]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - optional dependency in CI/local fetches
            raise RuntimeError("Playwright is required to fetch Russian MFA records.") from exc

        start_bound = iso_to_date(start_date)
        seen_urls = {
            normalize_generic_url(str(url))
            for url in getattr(self, "known_urls", set())
            if str(url).strip()
        }
        seen_titles = {
            (str(published_at), str(normalized_title))
            for published_at, normalized_title in getattr(self, "known_title_keys", set())
            if str(published_at).strip() and str(normalized_title).strip()
        }

        with sync_playwright() as playwright:
            headless = os.getenv("WDSI_PLAYWRIGHT_HEADLESS", "1").strip().lower() not in {"0", "false", "no"}
            browser = playwright.chromium.launch(
                headless=headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            records: list[ScrapedRecord] = []

            for section in self.sections:
                context = self._new_browser_context(browser)
                list_page = context.new_page()
                article_page = context.new_page()
                try:
                    section_candidates = self._collect_candidates(
                        list_page,
                        section,
                        start_date,
                        end_date,
                        start_bound,
                        max_pages,
                    )
                    for candidate in section_candidates:
                        url = candidate["url"]
                        title_key = (
                            str(candidate["published_at"]),
                            self._normalize_compare_text(str(candidate["title"])),
                        )
                        if url in seen_urls:
                            continue
                        if title_key in seen_titles:
                            continue
                        seen_urls.add(url)
                        seen_titles.add(title_key)
                        try:
                            body_text = self._fetch_article_text(
                                article_page,
                                url,
                                str(candidate["section_url"]),
                            )
                            content = self._extract_article_content(body_text, candidate["title"])
                            records.append(
                                ScrapedRecord(
                                    country_code=self.country_code,
                                    published_at=candidate["published_at"],
                                    url=normalize_generic_url(url),
                                    title=candidate["title"],
                                    content=content,
                                    source_kind=str(candidate["source_kind"]),
                                    language="en",
                                    speaker=self._speaker(candidate["title"], str(candidate["speaker"])),
                                )
                            )
                        except Exception:
                            continue
                finally:
                    article_page.close()
                    list_page.close()
                    context.close()
            browser.close()

        deduped = {record.url: record for record in records}
        return sorted(deduped.values(), key=lambda record: (record.published_at, record.url))

    def _collect_candidates(
        self,
        page: object,
        section: dict[str, str],
        start_date: str,
        end_date: str,
        start_bound: date,
        max_pages: int,
    ) -> list[dict[str, str]]:
        section_url = str(section["list_url"])
        collected: list[dict[str, str]] = []
        section_max_pages = min(max_pages, int(section.get("max_pages", max_pages)))
        section_start_date = str(section.get("section_start_date", "") or "")
        if section_start_date and end_date < section_start_date:
            return []
        effective_start_date = max(start_date, section_start_date) if section_start_date else start_date
        effective_start_bound = max(start_bound, iso_to_date(section_start_date)) if section_start_date else start_bound

        for page_number in range(1, section_max_pages + 1):
            page_url = section_url if page_number == 1 else f"{section_url}?PAGEN_1={page_number}"
            try:
                self._load_mid_list_page(page, page_url, referer=section_url if page_number > 1 else None)
            except RuntimeError:
                break

            page_candidates = self._extract_list_candidates(page, str(section["item_url_fragment"]))
            if not page_candidates:
                break

            oldest_on_page = min(iso_to_date(candidate["published_at"]) for candidate in page_candidates)
            for candidate in page_candidates:
                if not (effective_start_date <= candidate["published_at"] <= end_date):
                    continue
                candidate["section_url"] = section_url
                candidate["source_kind"] = str(section["source_kind"])
                candidate["speaker"] = str(section["speaker"])
                collected.append(candidate)

            if oldest_on_page < effective_start_bound:
                break

        return collected

    def _extract_list_candidates(self, page: object, item_url_fragment: str) -> list[dict[str, str]]:
        items = page.evaluate(
            f"""
() => Array.from(document.querySelectorAll('.announce__item')).map((item) => {{
  const link = item.querySelector('a[href*="{item_url_fragment}"]');
  if (!link) {{
    return null;
  }}
  return {{
    url: link.href,
    title: (link.innerText || '').trim(),
    context: (item.innerText || '').trim(),
  }};
}}).filter(Boolean)
"""
        )
        candidates: list[dict[str, str]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            url = normalize_generic_url(str(item.get("url", "")))
            title = clean_text(str(item.get("title", "")))
            context_text = clean_text(str(item.get("context", "")))
            if not title or item_url_fragment not in url or not re.search(r"/\d+/?$", url):
                continue
            match = RU_LIST_TIMESTAMP_RE.search(context_text)
            if not match:
                continue
            candidates.append(
                {
                    "url": url,
                    "title": title,
                    "published_at": parse_us_date(match.group("date")),
                }
            )
        return candidates

    def _fetch_article_text(self, page: object, article_url: str, referer: str) -> str:
        body_text = ""
        for _ in range(3):
            self._navigate_mid_page(page, article_url, referer=referer, warmup=False)
            body_text = self._read_mid_body_text(page)
            if not self._is_rejected_text(body_text) and len(body_text) > 500:
                return body_text
        raise RuntimeError(f"Russian MFA blocked article fetch for {article_url}")

    def _load_mid_list_page(self, page: object, page_url: str, referer: str | None = None) -> None:
        for attempt in range(4):
            self._navigate_mid_page(page, page_url, referer=referer, warmup=attempt == 0)
            try:
                if page.locator(".announce__item").count() > 0:
                    return
            except Exception:
                pass
        raise RuntimeError(f"Russian MFA list page did not load candidates for {page_url}")

    @staticmethod
    def _new_browser_context(browser: object) -> object:
        return browser.new_context(
            user_agent=BROWSER_HEADERS["User-Agent"],
            locale="en-US",
            extra_http_headers={
                "Accept": BROWSER_HEADERS["Accept"],
                "Accept-Language": BROWSER_HEADERS["Accept-Language"],
            },
        )

    def _navigate_mid_page(
        self,
        page: object,
        url: str,
        *,
        referer: str | None = None,
        warmup: bool = False,
    ) -> None:
        goto_kwargs: dict[str, object] = {"wait_until": "domcontentloaded", "timeout": 90_000}
        if referer:
            goto_kwargs["referer"] = referer
        page.goto(url, **goto_kwargs)
        page.wait_for_timeout(5_000 if warmup else 2_500)

    @staticmethod
    def _read_mid_body_text(page: object) -> str:
        last_error: Exception | None = None
        latest_text = ""
        for attempt in range(6):
            try:
                if attempt:
                    page.wait_for_timeout(1_500)
                text = clean_text(str(page.evaluate("() => document.body ? document.body.innerText : ''")))
                latest_text = text
                if text and "the requested url was rejected" not in text.lower():
                    return text
            except Exception as exc:  # pragma: no cover - browser timing dependent
                last_error = exc
                page.wait_for_timeout(1_000)
        if latest_text:
            return latest_text
        assert last_error is not None
        raise last_error

    @staticmethod
    def _extract_article_content(body_text: str, title: str) -> str:
        lines = [clean_text(line) for line in body_text.splitlines()]
        lines = [line for line in lines if line]

        normalized_title = RussiaMfaNewsSource._normalize_compare_text(title)
        started = False
        content_lines: list[str] = []
        footer_markers = ("The main foreign policy news", "Using website content", "Technical information")

        for line in lines:
            if not started:
                if RussiaMfaNewsSource._normalize_compare_text(line) == normalized_title:
                    started = True
                continue

            if any(line.startswith(marker) for marker in footer_markers) or line.startswith("© "):
                break
            if RU_ARTICLE_ID_RE.fullmatch(line):
                continue
            if RU_LIST_TIMESTAMP_RE.fullmatch(line):
                continue
            if re.fullmatch(rf"{MONTH_NAME_PATTERN}\s+\d{{4}}", line):
                continue
            if re.fullmatch(r"\d{1,2}", line):
                continue
            if line == "photo":
                continue
            content_lines.append(line)

        content = clean_text("\n".join(content_lines))
        if not content:
            raise ValueError(f"Missing parsed Russian MFA content for {title}")
        return content

    @staticmethod
    def _normalize_compare_text(value: str) -> str:
        return clean_text(value).replace("’", "'").replace("“", '"').replace("”", '"')

    @staticmethod
    def _is_rejected_text(value: str) -> bool:
        lowered = clean_text(value).lower()
        return not lowered or "the requested url was rejected" in lowered

    @staticmethod
    def _source_kind(title: str) -> str:
        lowered = clean_text(title).lower()
        if "briefing" in lowered:
            return "ru_mfa_briefing"
        if lowered.startswith("statement"):
            return "ru_mfa_statement"
        if lowered.startswith("comment"):
            return "ru_mfa_comment"
        return "ru_mfa_press_release"

    @staticmethod
    def _speaker(title: str, default_speaker: str = "") -> str:
        lowered = clean_text(title).lower()
        if "maria zakharova" in lowered:
            return "Maria Zakharova"
        if "sergey lavrov" in lowered:
            return "Sergey Lavrov"
        return default_speaker


class OpenAIWDSIScorer:
    pipeline_version = "paper_multistage_v1"

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("WDSI_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("WDSI_API_KEY or OPENAI_API_KEY is required for scoring.")

        self.base_url = base_url or os.getenv("WDSI_API_BASE_URL") or os.getenv("OPENAI_BASE_URL") or ""
        self.model = model or os.getenv("WDSI_MODEL") or os.getenv("WDSI_OPENAI_MODEL") or "gpt-5-mini"
        self.reasoning_effort = reasoning_effort or os.getenv("WDSI_REASONING_EFFORT") or "low"

        from openai import OpenAI

        client_kwargs = {"api_key": self.api_key}
        if self.base_url:
            client_kwargs["base_url"] = self.base_url
        self.client = OpenAI(**client_kwargs)

    def score_record(self, record: ScrapedRecord) -> dict[str, object]:
        units = self._build_units(record)
        relevance, stage_ids, stage_events = self._score_relevance(record, units)
        war_units = [unit for unit in units if relevance[unit.unit_id]["war_related"]]

        if not war_units:
            confidence = self._confidence_from_events(stage_events + [{"kind": "aggregate", "status": "accepted"}])
            return {
                "score": 0,
                "score_reasoning": "No war-related unit survived the paper-style screening stage.",
                "confidence": confidence,
                "war_related": False,
                "model": self.model,
                "response_id": self._join_response_ids(stage_ids),
                "scored_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "pipeline_version": self.pipeline_version,
            }

        categories, category_ids, category_events = self._score_categories(record, war_units)
        scores, score_ids, score_events = self._score_intensity(record, war_units, categories)
        aggregate_payload, aggregate_id = self._aggregate_units(record, war_units, categories, scores)

        response_ids = stage_ids + category_ids + score_ids + ([aggregate_id] if aggregate_id else [])
        confidence = self._confidence_from_events(
            stage_events + category_events + score_events + [{"kind": "aggregate", "status": "accepted"}]
        )

        return {
            "score": int(aggregate_payload["score"]),
            "score_reasoning": clean_text(str(aggregate_payload["reasoning"])),
            "confidence": confidence,
            "war_related": True,
            "model": self.model,
            "response_id": self._join_response_ids(response_ids),
            "scored_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "pipeline_version": self.pipeline_version,
        }

    def score_flat_records(
        self,
        records: list[ScrapedRecord],
        *,
        batch_size: int = 12,
    ) -> list[dict[str, object]]:
        scored: list[dict[str, object]] = []
        for start in range(0, len(records), batch_size):
            scored.extend(self._score_flat_batch_with_fallback(records[start : start + batch_size]))
        return scored

    def score_conference_records(
        self,
        records: list[ScrapedRecord],
        *,
        batch_size: int = 2,
    ) -> list[dict[str, object]]:
        scored: list[dict[str, object]] = []
        for start in range(0, len(records), batch_size):
            scored.extend(self._score_conference_batch_with_fallback(records[start : start + batch_size]))
        return scored

    def _score_flat_batch_with_fallback(self, records: list[ScrapedRecord]) -> list[dict[str, object]]:
        try:
            return self._score_flat_batch(records)
        except Exception:
            if len(records) <= 1:
                last_error: Exception | None = None
                for _ in range(2):
                    try:
                        return self._score_flat_batch(records)
                    except Exception as exc:
                        last_error = exc
                        time.sleep(2)
                assert last_error is not None
                raise last_error
            midpoint = max(1, len(records) // 2)
            return self._score_flat_batch_with_fallback(records[:midpoint]) + self._score_flat_batch_with_fallback(
                records[midpoint:]
            )

    def _score_conference_batch_with_fallback(self, records: list[ScrapedRecord]) -> list[dict[str, object]]:
        try:
            return self._score_conference_batch(records)
        except Exception:
            if len(records) <= 1:
                last_error: Exception | None = None
                for _ in range(2):
                    try:
                        return self._score_conference_batch(records)
                    except Exception as exc:
                        last_error = exc
                        time.sleep(2)
                assert last_error is not None
                raise last_error
            midpoint = max(1, len(records) // 2)
            return self._score_conference_batch_with_fallback(
                records[:midpoint]
            ) + self._score_conference_batch_with_fallback(records[midpoint:])

    def _score_flat_batch(self, records: list[ScrapedRecord]) -> list[dict[str, object]]:
        if not records:
            return []

        synthetic_record = ScrapedRecord(
            country_code=records[0].country_code,
            published_at=f"{records[0].published_at} to {records[-1].published_at}",
            url="",
            title=f"{len(records)} batched official statements",
            content="",
            source_kind=records[0].source_kind,
            language=records[0].language,
            speaker="",
        )
        units = [
            TextUnit(
                unit_id=f"u{index}",
                label=f"{record.published_at} | {record.title}",
                text=truncate_text(record.content, 2200),
            )
            for index, record in enumerate(records, start=1)
        ]

        relevance, stage_ids, stage_events = self._score_relevance(synthetic_record, units)
        war_units = [unit for unit in units if relevance[unit.unit_id]["war_related"]]
        categories: dict[str, dict[str, object]] = {}
        scores: dict[str, dict[str, object]] = {}
        category_ids: list[str] = []
        score_ids: list[str] = []
        category_events: list[dict[str, str]] = []
        score_events: list[dict[str, str]] = []

        if war_units:
            categories, category_ids, category_events = self._score_categories(synthetic_record, war_units)
            scores, score_ids, score_events = self._score_intensity(synthetic_record, war_units, categories)

        response_id = self._join_response_ids(stage_ids + category_ids + score_ids)
        scored_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        confidence = self._confidence_from_events(stage_events + category_events + score_events)

        results: list[dict[str, object]] = []
        for record, unit in zip(records, units, strict=False):
            if not relevance[unit.unit_id]["war_related"]:
                results.append(
                    {
                        "score": 0,
                        "score_reasoning": clean_text(
                            f"No war-related sentiment detected. {relevance[unit.unit_id].get('rationale', '')}"
                        ),
                        "confidence": confidence,
                        "war_related": False,
                        "model": self.model,
                        "response_id": response_id,
                        "scored_at": scored_at,
                        "pipeline_version": self.pipeline_version,
                    }
                )
                continue

            category = clean_text(str(categories[unit.unit_id]["category"]))
            score_value = int(scores[unit.unit_id]["score"])
            rationale = clean_text(str(scores[unit.unit_id].get("rationale", "")))
            results.append(
                {
                    "score": score_value,
                    "score_reasoning": clean_text(f"{category.capitalize()} tone. {rationale}"),
                    "confidence": confidence,
                    "war_related": True,
                    "model": self.model,
                    "response_id": response_id,
                    "scored_at": scored_at,
                    "pipeline_version": self.pipeline_version,
                }
            )

        return results

    def _score_conference_batch(self, records: list[ScrapedRecord]) -> list[dict[str, object]]:
        if not records:
            return []

        synthetic_record = ScrapedRecord(
            country_code=records[0].country_code,
            published_at=f"{records[0].published_at} to {records[-1].published_at}",
            url="",
            title=f"{len(records)} batched press conferences",
            content="",
            source_kind=records[0].source_kind,
            language=records[0].language,
            speaker="",
        )

        units_by_record: list[tuple[ScrapedRecord, list[TextUnit]]] = []
        batch_units: list[TextUnit] = []
        for record_index, record in enumerate(records, start=1):
            original_units = self._build_units(record)
            renamed_units: list[TextUnit] = []
            for unit in original_units:
                renamed = TextUnit(
                    unit_id=f"r{record_index}_{unit.unit_id}",
                    label=f"{record.published_at} | {record.title} | {unit.label}",
                    text=unit.text,
                )
                renamed_units.append(renamed)
                batch_units.append(renamed)
            units_by_record.append((record, renamed_units))

        if not batch_units:
            return [
                {
                    "score": 0,
                    "score_reasoning": "No usable Q&A units were extracted from the press conference.",
                    "confidence": 0.65,
                    "war_related": False,
                    "model": self.model,
                    "response_id": "",
                    "scored_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "pipeline_version": self.pipeline_version,
                }
                for _ in records
            ]

        relevance, stage_ids, stage_events = self._score_relevance(synthetic_record, batch_units)
        war_units = [unit for unit in batch_units if relevance[unit.unit_id]["war_related"]]
        categories: dict[str, dict[str, object]] = {}
        scores: dict[str, dict[str, object]] = {}
        category_ids: list[str] = []
        score_ids: list[str] = []
        category_events: list[dict[str, str]] = []
        score_events: list[dict[str, str]] = []

        if war_units:
            categories, category_ids, category_events = self._score_categories(synthetic_record, war_units)
            scores, score_ids, score_events = self._score_intensity(synthetic_record, war_units, categories)

        base_response_ids = stage_ids + category_ids + score_ids
        base_confidence = self._confidence_from_events(stage_events + category_events + score_events)
        scored_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        results: list[dict[str, object]] = []

        for record, record_units in units_by_record:
            own_war_units = [unit for unit in record_units if relevance[unit.unit_id]["war_related"]]
            if not own_war_units:
                results.append(
                    {
                        "score": 0,
                        "score_reasoning": "No war-related unit survived the paper-style screening stage.",
                        "confidence": base_confidence,
                        "war_related": False,
                        "model": self.model,
                        "response_id": self._join_response_ids(base_response_ids),
                        "scored_at": scored_at,
                        "pipeline_version": self.pipeline_version,
                    }
                )
                continue

            aggregate_id = ""
            if len(own_war_units) == 1:
                unit = own_war_units[0]
                aggregate_payload = {
                    "score": int(scores[unit.unit_id]["score"]),
                    "reasoning": clean_text(str(scores[unit.unit_id]["rationale"])),
                }
            else:
                aggregate_payload, aggregate_id = self._aggregate_units(record, own_war_units, categories, scores)

            results.append(
                {
                    "score": int(aggregate_payload["score"]),
                    "score_reasoning": clean_text(str(aggregate_payload["reasoning"])),
                    "confidence": base_confidence,
                    "war_related": True,
                    "model": self.model,
                    "response_id": self._join_response_ids(base_response_ids + ([aggregate_id] if aggregate_id else [])),
                    "scored_at": scored_at,
                    "pipeline_version": self.pipeline_version,
                }
            )

        return results

    def _build_units(self, record: ScrapedRecord) -> list[TextUnit]:
        if record.country_code == "CN" and record.source_kind == "mfa_regular_press_conference":
            units = self._segment_cn_press_conference(record)
            if units:
                return units
        return [TextUnit(unit_id="u1", label="statement", text=truncate_text(record.content, 18000))]

    def _segment_cn_press_conference(self, record: ScrapedRecord) -> list[TextUnit]:
        speaker = clean_text(record.speaker).replace("\u2019", "'")
        lines = [clean_text(line) for line in record.content.splitlines() if clean_text(line)]
        if not lines:
            return []

        units: list[TextUnit] = []
        index = 0
        while index < len(lines):
            match = QUESTION_LABEL_RE.match(lines[index])
            if not match:
                index += 1
                continue

            label = match.group("label").strip()
            if speaker and label.lower() == speaker.lower():
                index += 1
                continue

            question_parts = [match.group("body").strip()]
            index += 1
            while index < len(lines):
                next_match = QUESTION_LABEL_RE.match(lines[index])
                if next_match and speaker and next_match.group("label").strip().lower() == speaker.lower():
                    break
                question_parts.append(lines[index])
                index += 1

            if index >= len(lines):
                break

            answer_match = QUESTION_LABEL_RE.match(lines[index])
            if not answer_match:
                continue

            answer_parts = [answer_match.group("body").strip()]
            index += 1
            while index < len(lines):
                next_match = QUESTION_LABEL_RE.match(lines[index])
                if next_match and (not speaker or next_match.group("label").strip().lower() != speaker.lower()):
                    break
                answer_parts.append(lines[index])
                index += 1

            question_text = clean_text(" ".join(question_parts))
            answer_text = clean_text(" ".join(answer_parts))
            combined = clean_text(
                f"Question ({label}): {question_text}\nAnswer ({speaker or 'Spokesperson'}): {answer_text}"
            )
            if len(combined) < 60:
                continue
            units.append(TextUnit(unit_id=f"u{len(units) + 1}", label=label, text=truncate_text(combined, 2500)))

        return units

    def _score_relevance(
        self,
        record: ScrapedRecord,
        units: list[TextUnit],
    ) -> tuple[dict[str, dict[str, object]], list[str], list[dict[str, str]]]:
        runs: list[dict[str, dict[str, object]]] = []
        response_ids: list[str] = []
        events: list[dict[str, str]] = []
        recovered_units: set[str] = set()
        for variant in RELEVANCE_VARIANTS:
            payload, response_id = self._request_stage_payload(
                self._relevance_system_prompt(),
                self._relevance_user_prompt(record, units, variant),
            )
            response_ids.append(response_id)
            mapped = self._map_stage_results(payload, key="war_related")
            mapped, repair_ids, repaired = self._recover_missing_stage_results(
                stage_name="relevance",
                record=record,
                units=units,
                variant=variant,
                key="war_related",
                mapped=mapped,
            )
            response_ids.extend(repair_ids)
            recovered_units.update(repaired)
            runs.append(mapped)

        final: dict[str, dict[str, object]] = {}
        for unit in units:
            labels = [normalize_bool(run[unit.unit_id]["war_related"]) for run in runs]
            if len(set(labels)) == 1:
                final[unit.unit_id] = {
                    "war_related": labels[0],
                    "rationale": clean_text(str(runs[0][unit.unit_id].get("rationale", ""))),
                }
                status = "retry" if unit.unit_id in recovered_units else "unanimous"
                events.append({"kind": "relevance", "status": status})
                continue

            validated, validation_id, status = self._resolve_disagreement(
                stage_name="relevance",
                record=record,
                unit=unit,
                candidates=[run[unit.unit_id] for run in runs],
            )
            response_ids.append(validation_id)
            final[unit.unit_id] = {
                "war_related": normalize_bool(validated["label"]),
                "rationale": clean_text(str(validated["reasoning"])),
            }
            events.append({"kind": "relevance", "status": status})
        return final, response_ids, events

    def _score_categories(
        self,
        record: ScrapedRecord,
        war_units: list[TextUnit],
    ) -> tuple[dict[str, dict[str, object]], list[str], list[dict[str, str]]]:
        runs: list[dict[str, dict[str, object]]] = []
        response_ids: list[str] = []
        events: list[dict[str, str]] = []
        recovered_units: set[str] = set()
        for variant in CATEGORY_VARIANTS:
            payload, response_id = self._request_stage_payload(
                self._category_system_prompt(),
                self._category_user_prompt(record, war_units, variant),
            )
            response_ids.append(response_id)
            mapped = self._map_stage_results(payload, key="category")
            mapped, repair_ids, repaired = self._recover_missing_stage_results(
                stage_name="category",
                record=record,
                units=war_units,
                variant=variant,
                key="category",
                mapped=mapped,
            )
            response_ids.extend(repair_ids)
            recovered_units.update(repaired)
            runs.append(mapped)

        final: dict[str, dict[str, object]] = {}
        for unit in war_units:
            labels = [normalize_category(run[unit.unit_id]["category"]) for run in runs]
            if len(set(labels)) == 1:
                final[unit.unit_id] = {
                    "category": labels[0],
                    "rationale": clean_text(str(runs[0][unit.unit_id].get("rationale", ""))),
                }
                status = "retry" if unit.unit_id in recovered_units else "unanimous"
                events.append({"kind": "category", "status": status})
                continue

            validated, validation_id, status = self._resolve_disagreement(
                stage_name="category",
                record=record,
                unit=unit,
                candidates=[run[unit.unit_id] for run in runs],
            )
            response_ids.append(validation_id)
            final[unit.unit_id] = {
                "category": normalize_category(validated["label"]),
                "rationale": clean_text(str(validated["reasoning"])),
            }
            events.append({"kind": "category", "status": status})
        return final, response_ids, events

    def _score_intensity(
        self,
        record: ScrapedRecord,
        war_units: list[TextUnit],
        categories: dict[str, dict[str, object]],
    ) -> tuple[dict[str, dict[str, object]], list[str], list[dict[str, str]]]:
        runs: list[dict[str, dict[str, object]]] = []
        response_ids: list[str] = []
        events: list[dict[str, str]] = []
        recovered_units: set[str] = set()
        for variant in SCORE_VARIANTS:
            payload, response_id = self._request_stage_payload(
                self._score_system_prompt(),
                self._score_user_prompt(record, war_units, categories, variant),
            )
            response_ids.append(response_id)
            mapped = self._map_stage_results(payload, key="score")
            mapped, repair_ids, repaired = self._recover_missing_stage_results(
                stage_name="score",
                record=record,
                units=war_units,
                variant=variant,
                key="score",
                mapped=mapped,
                categories=categories,
            )
            response_ids.extend(repair_ids)
            recovered_units.update(repaired)
            runs.append(mapped)

        final: dict[str, dict[str, object]] = {}
        for unit in war_units:
            labels = [int(run[unit.unit_id]["score"]) for run in runs]
            if len(set(labels)) == 1:
                final[unit.unit_id] = {
                    "score": labels[0],
                    "rationale": clean_text(str(runs[0][unit.unit_id].get("rationale", ""))),
                }
                status = "retry" if unit.unit_id in recovered_units else "unanimous"
                events.append({"kind": "score", "status": status})
                continue

            validated, validation_id, status = self._resolve_disagreement(
                stage_name="score",
                record=record,
                unit=unit,
                candidates=[run[unit.unit_id] for run in runs],
            )
            response_ids.append(validation_id)
            final[unit.unit_id] = {
                "score": int(validated["label"]),
                "rationale": clean_text(str(validated["reasoning"])),
            }
            events.append({"kind": "score", "status": status})
        return final, response_ids, events

    def _recover_missing_stage_results(
        self,
        *,
        stage_name: str,
        record: ScrapedRecord,
        units: list[TextUnit],
        variant: str,
        key: str,
        mapped: dict[str, dict[str, object]],
        categories: dict[str, dict[str, object]] | None = None,
    ) -> tuple[dict[str, dict[str, object]], list[str], set[str]]:
        response_ids: list[str] = []
        repaired_units: set[str] = set()
        missing = [unit for unit in units if unit.unit_id not in mapped]
        if not missing:
            return mapped, response_ids, repaired_units

        repaired_units.update(unit.unit_id for unit in missing)
        recovered, recovered_ids = self._request_stage_for_units(
            stage_name=stage_name,
            record=record,
            units=missing,
            variant=variant,
            key=key,
            categories=categories,
        )
        mapped.update(recovered)
        response_ids.extend(recovered_ids)

        still_missing = [unit for unit in units if unit.unit_id not in mapped]
        for unit in still_missing:
            recovered, recovered_ids = self._request_stage_for_units(
                stage_name=stage_name,
                record=record,
                units=[unit],
                variant=variant,
                key=key,
                categories=categories,
            )
            mapped.update(recovered)
            response_ids.extend(recovered_ids)
            if unit.unit_id not in mapped:
                raise KeyError(unit.unit_id)
            repaired_units.add(unit.unit_id)

        return mapped, response_ids, repaired_units

    def _request_stage_for_units(
        self,
        *,
        stage_name: str,
        record: ScrapedRecord,
        units: list[TextUnit],
        variant: str,
        key: str,
        categories: dict[str, dict[str, object]] | None = None,
    ) -> tuple[dict[str, dict[str, object]], list[str]]:
        if stage_name == "relevance":
            system_prompt = self._relevance_system_prompt()
            user_prompt = self._relevance_user_prompt(record, units, variant)
        elif stage_name == "category":
            system_prompt = self._category_system_prompt()
            user_prompt = self._category_user_prompt(record, units, variant)
        elif stage_name == "score":
            if categories is None:
                raise ValueError("Score-stage repair requires categories.")
            system_prompt = self._score_system_prompt()
            user_prompt = self._score_user_prompt(record, units, categories, variant)
        else:
            raise ValueError(f"Unsupported stage repair: {stage_name}")

        payload, response_id = self._request_stage_payload(system_prompt, user_prompt)
        return self._map_stage_results(payload, key=key), [response_id]

    def _aggregate_units(
        self,
        record: ScrapedRecord,
        war_units: list[TextUnit],
        categories: dict[str, dict[str, object]],
        scores: dict[str, dict[str, object]],
    ) -> tuple[dict[str, object], str]:
        if len(war_units) == 1:
            unit = war_units[0]
            payload = {
                "score": int(scores[unit.unit_id]["score"]),
                "reasoning": (
                    f"Conference-level score inherits the single war-related unit {unit.unit_id} "
                    f"({categories[unit.unit_id]['category']}, {scores[unit.unit_id]['score']}). "
                    f"{scores[unit.unit_id]['rationale']}"
                ),
            }
            return payload, ""

        system_prompt = (
            "You are the conference-level aggregation and validation agent for the WDSI pipeline. "
            "Choose one conference-level raw score from -3 to 3. Do not average unit-level scores. "
            "Select the dominant official war-related tone of the conference day."
        )
        lines = [
            f"Country code: {record.country_code}",
            f"Source kind: {record.source_kind}",
            f"Published date: {record.published_at}",
            f"Title: {record.title}",
            "War-related units and prior stage outputs:",
        ]
        for unit in war_units:
            lines.extend(
                [
                    f"[{unit.unit_id}] {unit.label}",
                    truncate_text(unit.text, 1000),
                    f"Category: {categories[unit.unit_id]['category']}",
                    f"Unit score: {scores[unit.unit_id]['score']}",
                    f"Rationale: {scores[unit.unit_id]['rationale']}",
                    "",
                ]
            )
        lines.extend(
            [
                "Return only JSON with keys score, reasoning, selected_unit_ids.",
                "score must be an integer between -3 and 3.",
            ]
        )
        payload, response_id = self._request_stage_payload(system_prompt, "\n".join(lines))
        score = int(payload["score"])
        if score < -3 or score > 3:
            raise ValueError(f"Out-of-range aggregate score {score} for {record.url}")
        return {
            "score": score,
            "reasoning": clean_text(str(payload.get("reasoning", ""))),
        }, response_id

    def _resolve_disagreement(
        self,
        *,
        stage_name: str,
        record: ScrapedRecord,
        unit: TextUnit,
        candidates: list[dict[str, object]],
    ) -> tuple[dict[str, object], str, str]:
        payload, response_id = self._request_stage_payload(
            self._validator_system_prompt(stage_name),
            self._validator_user_prompt(stage_name, record, unit, candidates),
        )
        action = clean_text(str(payload.get("action", "accept"))).lower()
        if action != "retry":
            return payload, response_id, "validated"

        rerun_payload, rerun_id = self._request_stage_payload(
            self._retry_system_prompt(stage_name),
            self._retry_user_prompt(stage_name, record, unit, payload),
        )
        merged_id = self._join_response_ids([response_id, rerun_id])
        return {
            "label": rerun_payload["label"],
            "reasoning": clean_text(str(rerun_payload.get("reasoning", ""))),
        }, merged_id, "retry"

    def _map_stage_results(self, payload: dict[str, object], *, key: str) -> dict[str, dict[str, object]]:
        results = payload.get("results")
        if not isinstance(results, list):
            raise ValueError(f"Stage payload missing results list: {payload}")

        mapped: dict[str, dict[str, object]] = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            unit_id = clean_text(str(item.get("unit_id", "")))
            if not unit_id:
                continue
            if key not in item:
                raise ValueError(f"Missing {key} in stage item: {item}")
            mapped[unit_id] = {
                key: item[key],
                "rationale": clean_text(str(item.get("rationale", ""))),
            }

        if not mapped:
            raise ValueError(f"Stage payload did not contain any usable results: {payload}")
        return mapped

    def _request_stage_payload(self, system_prompt: str, user_prompt: str) -> tuple[dict[str, object], str]:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                payload, response_id = self._request_json_payload(system_prompt, user_prompt)
                return payload, response_id
            except Exception as exc:  # pragma: no cover - retry logic
                last_error = exc
                if attempt == 2:
                    break
                time.sleep(2 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def _request_json_payload(self, system_prompt: str, user_prompt: str) -> tuple[dict[str, object], str]:
        if self.base_url:
            return self._request_with_chat_completions(system_prompt, user_prompt)
        return self._request_with_responses_api(system_prompt, user_prompt)

    def _request_with_responses_api(self, system_prompt: str, user_prompt: str) -> tuple[dict[str, object], str]:
        response = self.client.responses.create(
            model=self.model,
            reasoning={"effort": self.reasoning_effort},
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_output_tokens=2200,
        )
        payload = extract_json_object(response.output_text)
        return payload, getattr(response, "id", "")

    def _request_with_chat_completions(self, system_prompt: str, user_prompt: str) -> tuple[dict[str, object], str]:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )
        content = response.choices[0].message.content or ""
        payload = extract_json_object(content)
        return payload, getattr(response, "id", "")

    @staticmethod
    def _relevance_system_prompt() -> str:
        return (
            "You are Agent 1 in a paper-style WDSI pipeline. "
            "For each unit, decide whether it is genuinely about war, armed conflict, military security, deterrence, "
            "sanctions related to conflict, terrorism, ceasefire, or closely related escalation risks. "
            "Return only JSON with a top-level key results. Each result must contain unit_id, war_related, rationale."
        )

    @staticmethod
    def _category_system_prompt() -> str:
        return (
            "You are Agent 2 in a paper-style WDSI pipeline. "
            "For each already war-related unit, classify the sentiment direction with respect to war and the use of force "
            "as negative, neutral, or positive. Return only JSON with key results. "
            "Each result must contain unit_id, category, rationale."
        )

    @staticmethod
    def _score_system_prompt() -> str:
        return (
            "You are Agent 3 in a paper-style WDSI pipeline. "
            "For each already war-related unit, assign an intensity score from -3 to 3, conditional on the supplied category. "
            "Use 0 only for neutral. Negative scores run from mildly negative (-1) to strongly negative (-3). "
            "Positive scores run from mildly positive (1) to strongly positive (3). "
            "Return only JSON with key results. Each result must contain unit_id, score, rationale."
        )

    @staticmethod
    def _validator_system_prompt(stage_name: str) -> str:
        return (
            f"You are the validation agent for the {stage_name} stage of a multi-step WDSI pipeline. "
            "Review three independent outputs and their rationales. If one interpretation is clearly more coherent, accept it. "
            "If the disagreement reflects unresolved ambiguity, request a retry with a brief clarification. "
            "Return only JSON with keys action, label, reasoning, clarification. Use action='accept' or action='retry'."
        )

    @staticmethod
    def _retry_system_prompt(stage_name: str) -> str:
        return (
            f"You are rerunning the {stage_name} stage after validator feedback in a paper-style WDSI pipeline. "
            "Return only JSON with keys label and reasoning."
        )

    def _relevance_user_prompt(self, record: ScrapedRecord, units: list[TextUnit], variant: str) -> str:
        return self._stage_user_prompt(
            record,
            units,
            instructions=[
                variant,
                "If a unit is only about economics, protocol, humanitarian relief without a security context, or culture, mark war_related=false.",
            ],
        )

    def _category_user_prompt(self, record: ScrapedRecord, units: list[TextUnit], variant: str) -> str:
        return self._stage_user_prompt(
            record,
            units,
            instructions=[
                variant,
                "Negative includes condemnation, warnings, threats, or sharp criticism tied to war or force.",
                "Positive includes support for ceasefire, de-escalation, mediation, peace talks, or peaceful outcomes.",
                "Neutral is descriptive or balanced with no clear evaluative tone.",
            ],
        )

    def _score_user_prompt(
        self,
        record: ScrapedRecord,
        units: list[TextUnit],
        categories: dict[str, dict[str, object]],
        variant: str,
    ) -> str:
        category_lines = [f"{unit.unit_id}: {categories[unit.unit_id]['category']}" for unit in units]
        return self._stage_user_prompt(
            record,
            units,
            instructions=[
                variant,
                "Condition strictly on the supplied category for each unit.",
                "Use -1 or 1 for mild tone, -2 or 2 for clear tone, and -3 or 3 only for especially strong tone.",
                "Supplied categories:",
                *category_lines,
            ],
        )

    def _validator_user_prompt(
        self,
        stage_name: str,
        record: ScrapedRecord,
        unit: TextUnit,
        candidates: list[dict[str, object]],
    ) -> str:
        key_name = "war_related" if stage_name == "relevance" else ("category" if stage_name == "category" else "score")
        lines = [
            f"Country code: {record.country_code}",
            f"Source kind: {record.source_kind}",
            f"Published date: {record.published_at}",
            f"Title: {record.title}",
            f"Unit id: {unit.unit_id}",
            f"Speaker or label: {unit.label}",
            "Unit text:",
            truncate_text(unit.text, 1800),
            "",
            "Independent outputs:",
        ]
        for index, candidate in enumerate(candidates, start=1):
            lines.extend(
                [
                    f"Run {index} label: {candidate[key_name]}",
                    f"Run {index} rationale: {candidate.get('rationale', '')}",
                    "",
                ]
            )
        lines.append("If you accept, put the chosen label in label. If you retry, fill clarification.")
        return "\n".join(lines)

    def _retry_user_prompt(
        self,
        stage_name: str,
        record: ScrapedRecord,
        unit: TextUnit,
        validator_payload: dict[str, object],
    ) -> str:
        if stage_name == "category":
            label_hint = "Output label as negative, neutral, or positive."
        elif stage_name == "score":
            label_hint = "Output label as an integer from -3 to 3."
        else:
            label_hint = "Output label as true or false."

        return "\n".join(
            [
                f"Country code: {record.country_code}",
                f"Source kind: {record.source_kind}",
                f"Published date: {record.published_at}",
                f"Title: {record.title}",
                f"Unit id: {unit.unit_id}",
                "Unit text:",
                truncate_text(unit.text, 1800),
                "",
                f"Validator clarification: {clean_text(str(validator_payload.get('clarification', 'Resolve the ambiguity carefully.')))}",
                label_hint,
            ]
        )

    def _stage_user_prompt(
        self,
        record: ScrapedRecord,
        units: list[TextUnit],
        *,
        instructions: list[str],
    ) -> str:
        lines = [
            f"Country code: {record.country_code}",
            f"Source kind: {record.source_kind}",
            f"Published date: {record.published_at}",
            f"Title: {record.title or '(none)'}",
            f"Speaker or author: {record.speaker or '(none)'}",
            f"URL: {record.url or '(none)'}",
            "",
            "Instructions:",
            *instructions,
            "",
            "Units:",
        ]
        for unit in units:
            lines.extend([f"[{unit.unit_id}] {unit.label}", truncate_text(unit.text, 2200), ""])
        return "\n".join(lines)

    @staticmethod
    def _confidence_from_events(events: list[dict[str, str]]) -> float:
        if not events:
            return 0.65
        statuses = Counter(event["status"] for event in events)
        total = len(events)
        unanimous_ratio = statuses.get("unanimous", 0) / total
        validated_ratio = statuses.get("validated", 0) / total
        retry_ratio = statuses.get("retry", 0) / total
        confidence = 0.62 + (0.28 * unanimous_ratio) + (0.12 * validated_ratio) - (0.08 * retry_ratio)
        return round(max(0.35, min(confidence, 0.98)), 3)

    @staticmethod
    def _join_response_ids(values: list[str]) -> str:
        cleaned = [value for value in values if value]
        unique = list(dict.fromkeys(cleaned))
        return "|".join(unique[:12])
