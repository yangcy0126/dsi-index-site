from __future__ import annotations

import hashlib
import html
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

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


def strip_html(value: str) -> str:
    soup = BeautifulSoup(value, "html.parser")
    return clean_text(soup.get_text("\n"))


def normalize_cn_article_url(value: str) -> str:
    url = clean_text(value)
    url = url.replace("http://www.mfa.gov.cn", "https://www.mfa.gov.cn")
    url = url.replace("http://www.fmprc.gov.cn", "https://www.fmprc.gov.cn")
    url = url.replace("https://www.fmprc.gov.cn/eng/", "https://www.mfa.gov.cn/eng/")
    return url


def parse_us_date(value: str) -> str:
    text = clean_text(value)
    for fmt in ("%B %d, %Y %H:%M", "%B %d, %Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Unsupported date format: {value}") from exc


def request_json(session: requests.Session, url: str) -> object:
    last_error: Exception | None = None
    for timeout_seconds in (30, 45, 60):
        try:
            response = session.get(url, headers=STATE_HEADERS, timeout=timeout_seconds)
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
    assert last_error is not None
    raise last_error


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
            payload = request_json(self.session, self.briefing_endpoint.format(page=page))
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
