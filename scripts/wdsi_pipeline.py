from __future__ import annotations

import hashlib
import html
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
from urllib.parse import urljoin

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

JSON_BLOCK_RE = re.compile(r"\{.*\}", re.S)
WHITESPACE_RE = re.compile(r"\s+")


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
    response = session.get(url, headers=STATE_HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def request_html(session: requests.Session, url: str) -> str:
    response = session.get(url, headers=BROWSER_HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def extract_json_object(text: str) -> dict[str, object]:
    match = JSON_BLOCK_RE.search(text.strip())
    if not match:
        raise ValueError(f"Model response did not contain JSON: {text[:200]}")
    return json.loads(match.group(0))


class ChinaMfaRegularPressSource:
    country_code = "CN"
    base_url = "https://www.mfa.gov.cn/eng/xw/fyrbt/lxjzh/"

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 4) -> list[ScrapedRecord]:
        article_urls: list[str] = []
        for page in range(max_pages):
            suffix = "" if page == 0 else f"index_{page}.shtml"
            url = urljoin(self.base_url, suffix)
            html_text = request_html(self.session, url)
            article_urls.extend(self._parse_listing(html_text, url))

        unique_urls = list(dict.fromkeys(article_urls))
        return [self._parse_article(url) for url in unique_urls]

    def _parse_listing(self, html_text: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html_text, "html.parser")
        article_urls: list[str] = []
        for anchor in soup.select("a[href]"):
            href = anchor.get("href", "").strip()
            if not href:
                continue
            if re.match(r"^\./\d{6}/t\d+_\d+\.html$", href):
                article_urls.append(urljoin(base_url, href))
        return article_urls

    def _parse_article(self, url: str) -> ScrapedRecord:
        html_text = request_html(self.session, url)
        soup = BeautifulSoup(html_text, "html.parser")

        title = clean_text(self._select_text(soup, [".news_header_title", ".news_header", "title"]))
        date_text = self._select_text(soup, [".news_header_time .xltime", ".xltime", ".news_header_bottom"])
        content_node = soup.select_one(".content_text") or soup.select_one(".news_content")
        if content_node is None:
            raise ValueError(f"Could not find article body for {url}")

        for node in content_node.select("img, script, style"):
            node.decompose()

        content = clean_text(content_node.get_text("\n"))
        if not title or not content:
            raise ValueError(f"Missing title or content for {url}")

        speaker = ""
        title_match = re.match(r"Foreign Ministry Spokesperson (.+?)'?s Regular Press Conference", title)
        if title_match:
            speaker = title_match.group(1).replace("’", "'")

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
            node = soup.select_one(selector)
            if node:
                return node.get_text(" ", strip=True)
        return ""


class UsStateDepartmentSource:
    country_code = "US"
    press_endpoint = (
        "https://www.state.gov/wp-json/wp/v2/state_press_release"
        "?per_page=100&page={page}"
    )
    briefing_endpoint = (
        "https://www.state.gov/wp-json/wp/v2/state_briefing"
        "?state_briefing_type=393&per_page=100&page={page}"
    )

    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_recent(self, max_pages: int = 2) -> list[ScrapedRecord]:
        records: list[ScrapedRecord] = []
        for page in range(1, max_pages + 1):
            records.extend(self._fetch_press_releases(page))
            records.extend(self._fetch_press_briefings(page))
        return list({record.url: record for record in records}.values())

    def _fetch_press_releases(self, page: int) -> list[ScrapedRecord]:
        payload = request_json(self.session, self.press_endpoint.format(page=page))
        if not isinstance(payload, list):
            return []

        records: list[ScrapedRecord] = []
        for item in payload:
            link = item.get("link", "")
            if "/releases/office-of-the-spokesperson/" not in link:
                continue
            records.append(self._make_record(item, "state_press_release"))
        return records

    def _fetch_press_briefings(self, page: int) -> list[ScrapedRecord]:
        payload = request_json(self.session, self.briefing_endpoint.format(page=page))
        if not isinstance(payload, list):
            return []

        records: list[ScrapedRecord] = []
        for item in payload:
            link = item.get("link", "")
            if "/briefings/department-press-briefing" not in link:
                continue
            records.append(self._make_record(item, "state_department_press_briefing"))
        return records

    def _make_record(self, item: dict[str, object], source_kind: str) -> ScrapedRecord:
        raw_html = str(((item.get("content") or {}) if isinstance(item.get("content"), dict) else {}).get("rendered", ""))
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
        title = clean_text(strip_html(str(((item.get("title") or {}) if isinstance(item.get("title"), dict) else {}).get("rendered", ""))))
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


class OpenAIWDSIScorer:
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
        user_prompt = self._build_prompt(record)
        last_error: Exception | None = None

        for attempt in range(3):
            try:
                payload = self._request_score_payload(user_prompt)
                score = int(payload["score"])
                if score < -3 or score > 3:
                    raise ValueError(f"Out-of-range score {score} for {record.url}")

                return {
                    "score": score,
                    "score_reasoning": clean_text(str(payload.get("reasoning", ""))),
                    "confidence": float(payload.get("confidence", 0.0)),
                    "war_related": bool(payload.get("war_related", score != 0)),
                    "model": self.model,
                    "response_id": str(payload.get("_response_id", "")),
                    "scored_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                }
            except Exception as exc:  # pragma: no cover - retry logic
                last_error = exc
                if attempt == 2:
                    break
                time.sleep(2 * (attempt + 1))

        assert last_error is not None
        raise last_error

    @staticmethod
    def _system_prompt() -> str:
        return (
            "You score diplomatic texts for a War-related Diplomatic Sentiment Index (WDSI). "
            "Focus only on war, military, security, sanctions, arms sales, terrorism, ceasefire, "
            "conflict escalation, peace talks, and conflict resolution language. "
            "Use this scale: -3 strongly escalatory/hostile war-related diplomatic tone; "
            "-2 clearly negative or condemnatory war-related tone; "
            "-1 mildly negative conflict-security concern or criticism; "
            "0 no meaningful war-related diplomatic sentiment or neutral/mixed with no clear direction; "
            "1 mildly positive de-escalatory or cooperative security tone; "
            "2 clearly positive peace-building, ceasefire, or reconciliation tone; "
            "3 strongly positive breakthrough-level peace or de-escalation. "
            "If the text is not meaningfully about war or security, return 0. "
            "Return only valid JSON with keys war_related, score, confidence, reasoning, salient_topics. "
            "score must be an integer from -3 to 3. confidence must be between 0 and 1. "
            "reasoning must be short and concrete."
        )

    def _request_score_payload(self, user_prompt: str) -> dict[str, object]:
        if self.base_url:
            return self._request_with_chat_completions(user_prompt)
        return self._request_with_responses_api(user_prompt)

    def _request_with_responses_api(self, user_prompt: str) -> dict[str, object]:
        response = self.client.responses.create(
            model=self.model,
            reasoning={"effort": self.reasoning_effort},
            input=[
                {"role": "system", "content": self._system_prompt()},
                {"role": "user", "content": user_prompt},
            ],
            max_output_tokens=400,
        )
        payload = extract_json_object(response.output_text)
        payload["_response_id"] = getattr(response, "id", "")
        return payload

    def _request_with_chat_completions(self, user_prompt: str) -> dict[str, object]:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self._system_prompt()},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )
        content = response.choices[0].message.content or ""
        payload = extract_json_object(content)
        payload["_response_id"] = getattr(response, "id", "")
        return payload

    @staticmethod
    def _build_prompt(record: ScrapedRecord) -> str:
        content = record.content
        if len(content) > 30000:
            content = content[:30000] + "\n\n[Truncated for scoring]"

        return (
            f"Country code: {record.country_code}\n"
            f"Source kind: {record.source_kind}\n"
            f"Published date: {record.published_at}\n"
            f"Title: {record.title or '(none)'}\n"
            f"Speaker or author: {record.speaker or '(none)'}\n"
            f"URL: {record.url or '(none)'}\n\n"
            "Text to score:\n"
            f"{content}"
        )
