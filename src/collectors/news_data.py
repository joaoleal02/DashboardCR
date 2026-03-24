from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from xml.etree import ElementTree

import requests
import yfinance as yf

from src.config import get_settings
from src.utils.validation import to_yahoo_symbol


class NewsDataCollector:
    """Collects up to five recent company-relevant news items."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def collect(self, ticker: str, company_name: str | None = None) -> list[dict[str, Any]]:
        items = self._fetch_yahoo_news(ticker)
        if len(items) < 5:
            fallback_items = self._fetch_google_news_rss(ticker=ticker, company_name=company_name)
            items = self._merge_unique_news(items + fallback_items)
        return items[:5]

    def _fetch_yahoo_news(self, ticker: str) -> list[dict[str, Any]]:
        try:
            symbol = to_yahoo_symbol(ticker)
            news = yf.Ticker(symbol).news or []
        except Exception:
            return []

        parsed_items: list[dict[str, Any]] = []
        for item in news:
            content = item.get("content") or {}
            title = item.get("title") or content.get("title")
            source = item.get("publisher")
            if not source:
                provider = content.get("provider") or {}
                source = provider.get("displayName")
            publish_ts = item.get("providerPublishTime") or content.get("pubDate")
            link = (
                item.get("link")
                or content.get("clickThroughUrl", {}).get("url")
                or content.get("canonicalUrl", {}).get("url")
            )
            parsed_items.append(
                {
                    "title": title,
                    "source": source,
                    "date": self._coerce_datetime(publish_ts),
                    "url": link,
                }
            )
        return self._merge_unique_news(parsed_items)

    def _fetch_google_news_rss(self, ticker: str, company_name: str | None = None) -> list[dict[str, Any]]:
        query = " ".join(part for part in [ticker, company_name, "B3"] if part)
        try:
            response = requests.get(
                self.settings.google_news_rss_url,
                params={"q": query, "hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-419"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=self.settings.request_timeout,
            )
            response.raise_for_status()
            root = ElementTree.fromstring(response.content)
        except Exception:
            return []

        items: list[dict[str, Any]] = []
        channel = root.find("./channel")
        if channel is None:
            return items

        for item in channel.findall("./item"):
            title = self._get_xml_text(item, "title")
            source = self._get_xml_text(item, "source")
            pub_date = self._get_xml_text(item, "pubDate")
            url = self._get_xml_text(item, "link")
            items.append(
                {
                    "title": title,
                    "source": source,
                    "date": self._coerce_datetime(pub_date),
                    "url": url,
                }
            )
        return self._merge_unique_news(items)

    def _get_xml_text(self, node: ElementTree.Element, tag: str) -> str | None:
        child = node.find(tag)
        return child.text.strip() if child is not None and child.text else None

    def _merge_unique_news(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        unique_items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in items:
            title = (item.get("title") or "").strip()
            url = (item.get("url") or "").strip()
            if not title:
                continue
            key = f"{title.lower()}::{url.lower()}"
            if key in seen:
                continue
            seen.add(key)
            unique_items.append(item)
        return unique_items

    def _coerce_datetime(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, int):
            return datetime.fromtimestamp(value, tz=timezone.utc).date().isoformat()
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                return parsedate_to_datetime(value).date().isoformat()
            except Exception:
                return value
        return None
