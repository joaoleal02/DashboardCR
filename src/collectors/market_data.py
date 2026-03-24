from __future__ import annotations

import re
import unicodedata
from typing import Any

import requests
import yfinance as yf
from bs4 import BeautifulSoup

from src.config import get_settings
from src.utils.validation import to_yahoo_symbol


class MarketDataCollector:
    """Collects quote and fundamentals using Yahoo Finance plus Fundamentus fallback."""

    FUNDAMENTUS_FIELD_MAP = {
        "empresa": "company_name",
        "setor": "sector",
        "subsetor": "segment",
        "p/l": "p_l",
        "roe": "roe",
        "div.liquida/ebitda": "net_debt_ebitda",
        "div.liq/ebitda": "net_debt_ebitda",
        "dividaliquida/ebitda": "net_debt_ebitda",
        "marg.liquida": "net_margin",
        "margliquida": "net_margin",
        "div.yield": "dividend_yield",
        "divyield": "dividend_yield",
    }

    def __init__(self) -> None:
        self.settings = get_settings()

    def collect(self, ticker: str) -> dict[str, Any]:
        yahoo_symbol = to_yahoo_symbol(ticker)
        quote = self._fetch_quote(yahoo_symbol)
        fundamentus_values = self._fetch_fundamentus_snapshot(ticker)

        return {
            "current_price": quote,
            "p_l": fundamentus_values.get("p_l"),
            "roe": fundamentus_values.get("roe"),
            "net_debt_ebitda": fundamentus_values.get("net_debt_ebitda"),
            "net_margin": fundamentus_values.get("net_margin"),
            "dividend_yield": fundamentus_values.get("dividend_yield"),
            "_profile_fallbacks": {
                "company_name": fundamentus_values.get("company_name"),
                "sector": fundamentus_values.get("sector"),
                "segment": fundamentus_values.get("segment"),
            },
            "_sources": ["yfinance", "fundamentus"],
        }

    def _fetch_quote(self, yahoo_symbol: str) -> float | None:
        try:
            ticker = yf.Ticker(yahoo_symbol)
            fast_info = getattr(ticker, "fast_info", None)
            if fast_info:
                last_price = fast_info.get("lastPrice") or fast_info.get("last_price")
                if last_price is not None:
                    return float(last_price)
            info = ticker.info or {}
            for key in ("currentPrice", "regularMarketPrice", "previousClose"):
                value = info.get(key)
                if value is not None:
                    return float(value)
        except Exception:
            return None
        return None

    def _fetch_fundamentus_snapshot(self, ticker: str) -> dict[str, Any]:
        try:
            response = requests.get(
                self.settings.fundamentus_url,
                params={"papel": ticker},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=self.settings.request_timeout,
            )
            response.raise_for_status()
        except Exception:
            return {}

        soup = BeautifulSoup(response.text, "html.parser")
        values: dict[str, Any] = {}
        for row in soup.select("table tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            for index in range(0, len(cells) - 1, 2):
                raw_label = cells[index]
                raw_value = cells[index + 1]
                key = self.FUNDAMENTUS_FIELD_MAP.get(self._normalize_label(raw_label))
                if not key:
                    continue
                if key in {"company_name", "sector", "segment"}:
                    values[key] = raw_value or None
                else:
                    values[key] = self._parse_numeric_value(raw_value)
        return values

    def _normalize_label(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
        normalized = normalized.lower()
        normalized = normalized.replace(" ", "")
        normalized = re.sub(r"[^a-z0-9/.\-]", "", normalized)
        return normalized

    def _parse_numeric_value(self, value: str) -> float | None:
        raw = value.strip()
        if not raw or raw == "-":
            return None
        cleaned = raw.replace("%", "").replace("R$", "").replace("x", "")
        cleaned = cleaned.replace(".", "").replace(",", ".").strip()
        cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
