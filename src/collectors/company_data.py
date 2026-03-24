from __future__ import annotations

from typing import Any

import yfinance as yf

from src.utils.validation import to_yahoo_symbol


class CompanyDataCollector:
    """Collects company profile data from Yahoo Finance on a best-effort basis."""

    def collect(self, ticker: str) -> dict[str, Any]:
        symbol = to_yahoo_symbol(ticker)
        info: dict[str, Any] = {}
        try:
            info = yf.Ticker(symbol).info or {}
        except Exception:
            info = {}

        company_name = info.get("shortName") or info.get("longName")
        sector = info.get("sector") or info.get("sectorDisp")
        segment = info.get("industry") or info.get("industryDisp")
        description = info.get("longBusinessSummary") or info.get("description")

        return {
            "company_name": company_name,
            "sector": sector,
            "segment": segment,
            "business_description": description,
            "_sources": ["yfinance"],
            "_symbol": symbol,
        }
