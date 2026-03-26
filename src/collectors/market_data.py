from __future__ import annotations

from typing import Any

from src.collectors.public_api import get_public_data_api


class MarketDataCollector:
    """Collects market data from public JSON/CSV APIs."""

    def __init__(self) -> None:
        self.public_api = get_public_data_api()

    def collect(self, ticker: str) -> dict[str, Any]:
        try:
            return self.public_api.get_market_data(ticker)
        except Exception:
            return {
                "current_price": None,
                "p_l": None,
                "roe": None,
                "net_debt_ebitda": None,
                "net_margin": None,
                "dividend_yield": None,
                "net_debt": None,
                "ebitda": None,
                "metric_sources": {},
                "metric_warnings": [],
                "price_history": [],
                "_profile_fallbacks": {},
                "_sources": [],
            }
