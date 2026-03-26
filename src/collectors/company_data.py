from __future__ import annotations

from typing import Any

from src.collectors.public_api import get_public_data_api


class CompanyDataCollector:
    """Collects company profile data from public JSON/CSV APIs."""

    def __init__(self) -> None:
        self.public_api = get_public_data_api()

    def collect(self, ticker: str) -> dict[str, Any]:
        return self.public_api.get_company_profile(ticker)
