from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from src.collectors.company_data import CompanyDataCollector
from src.collectors.market_data import MarketDataCollector
from src.collectors.news_data import NewsDataCollector
from src.llm.client import LLMClient, LLMGenerationError
from src.llm.schemas import LLMReport


@dataclass
class BriefingResult:
    ticker: str
    company_profile: dict[str, Any]
    market_data: dict[str, Any]
    price_history: list[dict[str, Any]]
    news: list[dict[str, Any]]
    llm_report: LLMReport | None
    llm_error: str | None
    raw_payload: dict[str, Any]
    debug_info: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.llm_report is not None:
            payload["llm_report"] = self.llm_report.to_dict()
        return payload


class BriefingService:
    def __init__(self) -> None:
        self.company_collector = CompanyDataCollector()
        self.market_collector = MarketDataCollector()
        self.news_collector = NewsDataCollector()
        self.llm_client = LLMClient()

    def generate_briefing(self, ticker: str) -> BriefingResult:
        company_profile = self.company_collector.collect(ticker)
        market_payload = self.market_collector.collect(ticker)
        company_profile = self._merge_company_profile(
            company_profile=company_profile,
            profile_fallbacks=market_payload.get("_profile_fallbacks", {}),
        )
        news = self.news_collector.collect(ticker, company_name=company_profile.get("company_name"))

        market_data = {
            key: value
            for key, value in market_payload.items()
            if not key.startswith("_") and key != "price_history"
        }
        price_history = market_payload.get("price_history", [])
        raw_payload = {
            "ticker": ticker,
            "company_profile": {
                key: value for key, value in company_profile.items() if not key.startswith("_")
            },
            "market_data": market_data,
            "price_history": price_history,
            "news": news,
        }

        llm_report: LLMReport | None = None
        llm_error: str | None = None
        llm_raw_response: str | None = None

        try:
            llm_report, llm_raw_response = self.llm_client.generate_report(raw_payload)
        except LLMGenerationError as exc:
            llm_error = str(exc)
            llm_raw_response = exc.raw_response
        except Exception as exc:
            llm_error = str(exc)

        debug_info = {
            "ticker": ticker,
            "profile_sources": company_profile.get("_sources", []),
            "market_sources": market_payload.get("_sources", []),
            "llm_configured": self.llm_client.is_configured(),
            "news_count": len(news),
            "price_history_points": len(price_history),
            "llm_raw_response": llm_raw_response,
        }

        return BriefingResult(
            ticker=ticker,
            company_profile=raw_payload["company_profile"],
            market_data=market_data,
            price_history=price_history,
            news=news,
            llm_report=llm_report,
            llm_error=llm_error,
            raw_payload=raw_payload,
            debug_info=debug_info,
        )

    def _merge_company_profile(
        self,
        company_profile: dict[str, Any],
        profile_fallbacks: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(company_profile)
        for field in ("company_name", "sector", "segment"):
            if not merged.get(field) and profile_fallbacks.get(field):
                merged[field] = profile_fallbacks[field]
        return merged
