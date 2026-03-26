from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from src.llm.schemas import LLMReport
from src.pipeline.runner import PipelineRunner


@dataclass
class BriefingResult:
    run_id: int
    ticker: str
    company_profile: dict[str, Any]
    market_data: dict[str, Any]
    price_history: list[dict[str, Any]]
    news: list[dict[str, Any]]
    llm_report: LLMReport | None
    llm_error: str | None
    raw_payload: dict[str, Any]
    debug_info: dict[str, Any]
    run_status: str
    llm_status: str
    error_events: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.llm_report is not None:
            payload["llm_report"] = self.llm_report.to_dict()
        return payload


class BriefingService:
    def __init__(self) -> None:
        self.pipeline_runner = PipelineRunner()

    def generate_briefing(self, ticker: str, trigger: str = "dashboard") -> BriefingResult:
        result = self.pipeline_runner.run(ticker=ticker, trigger=trigger)
        return BriefingResult(
            run_id=result.run_id,
            ticker=result.ticker,
            company_profile=result.company_profile,
            market_data=result.market_data,
            price_history=result.price_history,
            news=result.news,
            llm_report=result.llm_report,
            llm_error=result.llm_error,
            raw_payload=result.raw_payload,
            debug_info=result.debug_info,
            run_status=result.run_status,
            llm_status=result.llm_status,
            error_events=[event.to_dict() for event in result.errors],
        )
