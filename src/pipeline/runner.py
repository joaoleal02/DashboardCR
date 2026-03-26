from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.collectors.company_data import CompanyDataCollector
from src.collectors.market_data import MarketDataCollector
from src.collectors.news_data import NewsDataCollector
from src.errors import InvalidTickerError, SourceUnavailableError
from src.llm.client import LLMClient, LLMGenerationError
from src.llm.schemas import LLMReport
from src.logging_utils import configure_logging
from src.storage.repository import BriefingRepository
from src.utils.validation import normalize_ticker, validate_ticker


@dataclass
class PipelineErrorEvent:
    stage: str
    error_type: str
    message: str
    details: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class PipelineRunResult:
    run_id: int
    ticker: str
    company_profile: dict[str, Any]
    market_data: dict[str, Any]
    price_history: list[dict[str, Any]]
    news: list[dict[str, Any]]
    llm_report: LLMReport | None
    llm_error: str | None
    llm_raw_response: str | None
    raw_payload: dict[str, Any]
    errors: list[PipelineErrorEvent]
    run_status: str
    llm_status: str
    debug_info: dict[str, Any]


class PipelineRunner:
    def __init__(self, repository: BriefingRepository | None = None) -> None:
        configure_logging()
        self.logger = logging.getLogger(__name__)
        self.repository = repository or BriefingRepository()
        self.company_collector = CompanyDataCollector()
        self.market_collector = MarketDataCollector()
        self.news_collector = NewsDataCollector()
        self.llm_client = LLMClient()

    def run(self, ticker: str, trigger: str = "dashboard") -> PipelineRunResult:
        requested_ticker = ticker
        normalized = normalize_ticker(ticker)
        started_at = self._now()
        run_id = self.repository.start_run(
            requested_ticker=requested_ticker,
            normalized_ticker=normalized,
            trigger=trigger,
            started_at=started_at,
        )
        self.logger.info("Starting pipeline run %s for %s via %s", run_id, normalized, trigger)

        company_profile = self._empty_company_profile()
        market_payload = self._empty_market_payload()
        news: list[dict[str, Any]] = []
        llm_report: LLMReport | None = None
        llm_error: str | None = None
        llm_raw_response: str | None = None
        llm_status = "not_requested"
        company_id: int | None = None
        errors: list[PipelineErrorEvent] = []

        is_valid_input, validation_error = validate_ticker(normalized)
        if not is_valid_input:
            errors.append(PipelineErrorEvent("validation", "invalid_ticker", validation_error or "Invalid ticker input."))
            return self._finalize(
                run_id=run_id,
                ticker=normalized,
                company_id=company_id,
                company_profile=company_profile,
                market_payload=market_payload,
                news=news,
                llm_report=llm_report,
                llm_error=llm_error,
                llm_raw_response=llm_raw_response,
                errors=errors,
                run_status="failed_invalid_ticker",
                llm_status=llm_status,
            )

        try:
            company_profile = self.company_collector.collect(normalized)
            company_id = self.repository.upsert_company(
                ticker=normalized,
                cnpj=company_profile.get("_cnpj"),
                code_cvm=company_profile.get("_code_cvm"),
                is_active=True,
                observed_at=started_at,
            )
        except InvalidTickerError as exc:
            errors.append(PipelineErrorEvent("company_profile", "invalid_or_delisted_ticker", str(exc)))
            return self._finalize(
                run_id=run_id,
                ticker=normalized,
                company_id=company_id,
                company_profile=company_profile,
                market_payload=market_payload,
                news=news,
                llm_report=llm_report,
                llm_error=llm_error,
                llm_raw_response=llm_raw_response,
                errors=errors,
                run_status="failed_invalid_ticker",
                llm_status=llm_status,
            )
        except SourceUnavailableError as exc:
            errors.append(PipelineErrorEvent("company_profile", exc.source, str(exc), exc.details))
            self.logger.warning("Run %s company profile source unavailable: %s", run_id, exc)
        except Exception as exc:
            errors.append(PipelineErrorEvent("company_profile", "unexpected_error", str(exc)))
            self.logger.exception("Run %s company profile failed unexpectedly", run_id)

        try:
            market_payload = self.market_collector.collect(normalized)
        except InvalidTickerError as exc:
            errors.append(PipelineErrorEvent("market_data", "invalid_or_delisted_ticker", str(exc)))
            return self._finalize(
                run_id=run_id,
                ticker=normalized,
                company_id=company_id,
                company_profile=company_profile,
                market_payload=market_payload,
                news=news,
                llm_report=llm_report,
                llm_error=llm_error,
                llm_raw_response=llm_raw_response,
                errors=errors,
                run_status="failed_invalid_ticker",
                llm_status=llm_status,
            )
        except SourceUnavailableError as exc:
            errors.append(PipelineErrorEvent("market_data", exc.source, str(exc), exc.details))
            self.logger.warning("Run %s market data source unavailable: %s", run_id, exc)
        except Exception as exc:
            errors.append(PipelineErrorEvent("market_data", "unexpected_error", str(exc)))
            self.logger.exception("Run %s market data failed unexpectedly", run_id)

        if company_id is None and (market_payload.get("_cnpj") or market_payload.get("_code_cvm")):
            company_id = self.repository.upsert_company(
                ticker=normalized,
                cnpj=market_payload.get("_cnpj"),
                code_cvm=market_payload.get("_code_cvm"),
                is_active=True,
                observed_at=started_at,
            )

        company_profile = self._merge_company_profile(
            company_profile=company_profile,
            profile_fallbacks=market_payload.get("_profile_fallbacks", {}),
        )

        try:
            news = self.news_collector.collect(normalized, company_name=company_profile.get("company_name"))
        except SourceUnavailableError as exc:
            errors.append(PipelineErrorEvent("news", exc.source, str(exc), exc.details))
            self.logger.warning("Run %s news source unavailable: %s", run_id, exc)
        except Exception as exc:
            errors.append(PipelineErrorEvent("news", "unexpected_error", str(exc)))
            self.logger.exception("Run %s news collection failed unexpectedly", run_id)

        market_data = {
            key: value
            for key, value in market_payload.items()
            if not key.startswith("_") and key != "price_history"
        }
        price_history = market_payload.get("price_history", [])
        raw_payload = {
            "ticker": normalized,
            "company_profile": {
                key: value for key, value in company_profile.items() if not key.startswith("_")
            },
            "market_data": market_data,
            "price_history": price_history,
            "news": news,
        }

        if self._should_attempt_llm(raw_payload):
            try:
                llm_report, llm_raw_response = self.llm_client.generate_report(raw_payload)
                llm_status = "success"
            except LLMGenerationError as exc:
                llm_error = str(exc)
                llm_raw_response = exc.raw_response
                llm_status = self._classify_llm_error(exc)
                errors.append(PipelineErrorEvent("llm", llm_status, llm_error, llm_raw_response))
                self.logger.warning("Run %s LLM generation failed: %s", run_id, llm_error)
            except Exception as exc:
                llm_error = str(exc)
                llm_status = "failed_request"
                errors.append(PipelineErrorEvent("llm", llm_status, llm_error))
                self.logger.exception("Run %s LLM request failed unexpectedly", run_id)

        run_status = self._determine_run_status(company_profile, market_data, errors, llm_status)
        return self._finalize(
            run_id=run_id,
            ticker=normalized,
            company_id=company_id,
            company_profile=company_profile,
            market_payload=market_payload,
            news=news,
            llm_report=llm_report,
            llm_error=llm_error,
            llm_raw_response=llm_raw_response,
            errors=errors,
            run_status=run_status,
            llm_status=llm_status,
        )

    def _finalize(
        self,
        run_id: int,
        ticker: str,
        company_id: int | None,
        company_profile: dict[str, Any],
        market_payload: dict[str, Any],
        news: list[dict[str, Any]],
        llm_report: LLMReport | None,
        llm_error: str | None,
        llm_raw_response: str | None,
        errors: list[PipelineErrorEvent],
        run_status: str,
        llm_status: str,
    ) -> PipelineRunResult:
        finished_at = self._now()
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
        error_summary = "; ".join(event.message for event in errors[:3]) or None

        self.repository.save_company_profile_snapshot(
            run_id=run_id,
            company_id=company_id,
            profile=company_profile,
            captured_at=finished_at,
        )
        self.repository.save_market_snapshot(
            run_id=run_id,
            market_data=market_data,
            price_history=price_history,
            captured_at=finished_at,
        )
        self.repository.save_news_snapshot(run_id=run_id, news_items=news)
        self.repository.save_llm_report(
            run_id=run_id,
            status=llm_status,
            report_payload=llm_report.to_dict() if llm_report is not None else None,
            raw_response=llm_raw_response,
            error_message=llm_error,
            created_at=finished_at,
        )
        for event in errors:
            self.repository.save_error_event(
                run_id=run_id,
                stage=event.stage,
                error_type=event.error_type,
                message=event.message,
                details=event.details,
                created_at=finished_at,
            )
        self.repository.finalize_run(
            run_id=run_id,
            company_id=company_id,
            status=run_status,
            llm_status=llm_status,
            error_summary=error_summary,
            raw_payload=raw_payload,
            finished_at=finished_at,
        )

        debug_info = {
            "run_id": run_id,
            "ticker": ticker,
            "profile_sources": company_profile.get("_sources", []),
            "market_sources": market_payload.get("_sources", []),
            "llm_configured": self.llm_client.is_configured(),
            "news_count": len(news),
            "price_history_points": len(price_history),
            "llm_raw_response": llm_raw_response,
            "pipeline_errors": [event.to_dict() for event in errors],
        }
        self.logger.info("Finished pipeline run %s for %s with status %s", run_id, ticker, run_status)
        return PipelineRunResult(
            run_id=run_id,
            ticker=ticker,
            company_profile=raw_payload["company_profile"],
            market_data=market_data,
            price_history=price_history,
            news=news,
            llm_report=llm_report,
            llm_error=llm_error,
            llm_raw_response=llm_raw_response,
            raw_payload=raw_payload,
            errors=errors,
            run_status=run_status,
            llm_status=llm_status,
            debug_info=debug_info,
        )

    def _should_attempt_llm(self, raw_payload: dict[str, Any]) -> bool:
        company = raw_payload.get("company_profile") or {}
        market = raw_payload.get("market_data") or {}
        if not self.llm_client.is_configured():
            return False
        return any(value not in (None, "", [], {}, "Unavailable") for value in company.values()) or any(
            value not in (None, "", [], {}, "Unavailable") for value in market.values()
        )

    def _determine_run_status(
        self,
        company_profile: dict[str, Any],
        market_data: dict[str, Any],
        errors: list[PipelineErrorEvent],
        llm_status: str,
    ) -> str:
        if any(event.error_type in {"invalid_ticker", "invalid_or_delisted_ticker"} for event in errors):
            return "failed_invalid_ticker"
        has_company_data = any(
            company_profile.get(field)
            for field in ("company_name", "sector", "segment", "business_description")
        )
        has_market_data = any(
            market_data.get(field) is not None
            for field in ("current_price", "p_l", "roe", "net_margin", "dividend_yield", "ebitda")
        )
        if not has_company_data and not has_market_data:
            return "failed_source_unavailable"
        if errors or llm_status.startswith("failed"):
            return "partial_success"
        return "success"

    def _classify_llm_error(self, error: LLMGenerationError) -> str:
        message = str(error).lower()
        if "json" in message or "format" in message or error.raw_response:
            return "failed_invalid_format"
        return "failed_request"

    def _empty_company_profile(self) -> dict[str, Any]:
        return {
            "company_name": None,
            "sector": None,
            "segment": None,
            "business_description": None,
            "_sources": [],
        }

    def _empty_market_payload(self) -> dict[str, Any]:
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

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
