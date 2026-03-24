from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class NewsSentiment:
    title: str
    sentiment: str
    rationale: str


@dataclass
class NewsAnalysis:
    overall: str
    items: list[NewsSentiment]


@dataclass
class LLMReport:
    business_summary: str
    fundamentals_interpretation: str
    news_analysis: NewsAnalysis
    analyst_questions: list[str]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LLMReport":
        news_payload = payload.get("news_analysis") or {}
        item_payloads = news_payload.get("items") or []
        items = [
            NewsSentiment(
                title=str(item.get("title", "")).strip(),
                sentiment=_normalize_sentiment(item.get("sentiment")),
                rationale=str(item.get("rationale", "")).strip(),
            )
            for item in item_payloads
            if item.get("title")
        ]
        questions = [
            str(question).strip()
            for question in (payload.get("analyst_questions") or [])
            if str(question).strip()
        ][:3]
        return cls(
            business_summary=str(payload.get("business_summary", "")).strip(),
            fundamentals_interpretation=str(payload.get("fundamentals_interpretation", "")).strip(),
            news_analysis=NewsAnalysis(
                overall=str(news_payload.get("overall", "")).strip(),
                items=items,
            ),
            analyst_questions=questions,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def is_valid(self) -> bool:
        return bool(
            self.business_summary
            and self.fundamentals_interpretation
            and self.news_analysis.overall
            and len(self.analyst_questions) == 3
        )


def _normalize_sentiment(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in {"positive", "negative", "neutral"}:
        return "neutral"
    return normalized
