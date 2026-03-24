from __future__ import annotations

import json
from typing import Any

from src.utils.formatting import format_metric_value, safe_text


SYSTEM_PROMPT = """
You are a buy-side equity research assistant preparing a concise Monday morning briefing.

Follow these rules:
- Think like a value-oriented analyst.
- Prioritize business quality, downside protection, capital structure, profitability quality, and what could go wrong.
- Distinguish factual inputs from analytical inference.
- Never fabricate facts that are not present in the input.
- If a field is unavailable, acknowledge the gap and reason around it instead of inventing data.
- Avoid generic finance filler and motivational language.
- Be concise, specific, and useful.
- Classify each news item as positive, negative, or neutral for the company, based on likely fundamental impact.

Return valid JSON only with this exact structure:
{
  "business_summary": "string",
  "fundamentals_interpretation": "string",
  "news_analysis": {
    "overall": "string",
    "items": [
      {
        "title": "string",
        "sentiment": "positive | negative | neutral",
        "rationale": "string"
      }
    ]
  },
  "analyst_questions": ["string", "string", "string"]
}
""".strip()


def build_user_prompt(payload: dict[str, Any]) -> str:
    company = payload.get("company_profile", {})
    market = payload.get("market_data", {})
    news = payload.get("news", [])

    formatted_payload = {
        "ticker": payload.get("ticker"),
        "company_profile": {
            "company_name": safe_text(company.get("company_name")),
            "sector": safe_text(company.get("sector")),
            "segment": safe_text(company.get("segment")),
            "business_description": safe_text(company.get("business_description")),
        },
        "market_data": {
            "current_price": format_metric_value("current_price", market.get("current_price")),
            "p_l": format_metric_value("p_l", market.get("p_l")),
            "roe": format_metric_value("roe", market.get("roe")),
            "net_debt_ebitda": format_metric_value("net_debt_ebitda", market.get("net_debt_ebitda")),
            "net_margin": format_metric_value("net_margin", market.get("net_margin")),
            "dividend_yield": format_metric_value("dividend_yield", market.get("dividend_yield")),
        },
        "recent_news": [
            {
                "title": safe_text(item.get("title")),
                "source": safe_text(item.get("source")),
                "date": safe_text(item.get("date")),
                "url": safe_text(item.get("url")),
            }
            for item in news
        ],
    }

    return (
        "Prepare a concise Phase 1 research briefing for the following company. "
        "Use only the facts in the payload below, and keep inference grounded.\n\n"
        f"{json.dumps(formatted_payload, ensure_ascii=False, indent=2)}"
    )
