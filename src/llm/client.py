from __future__ import annotations

import json
from typing import Any

from src.config import get_settings
from src.llm.prompts import SYSTEM_PROMPT, build_user_prompt
from src.llm.schemas import LLMReport


class LLMGenerationError(RuntimeError):
    def __init__(self, message: str, raw_response: str | None = None) -> None:
        super().__init__(message)
        self.raw_response = raw_response


class LLMClient:
    """Thin OpenAI-compatible client for generating the structured briefing."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._client: Any | None = None
        self._client_error: str | None = None
        if self.settings.openai_api_key:
            try:
                from openai import OpenAI
            except ModuleNotFoundError:
                self._client = None
                self._client_error = "The `openai` package is not installed. Run `pip install -r requirements.txt`."
            else:
                self._client = OpenAI(
                    api_key=self.settings.openai_api_key,
                    base_url=self.settings.openai_base_url,
                )

    def is_configured(self) -> bool:
        return self._client is not None

    def generate_report(self, payload: dict[str, Any]) -> tuple[LLMReport, str]:
        if self._client is None:
            if self._client_error:
                raise RuntimeError(self._client_error)
            raise RuntimeError("OPENAI_API_KEY is not configured. Add it to your .env file to enable the LLM report.")

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(payload)},
        ]

        response, last_error = self._create_completion(messages, use_json_schema=True)
        if response is None:
            response, last_error = self._create_completion(messages, use_json_schema=False)
        if response is None:
            raise LLMGenerationError(
                f"The LLM request failed before a response body could be parsed. {last_error}".strip()
            )

        message = response.choices[0].message
        content = message.content or ""
        refusal = getattr(message, "refusal", None)
        if refusal:
            raise LLMGenerationError(f"The LLM refused to answer: {refusal}", raw_response=content)

        try:
            parsed = self._parse_json_response(content)
            report = LLMReport.from_dict(parsed)
        except Exception as exc:
            raise LLMGenerationError(str(exc), raw_response=content) from exc

        if not report.is_valid():
            raise LLMGenerationError(
                "The LLM returned JSON, but it did not match the expected structure well enough for rendering.",
                raw_response=content,
            )
        return report, content

    def _create_completion(self, messages: list[dict[str, str]], use_json_schema: bool) -> tuple[Any | None, str | None]:
        request_kwargs: dict[str, Any] = {
            "model": self.settings.openai_model,
            "temperature": 0.2,
            "max_completion_tokens": 900,
            "messages": messages,
        }
        if use_json_schema:
            request_kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "equity_briefing",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "business_summary": {"type": "string"},
                            "fundamentals_interpretation": {"type": "string"},
                            "news_analysis": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "overall": {"type": "string"},
                                    "items": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "additionalProperties": False,
                                            "properties": {
                                                "title": {"type": "string"},
                                                "sentiment": {
                                                    "type": "string",
                                                    "enum": ["positive", "negative", "neutral"],
                                                },
                                                "rationale": {"type": "string"},
                                            },
                                            "required": ["title", "sentiment", "rationale"],
                                        },
                                    },
                                },
                                "required": ["overall", "items"],
                            },
                            "analyst_questions": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                        "required": [
                            "business_summary",
                            "fundamentals_interpretation",
                            "news_analysis",
                            "analyst_questions",
                        ],
                    },
                },
            }
        else:
            request_kwargs["response_format"] = {"type": "json_object"}

        try:
            return self._client.chat.completions.create(**request_kwargs), None
        except Exception as exc:
            return None, str(exc)

    def _parse_json_response(self, content: str) -> dict[str, Any]:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise ValueError("The LLM response was not valid JSON.")
            try:
                return json.loads(content[start : end + 1])
            except json.JSONDecodeError as exc:
                raise ValueError("The LLM response could not be parsed into JSON.") from exc
