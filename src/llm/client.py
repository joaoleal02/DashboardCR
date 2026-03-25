from __future__ import annotations

import json
from typing import Any

from openai import OpenAI

from src.config import get_settings
from src.llm.prompts import SYSTEM_PROMPT, build_user_prompt
from src.llm.schemas import LLMReport


class LLMClient:
    """Thin OpenAI-compatible client for generating the structured briefing."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._client: OpenAI | None = None
        if self.settings.openai_api_key:
            self._client = OpenAI(
                api_key=self.settings.openai_api_key,
                base_url=self.settings.openai_base_url,
            )

    def is_configured(self) -> bool:
        return self._client is not None

    def generate_report(self, payload: dict[str, Any]) -> tuple[LLMReport, str]:
        if self._client is None:
            raise RuntimeError("OPENAI_API_KEY is not configured. Add it to your .env file to enable the LLM report.")

        response = self._client.chat.completions.create(
            model=self.settings.openai_model,
            temperature=0.2,
            max_completion_tokens=900,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_user_prompt(payload)},
            ],
        )
        content = response.choices[0].message.content or ""
        parsed = self._parse_json_response(content)
        report = LLMReport.from_dict(parsed)
        if not report.is_valid():
            raise ValueError("The LLM returned JSON, but it did not match the expected structure well enough for rendering.")
        return report, content

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
