from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def safe_text(value: Any) -> str:
    if value is None:
        return "Unavailable"
    text = str(value).strip()
    return text if text else "Unavailable"


def format_currency_brl(value: Any) -> str:
    if value is None:
        return "Unavailable"
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return safe_text(value)


def format_number(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "Unavailable"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return safe_text(value)


def format_percent(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "Unavailable"
    try:
        return f"{float(value):.{decimals}f}%"
    except (TypeError, ValueError):
        return safe_text(value)


def format_metric_value(field: str, value: Any) -> str:
    if field == "current_price":
        return format_currency_brl(value)
    if field in {"roe", "net_margin", "dividend_yield"}:
        return format_percent(value)
    return format_number(value)


def compact_date(value: Any) -> str:
    text = safe_text(value)
    if text == "Unavailable":
        return text
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return text


def normalize_title_key(value: str) -> str:
    return " ".join(value.lower().split())


def escape_streamlit_text(value: Any) -> str:
    text = safe_text(value)
    if text == "Unavailable":
        return text
    return text.replace("\\", "\\\\").replace("$", "\\$")


def to_pretty_json(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)
