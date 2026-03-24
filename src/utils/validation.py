from __future__ import annotations

from src.config import get_settings
from src.ticker_universe import ALLOWED_TICKERS


def normalize_ticker(value: str) -> str:
    return value.strip().upper().replace(".SA", "")


def validate_ticker(value: str) -> tuple[bool, str | None]:
    if not value:
        return False, "Please choose or type one ticker from the allowed Phase 1 universe."
    if value not in ALLOWED_TICKERS:
        allowed = ", ".join(ALLOWED_TICKERS)
        return False, f"Unsupported ticker for Phase 1. Allowed values: {allowed}."
    return True, None


def to_yahoo_symbol(ticker: str) -> str:
    settings = get_settings()
    normalized = normalize_ticker(ticker)
    return f"{normalized}{settings.yahoo_suffix}"
