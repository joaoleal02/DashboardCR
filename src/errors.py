from __future__ import annotations


class AppError(RuntimeError):
    """Base error for domain-specific failures."""


class SourceUnavailableError(AppError):
    def __init__(self, source: str, message: str, details: str | None = None) -> None:
        super().__init__(message)
        self.source = source
        self.details = details


class InvalidTickerError(AppError):
    def __init__(self, ticker: str, message: str | None = None) -> None:
        detail = message or f"{ticker} was not found in the B3 listed companies feed and may be invalid or delisted."
        super().__init__(detail)
        self.ticker = ticker
