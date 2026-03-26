from __future__ import annotations

import re

def normalize_ticker(value: str) -> str:
    return value.strip().upper().replace(".SA", "")


def validate_ticker(value: str) -> tuple[bool, str | None]:
    if not value:
        return False, "Please enter a B3 ticker."
    if not re.fullmatch(r"[A-Z]{4,6}\d{1,2}", value):
        return False, "Ticker must look like a B3 trading code, for example PETR4, VALE3, ITUB4, or SANB11."
    return True, None
