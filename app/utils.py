from __future__ import annotations

from decimal import Decimal, InvalidOperation


def format_price(value: object) -> str:
    if value is None:
        return ""
    try:
        normalized = f"{Decimal(str(value)):.2f}"
    except (InvalidOperation, ValueError):
        return str(value)
    return normalized.rstrip("0").rstrip(".")
