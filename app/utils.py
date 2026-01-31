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


def appointment_services_label(appt) -> str:
    comment = (getattr(appt, "admin_comment", None) or "").strip()
    if comment.lower().startswith("услуги:"):
        label = comment.split(":", 1)[1].strip()
        if label:
            return label
    service = getattr(appt, "service", None)
    if service and getattr(service, "name", None):
        return service.name
    return "Услуга"
