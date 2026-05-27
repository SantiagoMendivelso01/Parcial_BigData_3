"""
validators.py — Validación de esquema para eventos ShopStream.
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

SCHEMAS = {
    "page_view": {
        "required": ["user_id", "session_id", "page_url", "page_type",
                     "timestamp", "time_on_page_seconds", "device_type", "country"],
        "types": {
            "user_id": str, "session_id": str, "page_url": str, "page_type": str,
            "timestamp": str, "time_on_page_seconds": (int, float),
            "device_type": str, "country": str,
        },
        "enums": {
            "page_type": {"home", "category", "product", "cart", "checkout", "search", "other"},
            "device_type": {"mobile", "desktop", "tablet"},
        },
        "ranges": {
            "time_on_page_seconds": (1, 7200),
        },
    },
    "click": {
        "required": ["user_id", "session_id", "element_id", "element_type",
                     "page_url", "timestamp", "x_position", "y_position"],
        "types": {
            "user_id": str, "session_id": str, "element_id": str, "element_type": str,
            "page_url": str, "timestamp": str,
            "x_position": (int, float), "y_position": (int, float),
        },
        "enums": {
            "element_type": {"button", "link", "image", "product_card",
                             "menu", "banner", "search_result"},
        },
        "ranges": {
            "x_position": (0, 7680),
            "y_position": (0, 4320),
        },
    },
    "search": {
        "required": ["user_id", "session_id", "query", "results_count", "timestamp"],
        "types": {
            "user_id": str, "session_id": str, "query": str,
            "results_count": (int, float), "timestamp": str,
        },
        "ranges": {
            "results_count": (0, 1_000_000),
        },
    },
    "product_view": {
        "required": ["user_id", "session_id", "product_id", "category",
                     "price", "timestamp", "time_on_page_seconds"],
        "types": {
            "user_id": str, "session_id": str, "product_id": str, "category": str,
            "price": (int, float), "timestamp": str,
            "time_on_page_seconds": (int, float),
        },
        "enums": {
            "category": {"electronics", "clothing", "home", "sports",
                         "books", "beauty", "toys", "food"},
        },
        "ranges": {
            "price": (0.01, 1_000_000),
            "time_on_page_seconds": (1, 7200),
        },
    },
    "cart_event": {
        "required": ["user_id", "session_id", "product_id", "action", "timestamp"],
        "types": {
            "user_id": str, "session_id": str, "product_id": str,
            "action": str, "timestamp": str,
        },
        "enums": {
            "action": {"add", "remove"},
        },
    },
}

ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)
NOW_UTC = datetime.now(timezone.utc)


@dataclass
class ValidationResult:
    valid_count: int = 0
    invalid_count: int = 0
    errors: list = field(default_factory=list)

    def add_error(self, record_index, record, reason):
        self.invalid_count += 1
        self.errors.append({
            "record_index": record_index,
            "user_id": record.get("user_id", "unknown"),
            "session_id": record.get("session_id", "unknown"),
            "reason": reason,
        })

    def add_valid(self):
        self.valid_count += 1


def validate_timestamp(ts: Any):
    if not isinstance(ts, str):
        return f"timestamp debe ser string, recibido {type(ts).__name__}"
    if not ISO8601_RE.match(ts):
        return f"timestamp no tiene formato ISO8601: '{ts}'"
    return None


def validate_record(record: dict, event_type: str):
    if event_type not in SCHEMAS:
        return [f"Tipo de evento desconocido: '{event_type}'"]

    schema = SCHEMAS[event_type]
    errors = []

    # 1. Campos obligatorios
    for field_name in schema["required"]:
        if field_name not in record:
            errors.append(f"Campo obligatorio faltante: '{field_name}'")
        elif record[field_name] is None:
            errors.append(f"Campo obligatorio es null: '{field_name}'")

    if errors:
        return errors

    # 2. Tipos de datos
    for field_name, expected_type in schema.get("types", {}).items():
        if field_name not in record:
            continue
        val = record[field_name]
        if not isinstance(val, expected_type):
            expected_name = (
                " o ".join(t.__name__ for t in expected_type)
                if isinstance(expected_type, tuple)
                else expected_type.__name__
            )
            errors.append(f"'{field_name}' debe ser {expected_name}, recibido {type(val).__name__}")

    # 3. Enumeraciones
    for field_name, valid_values in schema.get("enums", {}).items():
        if field_name in record and record[field_name] not in valid_values:
            errors.append(f"'{field_name}' valor invalido: '{record[field_name]}'")

    # 4. Rangos
    for field_name, (lo, hi) in schema.get("ranges", {}).items():
        if field_name in record:
            val = record[field_name]
            if isinstance(val, (int, float)) and not (lo <= val <= hi):
                errors.append(f"'{field_name}' fuera de rango [{lo}, {hi}]: {val}")

    # 5. Timestamp
    if "timestamp" in record:
        ts_error = validate_timestamp(record["timestamp"])
        if ts_error:
            errors.append(ts_error)

    return errors


def validate_records(records: list, event_type: str) -> ValidationResult:
    result = ValidationResult()
    for i, record in enumerate(records):
        errors = validate_record(record, event_type)
        if errors:
            for err in errors:
                result.add_error(i, record, err)
        else:
            result.add_valid()
    return result
