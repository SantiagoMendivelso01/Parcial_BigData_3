"""Tests para validators.py"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from validators import validate_record, validate_records, validate_timestamp

VALID_PAGE_VIEW = {
    "event_type": "page_view",
    "user_id": "usr-abc123",
    "session_id": "sess-xyz456",
    "page_url": "/producto/PROD-00001",
    "page_type": "product",
    "timestamp": "2024-01-15T10:30:00+00:00",
    "time_on_page_seconds": 45,
    "referrer": "google.com",
    "device_type": "mobile",
    "country": "CO",
}

VALID_CLICK = {
    "event_type": "click",
    "user_id": "usr-abc123",
    "session_id": "sess-xyz456",
    "element_id": "elem-deadbeef",
    "element_type": "button",
    "page_url": "/carrito",
    "timestamp": "2024-01-15T10:31:00+00:00",
    "x_position": 640,
    "y_position": 480,
}

VALID_CART_EVENT = {
    "event_type": "cart_event",
    "user_id": "usr-abc123",
    "session_id": "sess-xyz456",
    "product_id": "PROD-00001",
    "action": "add",
    "timestamp": "2024-01-15T10:34:00+00:00",
}

VALID_PRODUCT_VIEW = {
    "event_type": "product_view",
    "user_id": "usr-abc123",
    "session_id": "sess-xyz456",
    "product_id": "PROD-00001",
    "category": "electronics",
    "price": 299.99,
    "timestamp": "2024-01-15T10:33:00+00:00",
    "time_on_page_seconds": 120,
}


class TestValidRecords:
    def test_page_view_valid(self):
        assert validate_record(VALID_PAGE_VIEW, "page_view") == []

    def test_click_valid(self):
        assert validate_record(VALID_CLICK, "click") == []

    def test_cart_event_valid(self):
        assert validate_record(VALID_CART_EVENT, "cart_event") == []

    def test_product_view_valid(self):
        assert validate_record(VALID_PRODUCT_VIEW, "product_view") == []


class TestMissingFields:
    def test_missing_user_id(self):
        record = {**VALID_PAGE_VIEW}
        del record["user_id"]
        errors = validate_record(record, "page_view")
        assert any("user_id" in e for e in errors)

    def test_null_field(self):
        record = {**VALID_PAGE_VIEW, "session_id": None}
        errors = validate_record(record, "page_view")
        assert any("session_id" in e for e in errors)


class TestEnums:
    def test_invalid_page_type(self):
        record = {**VALID_PAGE_VIEW, "page_type": "blog"}
        errors = validate_record(record, "page_view")
        assert any("page_type" in e for e in errors)

    def test_invalid_device_type(self):
        record = {**VALID_PAGE_VIEW, "device_type": "smartwatch"}
        errors = validate_record(record, "page_view")
        assert any("device_type" in e for e in errors)

    def test_invalid_cart_action(self):
        record = {**VALID_CART_EVENT, "action": "update"}
        errors = validate_record(record, "cart_event")
        assert any("action" in e for e in errors)


class TestRanges:
    def test_time_on_page_zero(self):
        record = {**VALID_PAGE_VIEW, "time_on_page_seconds": 0}
        errors = validate_record(record, "page_view")
        assert any("time_on_page_seconds" in e for e in errors)

    def test_price_zero(self):
        record = {**VALID_PRODUCT_VIEW, "price": 0}
        errors = validate_record(record, "product_view")
        assert any("price" in e for e in errors)


class TestTimestamp:
    def test_valid_timestamp(self):
        assert validate_timestamp("2024-01-15T10:30:00+00:00") is None

    def test_invalid_format(self):
        assert validate_timestamp("2024-01-15") is not None

    def test_invalid_type(self):
        assert validate_timestamp(1705312200) is not None


class TestValidateRecords:
    def test_all_valid(self):
        records = [VALID_PAGE_VIEW, VALID_PAGE_VIEW]
        result = validate_records(records, "page_view")
        assert result.valid_count == 2
        assert result.invalid_count == 0

    def test_mixed(self):
        bad = {**VALID_PAGE_VIEW, "device_type": "smartwatch"}
        records = [VALID_PAGE_VIEW, bad, VALID_PAGE_VIEW]
        result = validate_records(records, "page_view")
        assert result.valid_count == 2
        assert result.invalid_count == 1

    def test_unknown_event_type(self):
        result = validate_records([VALID_PAGE_VIEW], "purchase")
        assert result.invalid_count == 1
