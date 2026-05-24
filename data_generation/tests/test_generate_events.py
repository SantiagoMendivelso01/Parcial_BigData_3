"""Tests para generate_events.py"""

import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from generate_events import (
    gen_page_view, gen_click, gen_search,
    gen_product_view, gen_cart_event,
    generate_session, generate_day,
)

DATE     = datetime(2024, 1, 15)
USER_ID  = "usr-testuser123"
SESSION_ID = "sess-testsess123"


class TestPageView:
    def test_required_fields(self):
        ev = gen_page_view(USER_ID, SESSION_ID, DATE)
        for field in ["event_type","user_id","session_id","page_url",
                      "page_type","timestamp","time_on_page_seconds",
                      "device_type","country"]:
            assert field in ev

    def test_event_type(self):
        assert gen_page_view(USER_ID, SESSION_ID, DATE)["event_type"] == "page_view"

    def test_time_positive(self):
        for _ in range(20):
            assert gen_page_view(USER_ID, SESSION_ID, DATE)["time_on_page_seconds"] >= 1

    def test_device_valid(self):
        valid = {"mobile","desktop","tablet"}
        for _ in range(20):
            assert gen_page_view(USER_ID, SESSION_ID, DATE)["device_type"] in valid

    def test_page_type_valid(self):
        valid = {"home","category","product","cart","checkout","search","other"}
        for _ in range(20):
            assert gen_page_view(USER_ID, SESSION_ID, DATE)["page_type"] in valid


class TestClick:
    def test_required_fields(self):
        ev = gen_click(USER_ID, SESSION_ID, "/home", DATE)
        for field in ["event_type","user_id","session_id","element_id",
                      "element_type","page_url","timestamp","x_position","y_position"]:
            assert field in ev

    def test_positions_non_negative(self):
        for _ in range(20):
            ev = gen_click(USER_ID, SESSION_ID, "/home", DATE)
            assert ev["x_position"] >= 0
            assert ev["y_position"] >= 0


class TestSearch:
    def test_required_fields(self):
        ev = gen_search(USER_ID, SESSION_ID, DATE)
        for field in ["event_type","user_id","session_id","query","results_count","timestamp"]:
            assert field in ev

    def test_results_non_negative(self):
        for _ in range(20):
            assert gen_search(USER_ID, SESSION_ID, DATE)["results_count"] >= 0


class TestProductView:
    def test_required_fields(self):
        ev = gen_product_view(USER_ID, SESSION_ID, DATE)
        for field in ["event_type","user_id","session_id","product_id",
                      "category","price","timestamp","time_on_page_seconds"]:
            assert field in ev

    def test_price_positive(self):
        for _ in range(20):
            assert gen_product_view(USER_ID, SESSION_ID, DATE)["price"] > 0


class TestCartEvent:
    def test_required_fields(self):
        ev = gen_cart_event(USER_ID, SESSION_ID, DATE)
        for field in ["event_type","user_id","session_id","product_id","action","timestamp"]:
            assert field in ev

    def test_action_valid(self):
        for _ in range(20):
            assert gen_cart_event(USER_ID, SESSION_ID, DATE)["action"] in {"add","remove"}


class TestSession:
    def test_has_page_view(self):
        events = generate_session(USER_ID, DATE)
        assert any(e["event_type"] == "page_view" for e in events)

    def test_same_session_id(self):
        events = generate_session(USER_ID, DATE)
        assert len({e["session_id"] for e in events}) == 1


class TestGenerateDay:
    def test_generates_minimum_records(self):
        events = generate_day(DATE, n_records=500)
        assert len(events) == 500

    def test_all_event_types_present(self):
        events = generate_day(DATE, n_records=2000)
        types = {e["event_type"] for e in events}
        assert {"page_view","click","search","product_view","cart_event"}.issubset(types)

    def test_serializable_to_json(self):
        for ev in generate_day(DATE, n_records=100):
            json.dumps(ev)
