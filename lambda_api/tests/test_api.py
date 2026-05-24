"""
Tests para la API Flask usando mocks de la BD.
"""

import sys
from pathlib import Path
from unittest.mock import patch
import json
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import app as flask_app


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as c:
        yield c


MOCK_TOP_PAGES = [
    {"page_url": "/checkout", "page_type": "checkout",
     "avg_time_seconds": 279.71, "total_visits": 41},
    {"page_url": "/producto/PROD-00001", "page_type": "product",
     "avg_time_seconds": 200.0, "total_visits": 10},
]

MOCK_BOUNCE = [
    {"page_type": "home", "total_sessions": 313,
     "bounce_sessions": 233, "bounce_rate_pct": 74.44},
]

MOCK_SESSIONS = [
    {"device_type": "mobile", "country": "CO",
     "avg_time_seconds": 120.5, "total_sessions": 3000, "unique_users": 2100},
]

MOCK_ANOMALIES = [
    {"session_id": "sess-abc", "user_id": "usr-xyz",
     "page_url": "/checkout", "page_type": "checkout",
     "z_score": 4.52, "anomaly_type": "zscore_time_on_page_seconds",
     "anomaly_field": "time_on_page_seconds", "anomaly_value": 3500.0,
     "detected_at": "2024-01-15T02:30:00"},
]


class TestHealth:
    def test_health_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert json.loads(r.data)["status"] == "ok"


class TestPagesTop:
    @patch("routes.pages.query_to_list", return_value=MOCK_TOP_PAGES)
    def test_time_on_page_default(self, mock_q, client):
        r = client.get("/pages/top?date=2024-01-15")
        assert r.status_code == 200
        body = json.loads(r.data)
        assert body["metric"] == "time_on_page"
        assert body["count"] == 2

    @patch("routes.pages.query_to_list", return_value=MOCK_BOUNCE)
    def test_bounce_rate_metric(self, mock_q, client):
        r = client.get("/pages/top?metric=bounce_rate&date=2024-01-15")
        assert r.status_code == 200
        body = json.loads(r.data)
        assert body["metric"] == "bounce_rate"

    def test_invalid_metric(self, client):
        r = client.get("/pages/top?metric=clicks&date=2024-01-15")
        assert r.status_code == 400

    def test_invalid_date(self, client):
        r = client.get("/pages/top?date=hoy")
        assert r.status_code == 400

    def test_invalid_limit(self, client):
        r = client.get("/pages/top?date=2024-01-15&limit=abc")
        assert r.status_code == 400

    @patch("routes.pages.query_to_list", return_value=MOCK_TOP_PAGES)
    def test_limit_capped_at_100(self, mock_q, client):
        r = client.get("/pages/top?date=2024-01-15&limit=999")
        assert r.status_code == 200
        call_args = mock_q.call_args[0]
        assert call_args[1][1] == 100


class TestSessionsSummary:
    @patch("routes.sessions.query_to_list", return_value=MOCK_SESSIONS)
    def test_basic_query(self, mock_q, client):
        r = client.get("/sessions/summary?date=2024-01-15&country=CO")
        assert r.status_code == 200
        body = json.loads(r.data)
        assert body["total_sessions"] == 3000

    @patch("routes.sessions.query_to_list", return_value=MOCK_SESSIONS)
    def test_device_filter(self, mock_q, client):
        r = client.get("/sessions/summary?date=2024-01-15&device=mobile")
        assert r.status_code == 200

    def test_invalid_device(self, client):
        r = client.get("/sessions/summary?date=2024-01-15&device=smartwatch")
        assert r.status_code == 400

    def test_invalid_date(self, client):
        r = client.get("/sessions/summary?date=15-01-2024")
        assert r.status_code == 400

    @patch("routes.sessions.query_to_list", return_value=[])
    def test_no_results(self, mock_q, client):
        r = client.get("/sessions/summary?date=2099-01-01")
        assert r.status_code == 200
        body = json.loads(r.data)
        assert body["total_sessions"] == 0


class TestAnomalies:
    @patch("routes.anomalies.query_to_list", return_value=MOCK_ANOMALIES)
    def test_basic_query(self, mock_q, client):
        r = client.get("/anomalies?date=2024-01-15")
        assert r.status_code == 200
        body = json.loads(r.data)
        assert body["total"] == 1

    @patch("routes.anomalies.query_to_list", return_value=MOCK_ANOMALIES)
    def test_type_filter(self, mock_q, client):
        r = client.get("/anomalies?date=2024-01-15&type=zscore_time_on_page_seconds")
        assert r.status_code == 200

    @patch("routes.anomalies.query_to_list", return_value=MOCK_ANOMALIES)
    def test_min_zscore_filter(self, mock_q, client):
        r = client.get("/anomalies?date=2024-01-15&min_zscore=3.0")
        assert r.status_code == 200

    def test_invalid_date(self, client):
        r = client.get("/anomalies?date=bad-date")
        assert r.status_code == 400

    def test_invalid_min_zscore(self, client):
        r = client.get("/anomalies?date=2024-01-15&min_zscore=abc")
        assert r.status_code == 400

    @patch("routes.anomalies.query_to_list", return_value=[])
    def test_empty_day(self, mock_q, client):
        r = client.get("/anomalies?date=2024-01-15")
        assert r.status_code == 200
        body = json.loads(r.data)
        assert body["total"] == 0
        assert body["by_type"] == {}
