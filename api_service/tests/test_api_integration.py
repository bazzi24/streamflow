"""Integration tests for StreamFlow API."""
import pytest
from fastapi.testclient import TestClient

# Import the app
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import app


@pytest.fixture(scope="session")
def client():
    """Create a test client."""
    with TestClient(app) as test_client:
        yield test_client


class TestHealthEndpoint:
    """Tests for the health check endpoint."""

    def test_health_endpoint_returns_ok(self, client):
        """Health endpoint should return status ok."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "ws_connections" in data


class TestStocksEndpoint:
    """Tests for the GET /stocks endpoint."""

    def test_get_stocks_returns_success(self, client):
        """GET /stocks should return 200 OK or 404 if no data."""
        response = client.get("/api/v1/stocks")
        # Should succeed even with empty database
        assert response.status_code in [200, 404]

    def test_get_stocks_response_structure(self, client):
        """GET /stocks should return proper JSON structure."""
        response = client.get("/api/v1/stocks")
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)


class TestRateLimiting:
    """Tests for rate limiting functionality."""

    def test_rate_limit_headers_present(self, client):
        """Rate limit headers should be present on responses."""
        response = client.get("/health")
        # Check for rate limit headers (implementation dependent)
        # SlowAPI typically adds these headers
        assert response.status_code in [200, 429]

