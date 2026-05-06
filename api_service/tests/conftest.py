"""Pytest configuration and fixtures for StreamFlow API tests."""
import pytest
import sys
import os

# Set test environment variables BEFORE importing the app
os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("MYSQL_HOST", "localhost")
os.environ.setdefault("MYSQL_PORT", "3306")
os.environ.setdefault("RAW_DB_URL", "jdbc:mysql://localhost:3306/data?useSSL=false")
os.environ.setdefault("DW_DB_URL", "jdbc:mysql://localhost:3306/warehouse?useSSL=false")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing-only")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "1440")
os.environ.setdefault("CORS_ORIGINS", "http://localhost:3000")

# Add api_service and parent directory to Python path
api_service_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_dir = os.path.dirname(api_service_dir)
sys.path.insert(0, api_service_dir)
sys.path.insert(0, parent_dir)


@pytest.fixture
def mock_websocket():
    """Create a mock WebSocket with async methods."""
    from unittest.mock import AsyncMock, MagicMock
    ws = MagicMock()
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.close = AsyncMock()
    return ws
