"""Pytest configuration and fixtures for StreamFlow API tests."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Adjust import path
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app
from models.data import Base as DataBase
from models.warehouse import Base as WarehouseBase
from models.api import Base as ApiBase


@pytest.fixture(scope="session")
def test_engine():
    """Create a test database engine (SQLite in-memory)."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool
    )
    # Create all tables
    DataBase.metadata.create_all(bind=engine)
    WarehouseBase.metadata.create_all(bind=engine)
    ApiBase.metadata.create_all(bind=engine)
    yield engine
    # Teardown
    DataBase.metadata.drop_all(bind=engine)
    WarehouseBase.metadata.drop_all(bind=engine)
    ApiBase.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def test_db_session(test_engine):
    """Create a fresh database session for each test."""
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture(scope="function")
def client(test_db_session):
    """Create a test client with dependency overrides."""
    from api.v1 import router as api_router
    from database import get_streaming_db, get_warehouse_db

    def override_streaming_db():
        try:
            yield test_db_session
        finally:
            pass

    def override_warehouse_db():
        try:
            yield test_db_session
        finally:
            pass

    app.dependency_overrides[get_streaming_db] = override_streaming_db
    app.dependency_overrides[get_warehouse_db] = override_warehouse_db

    with pytest.TestingApp(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


@pytest.fixture
def mock_kafka(mocker):
    """Mock Kafka producer/consumer for tests."""
    from websocket.bridge import kafka_bridge_loop
    # Mock Kafka-related functionality
    return mocker.patch('websocket.bridge.KafkaProducer')
