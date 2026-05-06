# Testing Guide

## Overview

StreamFlow uses pytest for backend unit and integration tests. The frontend currently has no automated tests (Jest/React Testing Library not configured).

## Test Structure

```
api_service/tests/
├── conftest.py          # Fixtures and test configuration
├── test_api_integration.py  # API endpoint tests
├── test_validators.py   # Input validation tests
└── test_decorator.py    # Retry decorator tests
```

## Running Tests

### Backend Tests

```bash
cd api_service
uv pip install -e ".[dev]"  # Install dev dependencies including pytest
pytest                    # Run all tests
pytest -v                 # Verbose output
pytest -k "test_validators"  # Run specific test file
pytest --cov=src          # With coverage report
```

### Integration Tests

Integration tests use SQLite in-memory database and mock Kafka:

```bash
pytest tests/test_api_integration.py -v
```

## Writing Tests

### Unit Tests

Test individual functions/classes in isolation:

```python
def test_validate_symbol():
    from schemas.validators import validate_symbol
    assert validate_symbol("VNM") == "VNM"
    with pytest.raises(ValueError):
        validate_symbol("invalid")
```

### Fixtures

Common fixtures in `conftest.py`:

- `test_engine`: SQLite in-memory engine with all tables created
- `test_db_session`: Fresh database session per test (rolls back after)
- `client`: TestClient with database dependency overrides

### Mocking External Dependencies

Use `pytest-mock` for mocking:

```python
def test_something(mocker):
    mocker.patch('some.module.function', return_value='mocked')
```

## Test Coverage

Current coverage areas:
- Input validation (validators.py)
- Retry decorator
- Health endpoint
- Stocks endpoint (structure)

Areas needing coverage:
- StockService methods (with mocked DB)
- Auth endpoints
- WebSocket manager
- Kafka bridge

## Manual Testing

### Using curl

```bash
# Health check
curl http://localhost:8000/health

# Get stocks (requires auth)
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/v1/stocks

# Get specific quote
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/v1/stocks/VNM
```

### Using Swagger UI

Visit http://localhost:8000/docs for interactive API testing.

## Load Testing

Recommended tool: `locust`

```bash
pip install locust
locust -f tests/load_test.py
```

## CI/CD Integration

Add to your CI pipeline:

```yaml
test:
  script:
    - cd api_service
    - uv pip install -e ".[dev]"
    - pytest --cov=src --cov-report=xml
```
