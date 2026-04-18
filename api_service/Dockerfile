FROM python:3.12-slim

WORKDIR /app/api_service

# Install uv
RUN pip install uv

# Copy pyproject.toml first for layer caching
COPY pyproject.toml .
RUN uv pip install --system -r pyproject.toml

# Copy the entire src/ directory as a single unit (proper Python package at /app/api_service/src/)
COPY src/ ./src/
COPY alembic.ini .
COPY alembic_migrations/ ./alembic_migrations/

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
