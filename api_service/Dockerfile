FROM python:3.12-slim

# Create non-root user
RUN groupadd -r app && useradd -r -g app -s /bin/bash app \
    && mkdir -p /app/api_service \
    && chown -R app:app /app

WORKDIR /app/api_service

# Install uv
RUN pip install uv

# Copy pyproject.toml first for layer caching
COPY pyproject.toml .

# Switch to non-root user for dependency installation
USER app

RUN uv pip install --system -r pyproject.toml

# Copy the entire src/ directory as a single unit (proper Python package at /app/api_service/src/)
COPY --chown=app:app src/ ./src/
COPY --chown=app:app alembic.ini .
COPY --chown=app:app alembic_migrations/ ./alembic_migrations/

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]