FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy pyproject.toml first for layer caching
COPY pyproject.toml .
RUN uv pip install --system -r pyproject.toml

# Copy the entire src/ directory as a single unit (proper Python package at /app/src/)
COPY src/ ./src/
COPY migrations/ ./migrations/

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
