FROM python:3.12-slim

# Create non-root user
RUN groupadd -r app && useradd -r -g app -s /bin/bash app \
    && mkdir -p /app \
    && chown -R app:app /app

WORKDIR /app

RUN pip install uv

COPY pyproject.toml ./

USER app

RUN uv pip install --system -r pyproject.toml

COPY --chown=app:app dataSSI/load_index_components.py ./

# All sensitive values (MYSQL_HOST, MYSQL_PORT, MYSQL_ROOT_PASSWORD,
# consumerID, consumerSecret) are sourced from .env via env_file in
# docker-compose.yaml — NOT hardcoded here.

CMD ["python", "load_index_components.py"]