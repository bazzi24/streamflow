FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml ./
RUN uv pip install --system -r pyproject.toml

COPY dataSSI/load_index_components.py ./

# All sensitive values (MYSQL_HOST, MYSQL_PORT, MYSQL_ROOT_PASSWORD,
# consumerID, consumerSecret) are sourced from .env via env_file in
# docker-compose.yaml — NOT hardcoded here.

CMD ["python", "load_index_components.py"]
