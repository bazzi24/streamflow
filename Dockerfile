FROM ubuntu:24.04 AS base
USER root
SHELL ["/bin/bash", "-c"]

ARG NEED_MIRROR=0

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r streamflow && useradd -r -g streamflow -s /bin/bash streamflow \
    && mkdir -p /streamflow \
    && chown -R streamflow:streamflow /streamflow

# Install uv from the official multi-stage image (no external script needed)
COPY --from=ghcr.io/astral-sh/uv:latest /uv  /usr/local/bin/uv
COPY --from=ghcr.io/astral-sh/uv:latest /uvx /usr/local/bin/uvx

RUN if [ "$NEED_MIRROR" = "1" ]; then \
        mkdir -p /etc/uv && \
        echo 'python-install-mirror = "https://registry.npmmirror.com/-/binary/python-build-standalone/"' > /etc/uv/uv.toml && \
        echo '[[index]]' >> /etc/uv/uv.toml && \
        echo 'url = "https://pypi.tuna.tsinghua.edu.cn/simple"' >> /etc/uv/uv.toml && \
        echo 'default = true' >> /etc/uv/uv.toml; \
    fi

RUN uv python install 3.12

ENV PATH="/root/.local/bin:$PATH"

# ============================================================
# Builder — install Python dependencies
# ============================================================
FROM base AS builder
USER root
WORKDIR /streamflow

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

# ============================================================
# Production
# ============================================================
FROM base AS production
# Switch to non-root user
USER streamflow
WORKDIR /streamflow

ENV VIRTUAL_ENV=/streamflow/.venv
COPY --from=builder /streamflow/.venv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONPATH=/streamflow

# Download MySQL JDBC connector for Spark ETL jobs
RUN wget -q \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
    -O /tmp/mysql-connector-j-8.0.33.jar

# Copy application source
COPY api_service api_service
COPY common      common
COPY consumer    consumer
COPY dataSSI     dataSSI
COPY etl         etl
COPY kafkaStream kafkaStream
COPY lib         lib
COPY pyproject.toml uv.lock ./

# Place the MySQL JDBC jar where Spark can find it
RUN cp /tmp/mysql-connector-j-8.0.33.jar lib/mysql-connector-j-8.0.33.jar \
    && rm /tmp/mysql-connector-j-8.0.33.jar

ENTRYPOINT ["python"]
