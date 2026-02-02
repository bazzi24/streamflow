FROM ubuntu:24.04 AS base
USER root
SHELL ["/bin/bash", "-c"]

ARG NEED_MIRROR=0

WORKDIR /streamflow

###
#
# UPDATE LATER
#
###



# Install uv
RUN --mount=type=bind,from=infiniflow/ragflow_deps:latest,source=/,target=/deps \
    if [ "$NEED_MIRROR" == "1" ]; then \
        mkdir -p /etc/uv && \
        echo 'python-install-mirror = "https://registry.npmmirror.com/-/binary/python-build-standalone/"' > /etc/uv/uv.toml && \
        echo '[[index]]' >> /etc/uv/uv.toml && \
        echo 'url = "https://pypi.tuna.tsinghua.edu.cn/simple"' >> /etc/uv/uv.toml && \
        echo 'default = true' >> /etc/uv/uv.toml; \
    fi; \
    arch="$(uname -m)"; \
    if [ "$arch" = "x86_64" ]; then uv_arch="x86_64"; else uv_arch="aarch64"; fi; \
    tar xzf "/deps/uv-${uv_arch}-unknown-linux-gnu.tar.gz" \
    && cp "uv-${uv_arch}-unknown-linux-gnu/"* /usr/local/bin/ \
    && rm -rf "uv-${uv_arch}-unknown-linux-gnu" \
    && uv python install 3.12

ENV PYTHONDONTWRITEBYTECODE=1 DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
ENV PATH=/root/.local/bin:$PATH

# Builder stage
FROM base AS builder 
USER root

WORKDIR /streamflow

# Install dependencies from uv.lock file
COPY pyproject.toml uv.lock ./

# production stage
FROM base AS production
USER root

WORKDIR /streamflow

# Copy Python environment and packages
ENV VIRTUAL_ENV=/streamflow/.venv
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

ENV PYTHONPATH=/streamflow/

COPY api api
COPY common common
COPY consumer consumer
COPY data data
COPY dataSSI dataSSI
COPY etl etl
COPY kafkaStream kafkaStream
COPY orchestration orchestration
COPY pyproject.toml uv.lock ./

COPY docker/entrypoint.sh ./
RUN chmod +x ./entrypoint*.sh

ENTRYPOINT [ "./entrypoint.sh" ]

