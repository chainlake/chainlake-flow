# using root-level lockfile (single source of truth)
FROM python:3.11-slim AS builder

WORKDIR /app

# system deps (Kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:/root/.local/bin:$PATH"

# copy only dependency files first (for caching)
COPY pyproject.toml uv.lock ./

# create venv (isolated build stage)
RUN uv venv /opt/venv

# install INTO venv explicitly
RUN uv pip install --python /opt/venv/bin/python .

# ---------------- runtime ----------------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH=/app

WORKDIR /app

# runtime Kafka lib only
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# copy ONLY final env (no pip install in runtime)
COPY --from=builder /opt/venv /opt/venv

# copy application code last (max cache efficiency)
COPY rpcstream /app/rpcstream
COPY scripts/ /app/scripts/

WORKDIR /app/scripts

CMD ["python", "block_pipeline_realtime.py"]