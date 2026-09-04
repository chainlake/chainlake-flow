# using root-level lockfile (single source of truth)
FROM python:3.11-slim AS builder

WORKDIR /app

# system deps: Kafka headers + C linker for Rust (maturin needs cc to link .so)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    curl \
    gcc \
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

# ---------- chainlake_avro: Rust PyO3 GIL-free Avro encoder ----------
# Rust is already available at /root/.cargo/bin (same prefix uv uses above).
# --profile minimal = rustc + cargo only; saves ~1 GB vs default profile.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path
COPY chainlake_avro /app/chainlake_avro
RUN uv pip install maturin --python /opt/venv/bin/python \
    && cd /app/chainlake_avro \
    && /opt/venv/bin/maturin build --release -o /tmp/wheels \
    && uv pip install /tmp/wheels/chainlake_avro-*.whl --python /opt/venv/bin/python \
    && rm -rf /tmp/wheels /app/chainlake_avro/target

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

CMD ["rpcstream"]