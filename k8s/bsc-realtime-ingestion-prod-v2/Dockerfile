# Reuse chainlake-connector (python 3.12, uv, librdkafka1 already installed)
# as the base — it pulls from ghcr.io which is reachable from clh001, while
# docker.io is not. The connector is already chainlake-published so it has
# no provenance concerns.
FROM ghcr.io/chainlake/chainlake-connector:8931137 AS builder

WORKDIR /app

# Install uv via aliyun pypi mirror (pypi.org is unreachable from the home
# network — clh001 sits on residential联通 which can only reach pypi via the
# aliyun mirror).
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ uv
ENV PATH="/usr/local/bin:$PATH"

COPY pyproject.toml uv.lock ./
RUN uv venv /opt/venv && \
    UV_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
    uv pip install --python /opt/venv/bin/python .

# ---------------- runtime ----------------
FROM ghcr.io/chainlake/chainlake-connector:8931137

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH=/app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY rpcstream /app/rpcstream

CMD ["rpcstream"]