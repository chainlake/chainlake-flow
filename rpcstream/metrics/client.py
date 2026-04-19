from opentelemetry import metrics

meter = metrics.get_meter("client")

REQUEST_COUNTER = meter.create_counter(
    name="rpc_requests_total",
    description="RPC requests by status",
)

REQUEST_SUBMITTED_COUNTER = meter.create_counter(
    name="rpc_requests_submitted_total",
)

INFLIGHT_GAUGE = meter.create_up_down_counter(
    name="rpc_inflight",
)

RETRY_COUNTER = meter.create_counter(
    name="rpc_retries_total",
)

LATENCY_HISTOGRAM = meter.create_histogram(
    name="rpc_latency_ms",
    unit="ms",
)