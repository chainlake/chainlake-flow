from opentelemetry import metrics

meter = metrics.get_meter("engine")

# Throughput
BLOCK_COUNTER = meter.create_counter(
    "engine_blocks_total",
)

ROW_COUNTER = meter.create_counter(
    "engine_rows_total",
)

DLQ_COUNTER = meter.create_counter(
    "engine_dlq_total",
)

# Latency
BLOCK_LATENCY = meter.create_histogram(
    "engine_block_latency_ms",
    unit="ms",
)

QUEUE_WAIT = meter.create_histogram(
    "engine_queue_wait_ms",
    unit="ms",
)

TOTAL_TIME = meter.create_histogram(
    "engine_total_time_ms",
    unit="ms",
)

# Load
INFLIGHT = meter.create_up_down_counter(
    "engine_inflight",
)

# Errors
ERROR_COUNTER = meter.create_counter(
    "engine_errors_total",
)

# lag
CHAIN_LAG = meter.create_histogram(
    name="engine_chain_lag",
    description="point-in-time lag at processing moment",
)

INGESTION_LAG = meter.create_histogram(
    name="engine_ingestion_lag",
    description="TRUE pipeline lag (monotonic)",
)