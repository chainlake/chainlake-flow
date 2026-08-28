from typing import Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator
from rpcstream.runtime.observability.config import ObservabilityConfig


class KafkaAuth(BaseModel):
    username_env: Optional[str] = None
    password_env: Optional[str] = None


class KafkaSsl(BaseModel):
    ca_path_env: Optional[str] = None


class KafkaConnection(BaseModel):
    bootstrap_servers: str
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    auth: KafkaAuth = Field(default_factory=KafkaAuth)
    ssl: KafkaSsl = Field(default_factory=KafkaSsl)


class KafkaCommon(BaseModel):
    topic_template: Optional[str] = None


class KafkaProducer(BaseModel):
    linger_ms: int
    batch_size: int
    compression_type: str = "zstd"


class KafkaStreaming(BaseModel):
    batch_size: int = 100
    flush_interval_ms: int = 20
    # Sink buffer between the engine workers and the single sink worker. At
    # ~9 enqueues/s the old 100-item cap was only ~11s of slack, so a short
    # broker hiccup saturated it and every send started timing out.
    queue_maxsize: int = 400
    # How long a producer waits for room in the sink queue before the batch is
    # reported as failed. Was hardcoded at 0.1s, which turned sub-second
    # backpressure into failed cursors (-> sink cooldown -> circuit breaker trip).
    enqueue_timeout_ms: int = 2000


class KafkaSchemaRegistry(BaseModel):
    enabled: bool = True
    type: str = "avro"
    url: str = Field(
        default="http://localhost:30081",
        validation_alias=AliasChoices("url", "schema_registry_url"),
    )

    @model_validator(mode="after")
    def validate_schema_registry(self):
        schema_type = str(self.type).strip().lower()
        if schema_type not in {"avro", "protobuf"}:
            raise ValueError("kafka.schemaRegistry.type must be avro or protobuf")
        self.type = schema_type

        url = str(self.url).strip()
        if not url:
            raise ValueError("kafka.schemaRegistry.url must not be empty")
        self.url = url

        return self


class KafkaEos(BaseModel):
    enabled: bool = False
    transactional_id_template: str = (
        "{pipeline}.{chain_uid}.{mode}.{entities}.{hostname}.{pid}"
    )
    init_timeout_sec: float = 30.0
    transaction_timeout_ms: int = 60000


class KafkaConfig(BaseModel):
    connection: KafkaConnection
    common: KafkaCommon
    producer: KafkaProducer
    streaming: KafkaStreaming
    schemaRegistry: KafkaSchemaRegistry = Field(
        default_factory=KafkaSchemaRegistry,
        validation_alias=AliasChoices("schemaRegistry", "protobuf"),
    )
    eos: KafkaEos = Field(default_factory=KafkaEos)

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_schema_registry(cls, values):
        if not isinstance(values, dict):
            return values

        if "schemaRegistry" in values or "protobuf" not in values:
            return values

        legacy = values.get("protobuf") or {}
        if isinstance(legacy, BaseModel):
            legacy = legacy.model_dump()
        elif not isinstance(legacy, dict):
            legacy = dict(getattr(legacy, "__dict__", {}))

        migrated = dict(legacy)
        migrated.setdefault("type", "protobuf")

        copied = dict(values)
        copied["schemaRegistry"] = migrated
        return copied


class ChainConfig(BaseModel):
    uid: str
    type: str
    name: str
    network: str


class ErpcInflight(BaseModel):
    max_inflight: int
    latency_target_ms: int = 0
    min_inflight: int = 1
    initial_inflight: int | None = None
    target_multiplier: float = 3.0
    # Queue-wait budget (ms). The PRIMARY congestion signal is how long a request
    # waits for an admission slot, not the raw rpc_latency of one (possibly heavy)
    # request. 0 = adaptive (derived from the effective latency target).
    queue_wait_target_ms: int = 0
    # Contiguous windows a congestion/growth signal must persist before the
    # adaptive window reacts, so a single heavy-request latency spike cannot
    # collapse concurrency.
    adjust_cooldown_windows: int = 3
    # Failure-aware circuit breaker: pause admission (and collapse concurrency)
    # when the upstream is unhealthy, so a temporary fault can't saturate CPU /
    # memory / disk. Safe defaults; only trips on real sustained failures.
    circuit_breaker_enabled: bool = True
    trip_consecutive_failures: int = 5
    trip_failure_rate: float = 0.5
    backoff_base_sec: float = 1.0
    backoff_max_sec: float = 30.0
    probe_budget: int = 3

    @model_validator(mode="after")
    def validate_bounds(self):
        if self.latency_target_ms < 0:
            raise ValueError("erpc.inflight.latency_target_ms must be >= 0")
        if self.target_multiplier <= 0:
            raise ValueError("erpc.inflight.target_multiplier must be > 0")
        if self.min_inflight < 1:
            raise ValueError("erpc.inflight.min_inflight must be >= 1")
        if self.max_inflight < self.min_inflight:
            raise ValueError("erpc.inflight.max_inflight must be >= erpc.inflight.min_inflight")
        if self.initial_inflight is None:
            self.initial_inflight = max(self.min_inflight, max(1, self.max_inflight // 2))
        elif not (self.min_inflight <= self.initial_inflight <= self.max_inflight):
            raise ValueError(
                "erpc.inflight.initial_inflight must be between "
                "erpc.inflight.min_inflight and erpc.inflight.max_inflight"
            )
        if self.trip_consecutive_failures < 1:
            raise ValueError("erpc.inflight.trip_consecutive_failures must be >= 1")
        if not (0.0 < self.trip_failure_rate <= 1.0):
            raise ValueError("erpc.inflight.trip_failure_rate must be in (0, 1]")
        if self.backoff_base_sec <= 0:
            raise ValueError("erpc.inflight.backoff_base_sec must be > 0")
        if self.backoff_max_sec < self.backoff_base_sec:
            raise ValueError("erpc.inflight.backoff_max_sec must be >= backoff_base_sec")
        if self.probe_budget < 1:
            raise ValueError("erpc.inflight.probe_budget must be >= 1")
        if self.queue_wait_target_ms < 0:
            raise ValueError("erpc.inflight.queue_wait_target_ms must be >= 0")
        if self.adjust_cooldown_windows < 1:
            raise ValueError("erpc.inflight.adjust_cooldown_windows must be >= 1")
        return self


class ErpcConfig(BaseModel):
    project_id: str
    base_url: str
    timeout_sec: int
    max_retries: int
    inflight: ErpcInflight
    

class CheckpointConfig(BaseModel):
    topic: Optional[str] = None
    flush_interval_ms: int = 100
    commit_batch_size: int = 100


class PipelineConfigModel(BaseModel):
    name: str | None = None
    mode: str | None = None
    from_: str | int | None = Field(default=None, alias="from")
    to: str | int | None = None
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)

    @model_validator(mode="after")
    def validate_mode_fields(self):
        mode = _infer_pipeline_mode(self.from_, self.to, self.mode)
        self.mode = mode

        if self.name is not None:
            name = str(self.name).strip()
            if not name:
                raise ValueError("pipeline.name must not be empty")
            self.name = name

        if self.from_ is None:
            raise ValueError("pipeline.from is required")

        if mode == "realtime":
            if self.to is not None:
                raise ValueError("pipeline.to is not allowed in realtime mode")
            if isinstance(self.from_, str):
                start_value = self.from_.strip().lower()
                if start_value == "latest":
                    start_value = "chainhead"
                if start_value not in {"chainhead", "checkpoint"}:
                    _parse_cursor_value(start_value, "pipeline.from")
                self.from_ = start_value
            else:
                _parse_cursor_value(self.from_, "pipeline.from")
            return self

        start_cursor = _parse_cursor_value(self.from_, "pipeline.from")
        end_cursor = _parse_cursor_value(self.to, "pipeline.to")
        if start_cursor > end_cursor:
            raise ValueError("pipeline.from must be <= pipeline.to in backfill mode")
        self.from_ = start_cursor
        self.to = end_cursor
        return self

class TrackerConfig(BaseModel):
    poll_interval: float = 0.5
    websocket_url: str | None = None

    @model_validator(mode="after")
    def validate_poll_interval(self):
        if self.poll_interval <= 0:
            raise ValueError("tracker.poll_interval must be > 0")
        if self.websocket_url is not None:
            websocket_url = str(self.websocket_url).strip()
            if not websocket_url:
                self.websocket_url = None
            elif not websocket_url.startswith(("ws://", "wss://")):
                raise ValueError("tracker.websocket_url must start with ws:// or wss://")
            else:
                self.websocket_url = websocket_url
        return self


class EngineConfig(BaseModel):
    concurrency: int = 0
    # Sink (Kafka) health: when delivery hangs/fails, pause pulling new cursors
    # so we don't generate unbounded failed work. Bounds checkpoint-task growth.
    sink_failure_timeout_sec: float = 10.0
    sink_cooldown_sec: float = 15.0

    @model_validator(mode="after")
    def validate_concurrency(self):
        if self.concurrency < 0:
            raise ValueError("engine.concurrency must be >= 0 (0 = adaptive)")
        if self.sink_failure_timeout_sec <= 0:
            raise ValueError("engine.sink_failure_timeout_sec must be > 0")
        if self.sink_cooldown_sec <= 0:
            raise ValueError("engine.sink_cooldown_sec must be > 0")
        return self


class PipelineConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    logLevel: str
    pipeline: PipelineConfigModel
    chain: ChainConfig
    entities: list[str]
    erpc: ErpcConfig
    tracker: TrackerConfig = Field(default_factory=TrackerConfig)
    engine: EngineConfig = Field(default_factory=EngineConfig)
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)
    kafka: KafkaConfig
    observability: ObservabilityConfig = Field(
        default_factory=ObservabilityConfig,
        alias="telemetry",
    )


def _parse_cursor_value(value, field_name: str) -> int:
    if value is None:
        raise ValueError(f"{field_name} is required")

    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{field_name} must be >= 0")
        return value

    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty")

    number = int(text)
    if number < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return number


def _infer_pipeline_mode(from_value, to_value, explicit_mode: str | None) -> str:
    inferred = "backfill" if to_value is not None else "realtime"
    if explicit_mode is None:
        return inferred

    mode = str(explicit_mode).strip().lower()
    if mode not in {"realtime", "backfill"}:
        raise ValueError("pipeline.mode must be either 'realtime' or 'backfill'")
    if mode != inferred:
        raise ValueError(
            f"pipeline.mode={mode!r} conflicts with pipeline.from/pipeline.to inferred mode {inferred!r}"
        )
    return mode
