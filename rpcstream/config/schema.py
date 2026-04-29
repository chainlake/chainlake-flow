from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional
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
    queue_maxsize: int = 100


class KafkaProtobuf(BaseModel):
    enabled: bool = True
    schema_registry_url: str = "http://localhost:30081"


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
    protobuf: KafkaProtobuf = Field(default_factory=KafkaProtobuf)
    eos: KafkaEos = Field(default_factory=KafkaEos)


class ChainConfig(BaseModel):
    uid: str
    type: str
    name: str
    network: str


class ErpcInflight(BaseModel):
    min_inflight: int
    max_inflight: int
    initial_inflight: int
    latency_target_ms: int

    @model_validator(mode="after")
    def validate_bounds(self):
        if self.min_inflight < 1:
            raise ValueError("erpc.inflight.min_inflight must be >= 1")
        if self.max_inflight < self.min_inflight:
            raise ValueError("erpc.inflight.max_inflight must be >= erpc.inflight.min_inflight")
        if not (self.min_inflight <= self.initial_inflight <= self.max_inflight):
            raise ValueError(
                "erpc.inflight.initial_inflight must be between "
                "erpc.inflight.min_inflight and erpc.inflight.max_inflight"
            )
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
                if start_value not in {"latest", "checkpoint"}:
                    _parse_block_number(start_value, "pipeline.from")
                self.from_ = start_value
            else:
                _parse_block_number(self.from_, "pipeline.from")
            return self

        start_block = _parse_block_number(self.from_, "pipeline.from")
        end_block = _parse_block_number(self.to, "pipeline.to")
        if start_block > end_block:
            raise ValueError("pipeline.from must be <= pipeline.to in backfill mode")
        self.from_ = start_block
        self.to = end_block
        return self

    @property
    def start_block(self):
        return self.from_

    @property
    def end_block(self):
        return self.to

class TrackerConfig(BaseModel):
    poll_interval: float = 0.5

    @model_validator(mode="after")
    def validate_poll_interval(self):
        if self.poll_interval <= 0:
            raise ValueError("tracker.poll_interval must be > 0")
        return self


class EngineConfig(BaseModel):
    concurrency: int


class PipelineConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    logLevel: str
    pipeline: PipelineConfigModel
    chain: ChainConfig
    entities: list[str]
    erpc: ErpcConfig
    tracker: TrackerConfig = Field(default_factory=TrackerConfig)
    engine: EngineConfig
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)
    kafka: KafkaConfig
    observability: ObservabilityConfig = Field(
        default_factory=ObservabilityConfig,
        alias="telemetry",
    )


def _parse_block_number(value, field_name: str) -> int:
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
