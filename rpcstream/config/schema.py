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


class KafkaConfig(BaseModel):
    connection: KafkaConnection
    common: KafkaCommon
    producer: KafkaProducer
    streaming: KafkaStreaming
    protobuf: KafkaProtobuf = Field(default_factory=KafkaProtobuf)


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


class ErpcConfig(BaseModel):
    project_id: str
    base_url: str
    timeout_sec: int
    max_retries: int
    inflight: ErpcInflight
    

class PipelineConfigModel(BaseModel):
    name: str
    mode: str
    start_block: str | int | None = None
    end_block: str | int | None = None

    @model_validator(mode="after")
    def validate_mode_fields(self):
        mode = (self.mode or "").strip().lower()
        self.mode = mode

        if mode not in {"realtime", "backfill"}:
            raise ValueError("pipeline.mode must be either 'realtime' or 'backfill'")

        if self.start_block is None:
            raise ValueError("pipeline.start_block is required")

        if mode == "realtime":
            if self.end_block is not None:
                raise ValueError("pipeline.end_block is not allowed in realtime mode")
            if isinstance(self.start_block, str):
                start_value = self.start_block.strip().lower()
                if start_value != "latest":
                    _parse_block_number(start_value, "pipeline.start_block")
                self.start_block = start_value
            else:
                _parse_block_number(self.start_block, "pipeline.start_block")
            return self

        start_block = _parse_block_number(self.start_block, "pipeline.start_block")
        end_block = _parse_block_number(self.end_block, "pipeline.end_block")
        if start_block > end_block:
            raise ValueError("pipeline.start_block must be <= pipeline.end_block in backfill mode")
        self.start_block = start_block
        self.end_block = end_block
        return self

class TrackerConfig(BaseModel):
    poll_interval: float


class EngineConfig(BaseModel):
    concurrency: int

class PipelineConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    logLevel: str
    pipeline: PipelineConfigModel
    chain: ChainConfig
    entities: list[str]
    erpc: ErpcConfig
    tracker: TrackerConfig
    engine: EngineConfig
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
