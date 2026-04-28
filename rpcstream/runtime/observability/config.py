from pydantic import BaseModel, Field


class TracingConfig(BaseModel):
    enabled: bool = False
    endpoint: str | None = None
    sampleRate: float = Field(default=1.0, ge=0.0, le=1.0)


class MetricsConfig(BaseModel):
    enabled: bool = False
    endpoint: str | None = None
    export_interval_ms: int = 5000


class LogsConfig(BaseModel):
    enabled: bool = False
    endpoint: str | None = None


class ObservabilityConfig(BaseModel):
    tracing: TracingConfig = Field(default_factory=TracingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logs: LogsConfig = Field(default_factory=LogsConfig)
