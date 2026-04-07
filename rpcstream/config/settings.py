from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGESTION_", extra="ignore")

    erpc_url: str = Field(default="http://erpc-service.erpc:4000")
    rpc_timeout_sec: int = Field(default=10)
    max_inflight_rpc: int = Field(default=10)
    poll_interval_sec: float = Field(default=2.0)


def load_settings() -> Settings:
    return Settings()
