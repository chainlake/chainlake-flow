from dataclasses import dataclass
from typing import Dict, Any

from rpcstream.adapters import build_chain_adapter
from rpcstream.config.builder import (
    build_erpc_endpoint,
    build_kafka_config,
    build_schema_registry_config,
    build_schema_registry_url,
    build_topic_maps,
)
from rpcstream.config.naming import build_pipeline_name
from rpcstream.config.profiles.store import get_chain_profile
from rpcstream.runtime.observability.config import ObservabilityConfig
from rpcstream.runtime.topic import TopicMaps


@dataclass
class KafkaRuntime:
    config: Dict[str, Any]
    streaming: any
    schema_registry_enabled: bool
    schema_registry_type: str | None
    schema_registry_url: str | None
    protobuf_enabled: bool
    eos_enabled: bool
    transactional_id: str | None
    eos_init_timeout_sec: float


@dataclass
class CheckpointRuntime:
    topic: str
    watermark_state_topic: str
    flush_interval_ms: int
    commit_batch_size: int

@dataclass
class ClientRuntime:
    base_url: str
    timeout_sec: int
    max_retries: int

@dataclass
class SchedulerRuntime:
    initial_inflight: int
    max_inflight: int
    min_inflight: int
    latency_target_ms: int
    target_multiplier: float = 3.0
    circuit_breaker_enabled: bool = True
    trip_consecutive_failures: int = 5
    trip_failure_rate: float = 0.5
    backoff_base_sec: float = 1.0
    backoff_max_sec: float = 30.0
    probe_budget: int = 3

@dataclass
class EngineRuntime:
    concurrency: int
    sink_failure_timeout_sec: float = 10.0
    sink_cooldown_sec: float = 15.0

@dataclass
class TrackerRuntime:
    poll_interval: float
    websocket_url: str | None = None

@dataclass
class PipelineRuntime:
    name: str
    mode: str
    start_cursor: str | int
    end_cursor: int | None

@dataclass
class ChainRuntime:
    uid: str
    type: str
    name: str
    network: str
    interval_seconds: float
    network_label: str

@dataclass
class ObservabilityRuntime:
    config: ObservabilityConfig

@dataclass
class RuntimeConfig:
    kafka: KafkaRuntime
    topic_map: TopicMaps
    checkpoint: CheckpointRuntime
    client: ClientRuntime
    scheduler: SchedulerRuntime
    engine: EngineRuntime
    tracker: TrackerRuntime
    pipeline: PipelineRuntime
    chain: ChainRuntime
    entities: list[str]
    internal_entities: list[str]
    observability: ObservabilityRuntime


def resolve(cfg, adapter=None) -> RuntimeConfig:
    chain_profile = get_chain_profile(cfg.chain.name, cfg.chain.network)
    adapter = adapter or build_chain_adapter(cfg.chain.type)

    kafka_config = build_kafka_config(cfg)

    kafka = KafkaRuntime(
        config=kafka_config,
        streaming=cfg.kafka.streaming,
        schema_registry_enabled=bool(schema_registry := build_schema_registry_config(cfg)),
        schema_registry_type=schema_registry["type"] if schema_registry else None,
        schema_registry_url=build_schema_registry_url(cfg),
        protobuf_enabled=bool(
            schema_registry and schema_registry["type"] == "protobuf"
        ),
        eos_enabled=cfg.kafka.eos.enabled,
        transactional_id=kafka_config.get("transactional.id"),
        eos_init_timeout_sec=cfg.kafka.eos.init_timeout_sec,
    )

    client = ClientRuntime(
        base_url=build_erpc_endpoint(cfg),
        timeout_sec=cfg.erpc.timeout_sec,
        max_retries=cfg.erpc.max_retries,
    )

    scheduler = SchedulerRuntime(
        initial_inflight=cfg.erpc.inflight.initial_inflight,
        max_inflight=cfg.erpc.inflight.max_inflight,
        min_inflight=cfg.erpc.inflight.min_inflight,
        latency_target_ms=cfg.erpc.inflight.latency_target_ms,
        target_multiplier=cfg.erpc.inflight.target_multiplier,
        circuit_breaker_enabled=cfg.erpc.inflight.circuit_breaker_enabled,
        trip_consecutive_failures=cfg.erpc.inflight.trip_consecutive_failures,
        trip_failure_rate=cfg.erpc.inflight.trip_failure_rate,
        backoff_base_sec=cfg.erpc.inflight.backoff_base_sec,
        backoff_max_sec=cfg.erpc.inflight.backoff_max_sec,
        probe_budget=cfg.erpc.inflight.probe_budget,
    )

    engine = EngineRuntime(
        concurrency=cfg.engine.concurrency or cfg.erpc.inflight.max_inflight,
        sink_failure_timeout_sec=cfg.engine.sink_failure_timeout_sec,
        sink_cooldown_sec=cfg.engine.sink_cooldown_sec,
    )

    pipeline = PipelineRuntime(
        name=cfg.pipeline.name
        or build_pipeline_name(
            chain_name=chain_profile.chain_name,
            network=chain_profile.network,
            mode=cfg.pipeline.mode,
            from_value=cfg.pipeline.from_,
            to_value=cfg.pipeline.to,
        ),
        mode=cfg.pipeline.mode,
        start_cursor=cfg.pipeline.from_,
        end_cursor=cfg.pipeline.to,
    )

    chain = ChainRuntime(
        uid=chain_profile.chain_uid,
        type=chain_profile.chain_type,
        name=chain_profile.chain_name,
        network=chain_profile.network,
        interval_seconds=chain_profile.interval_seconds,
        network_label=f"{chain_profile.chain_name}-{chain_profile.network}",
    )

    topic_map = build_topic_maps(cfg, adapter=adapter)
    checkpoint_cfg = _resolve_checkpoint_config(cfg)
    tracker = TrackerRuntime(
        poll_interval=chain_profile.interval_seconds * cfg.tracker.poll_interval,
        websocket_url=cfg.tracker.websocket_url,
    )

    checkpoint = CheckpointRuntime(
        topic=topic_map.checkpoint,
        watermark_state_topic=topic_map.watermark_state,
        flush_interval_ms=checkpoint_cfg.flush_interval_ms,
        commit_batch_size=checkpoint_cfg.commit_batch_size,
    )
    
    entities = cfg.entities
    internal_entities = adapter.resolve_internal_entities(cfg.entities)
    
    observability = ObservabilityRuntime(
        config=cfg.observability.model_copy(deep=True),
    )
    
    return RuntimeConfig(
        kafka=kafka,
        topic_map=topic_map,
        checkpoint=checkpoint,
        client=client,
        scheduler=scheduler,
        engine=engine,
        tracker=tracker,
        pipeline=pipeline,
        chain=chain,
        entities=entities,
        internal_entities=internal_entities,
        observability=observability,
    )


def _resolve_checkpoint_config(cfg):
    pipeline_fields = getattr(cfg.pipeline, "model_fields_set", set())
    root_fields = getattr(cfg, "model_fields_set", set())
    if "checkpoint" not in pipeline_fields and "checkpoint" in root_fields:
        return cfg.checkpoint
    return cfg.pipeline.checkpoint
