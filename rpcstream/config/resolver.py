from rpcstream.config.builder import (
    build_erpc_endpoint,
    build_kafka_config,
    build_schema_registry_url,
    build_topic_maps,
)
from rpcstream.runtime.observability.config import ObservabilityConfig
from rpcstream.runtime.topic import TopicMaps
    
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class KafkaRuntime:
    config: Dict[str, Any] 
    streaming: any
    protobuf_enabled: bool
    schema_registry_url: str | None
    
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

@dataclass
class EngineRuntime:
    concurrency: int

@dataclass
class TrackerRuntime:
    poll_interval: float

@dataclass
class PipelineRuntime:
    name: str
    mode: str
    start_block: str | int
    end_block: int | None

@dataclass
class ChainRuntime:
    uid: str
    type: str
    name: str
    network: str
    network_label: str

@dataclass
class ObservabilityRuntime:
    config: ObservabilityConfig

@dataclass
class RuntimeConfig:
    kafka: KafkaRuntime
    topic_map: TopicMaps
    client: ClientRuntime
    scheduler: SchedulerRuntime
    engine: EngineRuntime
    tracker: TrackerRuntime
    pipeline: PipelineRuntime
    chain: ChainRuntime
    entities: list[str]
    observability: ObservabilityRuntime

    
def resolve(cfg) -> RuntimeConfig:

    kafka = KafkaRuntime(
        config=build_kafka_config(cfg),
        streaming=cfg.kafka.streaming,
        protobuf_enabled=cfg.kafka.protobuf.enabled,
        schema_registry_url=build_schema_registry_url(),
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
    )

    engine = EngineRuntime(
        concurrency=cfg.engine.concurrency
    )

    tracker = TrackerRuntime(
        poll_interval=cfg.tracker.poll_interval
    )

    pipeline = PipelineRuntime(
        name=cfg.pipeline.name,
        mode=cfg.pipeline.mode,
        start_block=cfg.pipeline.start_block,
        end_block=cfg.pipeline.end_block,
    )

    chain = ChainRuntime(
        uid=cfg.chain.uid,
        type=cfg.chain.type,
        name=cfg.chain.name,
        network=cfg.chain.network,
        network_label=f"{cfg.chain.name}-{cfg.chain.network}",
    )

    topic_map = build_topic_maps(cfg)
    
    entities = cfg.entities
    
    observability = ObservabilityRuntime(
        config=cfg.observability.model_copy(deep=True),
    )
    
    return RuntimeConfig(
        kafka=kafka,
        topic_map=topic_map,
        client=client,
        scheduler=scheduler,
        engine=engine,
        tracker=tracker,
        pipeline=pipeline,
        chain=chain,
        entities=entities,
        observability=observability,
    )
