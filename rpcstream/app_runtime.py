from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka import Producer

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator
from rpcstream.adapters.evm.processor import PROCESSOR_REGISTRY
from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import EvmRpcFetcher
from rpcstream.runtime.block_tracker import BlockHeadTracker
from rpcstream.runtime.observability.provider import build_observability
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.sinks.kafka.bootstrap import build_protobuf_topic_schemas
from rpcstream.sinks.kafka.producer import KafkaWriter
from rpcstream.state.checkpoint import CheckpointManager, KafkaCheckpointStore, build_checkpoint_identity
from rpcstream.utils.logger import JsonLogger


@dataclass
class RuntimeStack:
    config: object
    runtime: object
    logger: JsonLogger
    observability: object
    client: JsonRpcClient
    tracker: BlockHeadTracker | None
    engine: IngestionEngine
    resume_cursor: int | None = None

    async def start(self) -> None:
        await self.observability.start()
        if self.tracker is not None:
            await self.tracker.start()

    async def close(self) -> None:
        if self.tracker is not None:
            await self.tracker.stop()
        else:
            await self.client.close()
        await self.observability.shutdown()


def build_runtime_stack(
    *,
    config_path: str,
    with_tracker: bool,
    with_checkpoint: bool = False,
) -> RuntimeStack:
    config = load_pipeline_config(config_path)
    runtime = resolve(config)
    logger = JsonLogger(level=config.logLevel)
    observability = build_observability(runtime.observability.config, runtime.pipeline.name)

    client = JsonRpcClient(
        base_url=runtime.client.base_url,
        timeout_sec=runtime.client.timeout_sec,
        max_retries=runtime.client.max_retries,
        logger=logger,
        observability=observability,
    )
    tracker = None
    if with_tracker and runtime.pipeline.mode == "realtime":
        tracker = BlockHeadTracker(
            client=client,
            poll_interval=runtime.tracker.poll_interval,
            logger=logger,
        )

    scheduler = AdaptiveRpcScheduler(
        client,
        initial_inflight=runtime.scheduler.initial_inflight,
        max_inflight=runtime.scheduler.max_inflight,
        min_inflight=runtime.scheduler.min_inflight,
        latency_target_ms=runtime.scheduler.latency_target_ms,
        logger=logger,
        observability=observability,
    )
    fetcher = EvmRpcFetcher(scheduler, runtime.entities, logger, tracker)
    checkpoint_manager = None
    checkpoint_store = None
    resume_cursor = None
    eos_active = with_checkpoint and runtime.kafka.eos_enabled
    if eos_active and not runtime.checkpoint.enabled:
        raise ValueError("kafka.eos.enabled requires pipeline.checkpoint.enabled=true")
    if with_checkpoint and runtime.checkpoint.enabled:
        checkpoint_store = KafkaCheckpointStore(
            topic=runtime.checkpoint.topic,
            producer_config=runtime.kafka.config,
            identity=build_checkpoint_identity(runtime),
            logger=logger,
        )
        checkpoint_record = checkpoint_store.load()
        if checkpoint_record is not None:
            resume_cursor = checkpoint_record.cursor
        if not eos_active:
            checkpoint_manager = CheckpointManager(
                store=checkpoint_store,
                initial_cursor=resume_cursor,
                flush_interval_ms=runtime.checkpoint.flush_interval_ms,
                commit_batch_size=runtime.checkpoint.commit_batch_size,
                logger=logger,
            )
    processors = {
        entity: PROCESSOR_REGISTRY[entity]
        for entity in runtime.entities
    }
    producer = Producer(runtime.kafka.config)
    kafka_writer = KafkaWriter(
        producer=producer,
        id_calculator=EventIdCalculator(),
        time_calculator=EventTimeCalculator(),
        logger=logger,
        config=runtime.kafka.streaming,
        producer_config=runtime.kafka.config,
        topic_maps=runtime.topic_map,
        protobuf_enabled=runtime.kafka.protobuf_enabled,
        schema_registry_url=runtime.kafka.schema_registry_url,
        protobuf_topic_schemas=build_protobuf_topic_schemas(runtime.topic_map, runtime.entities),
        observability=observability,
        eos_enabled=eos_active,
        eos_init_timeout_sec=runtime.kafka.eos_init_timeout_sec,
    )
    engine = IngestionEngine(
        fetcher=fetcher,
        processors=processors,
        sink=kafka_writer,
        topics=runtime.topic_map.main,
        dlq_topic=runtime.topic_map.dlq,
        chain=runtime.chain,
        pipeline=runtime.pipeline,
        max_retry=runtime.client.max_retries,
        concurrency=runtime.engine.concurrency,
        logger=logger,
        observability=observability,
        checkpoint_manager=checkpoint_manager,
        checkpoint_store=checkpoint_store,
        eos_enabled=eos_active,
    )

    return RuntimeStack(
        config=config,
        runtime=runtime,
        logger=logger,
        observability=observability,
        client=client,
        tracker=tracker,
        engine=engine,
        resume_cursor=resume_cursor,
    )
