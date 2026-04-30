from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka import Producer

from rpcstream.adapters import build_chain_adapter
from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.runtime.observability.provider import build_observability
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.sinks.kafka.producer import KafkaWriter
from rpcstream.state.checkpoint import (
    KafkaCheckpointReader,
    KafkaWatermarkStateReader,
    WatermarkManager,
    build_checkpoint_identity,
)
from rpcstream.utils.logger import JsonLogger


@dataclass
class RuntimeStack:
    config: object
    runtime: object
    logger: JsonLogger
    observability: object
    client: JsonRpcClient
    tracker: object | None
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
    config_path: str | None = None,
    config: object | None = None,
    with_tracker: bool,
) -> RuntimeStack:
    if config is None:
        if config_path is None:
            raise ValueError("config_path is required when config is not provided")
        config = load_pipeline_config(config_path)
    runtime = resolve(config)
    adapter = build_chain_adapter(runtime.chain.type)
    observability = build_observability(runtime.observability.config, runtime.pipeline.name)
    logger = JsonLogger(
        level=config.logLevel,
        logger_provider=observability.get_logger_provider(),
    )

    client = JsonRpcClient(
        base_url=runtime.client.base_url,
        timeout_sec=runtime.client.timeout_sec,
        max_retries=runtime.client.max_retries,
        logger=logger,
        observability=observability,
    )
    tracker = None
    if with_tracker and runtime.pipeline.mode == "realtime":
        tracker = adapter.build_tracker(
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
    internal_entities = getattr(runtime, "internal_entities", runtime.entities)
    fetcher = adapter.build_fetcher(
        scheduler=scheduler,
        entities=internal_entities,
        logger=logger,
        tracker=tracker,
    )
    watermark_manager = None
    checkpoint_reader = None
    state_reader = None
    checkpoint_identity = None
    resume_cursor = None
    state_records = {}
    pipeline_start_cursor = getattr(runtime.pipeline, "start_cursor", "checkpoint")
    checkpoint_resume_enabled = (
        runtime.pipeline.mode == "backfill"
        or pipeline_start_cursor == "checkpoint"
    )
    eos_active = runtime.kafka.eos_enabled
    producer_config = dict(runtime.kafka.config)
    checkpoint_identity = build_checkpoint_identity(runtime)
    if checkpoint_resume_enabled:
        checkpoint_reader = KafkaCheckpointReader(
            topic=runtime.checkpoint.topic,
            producer_config=runtime.kafka.config,
            identity=checkpoint_identity,
            schema_registry_url=(
                runtime.kafka.schema_registry_url
                if runtime.kafka.protobuf_enabled
                else None
            ),
            logger=logger,
        )
        checkpoint_record = checkpoint_reader.load()
        if checkpoint_record is not None:
            resume_cursor = checkpoint_record.cursor
        state_reader = KafkaWatermarkStateReader(
            topic=runtime.checkpoint.watermark_state_topic,
            producer_config=runtime.kafka.config,
            identity=checkpoint_identity,
            schema_registry_url=(
                runtime.kafka.schema_registry_url
                if runtime.kafka.protobuf_enabled
                else None
            ),
            logger=logger,
        )
        state_records = state_reader.load()
    processors = adapter.build_processors(entities=internal_entities)
    producer = Producer(producer_config)
    kafka_writer = KafkaWriter(
        producer=producer,
        id_calculator=adapter.build_event_id_calculator(),
        time_calculator=adapter.build_event_time_calculator(),
        logger=logger,
        config=runtime.kafka.streaming,
        producer_config=producer_config,
        topic_maps=runtime.topic_map,
        protobuf_enabled=runtime.kafka.protobuf_enabled,
        schema_registry_url=runtime.kafka.schema_registry_url,
        protobuf_topic_schemas=adapter.build_protobuf_topic_schemas(
            topic_maps=runtime.topic_map,
            entities=runtime.entities,
        ),
        observability=observability,
        eos_enabled=eos_active,
        eos_init_timeout_sec=runtime.kafka.eos_init_timeout_sec,
    )
    watermark_manager = WatermarkManager(
        sink=kafka_writer,
        topic=runtime.checkpoint.topic,
        state_topic=runtime.checkpoint.watermark_state_topic,
        identity=checkpoint_identity,
        initial_cursor=resume_cursor,
        state_records=state_records,
        state_reader=state_reader,
        flush_interval_ms=runtime.checkpoint.flush_interval_ms,
        commit_batch_size=runtime.checkpoint.commit_batch_size,
        flush_on_advance=not eos_active,
        logger=logger,
        meter=observability.get_meter("rpcstream.watermark"),
    )
    engine = IngestionEngine(
        fetcher=fetcher,
        processors=processors,
        enricher=adapter.build_enricher(),
        sink=kafka_writer,
        topics=runtime.topic_map.main,
        dlq_topic=runtime.topic_map.dlq,
        chain=runtime.chain,
        pipeline=runtime.pipeline,
        max_retry=runtime.client.max_retries,
        concurrency=runtime.engine.concurrency,
        logger=logger,
        observability=observability,
        watermark_manager=watermark_manager,
        checkpoint_reader=checkpoint_reader,
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
