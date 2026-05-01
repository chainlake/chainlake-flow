import asyncio
import os
import signal
from contextlib import suppress
from confluent_kafka import Producer

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.adapters import build_chain_adapter
from rpcstream.runtime.observability.provider import build_observability
from rpcstream.sinks.kafka.admin import KafkaTopicManager
from rpcstream.sinks.kafka.bootstrap import bootstrap_kafka_resources

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.sinks.kafka.producer import KafkaWriter
from rpcstream.planner.cursor_source import build_cursor_source
from rpcstream.state.checkpoint import (
    KafkaCheckpointReader,
    KafkaWatermarkStateReader,
    WatermarkManager,
    build_checkpoint_identity,
)

from rpcstream.utils.logger import JsonLogger


def install_shutdown_handlers(logger) -> asyncio.Event:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown(signal_name: str) -> None:
        if shutdown_event.is_set():
            return
        if logger:
            logger.warn(
                "runtime.shutdown_requested",
                component="runtime",
                signal=signal_name,
            )
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(
                sig,
                lambda _signum, _frame, name=sig.name: loop.call_soon_threadsafe(
                    request_shutdown,
                    name,
                ),
            )
        except (ValueError, RuntimeError):
            with suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, request_shutdown, sig.name)

    return shutdown_event


async def run_pipeline(*, config_path: str | None = None, config=None):
    if config is None:
        config_path = config_path or os.getenv("PIPELINE_CONFIG", "pipeline.yaml")
        config = load_pipeline_config(config_path)

    # Resolve config
    runtime = resolve(config)
    adapter = build_chain_adapter(runtime.chain.type)
    topic_maps = runtime.topic_map
    main_topics = topic_maps.main
    dlq_topics = topic_maps.dlq
    observability = build_observability(runtime.observability.config, runtime.pipeline.name)
    logger = JsonLogger(
        level=config.logLevel,
        logger_provider=observability.get_logger_provider(),
    )
    shutdown_event = install_shutdown_handlers(logger)
    await observability.start()
    pipeline_start_cursor = runtime.pipeline.start_cursor
    logger.info(
        "runtime.startup_context",
        component="runtime",
        schema_registry_url=runtime.kafka.schema_registry_url,
        checkpoint_topic=runtime.checkpoint.topic,
        watermark_state_topic=runtime.checkpoint.watermark_state_topic,
        protobuf_enabled=runtime.kafka.protobuf_enabled,
        pipeline_start_cursor=pipeline_start_cursor,
        checkpoint_resume_enabled=(
            runtime.pipeline.mode == "backfill" or pipeline_start_cursor == "checkpoint"
        ),
    )

    client = None
    tracker = None
    watermark_manager = None
    checkpoint_reader = None
    state_reader = None
    checkpoint_identity = None
    resume_cursor = None
    state_records = {}
    checkpoint_resume_enabled = runtime.pipeline.mode == "backfill" or pipeline_start_cursor == "checkpoint"
    
    try:
        internal_entities = getattr(runtime, "internal_entities", runtime.entities)

        client = JsonRpcClient(
            base_url=runtime.client.base_url,
            timeout_sec=runtime.client.timeout_sec,
            max_retries=runtime.client.max_retries,
            logger=logger,
            observability=observability,
        )
        
        # -------------------------
        # TRACKER
        # -------------------------
        if runtime.pipeline.mode == "realtime":
            tracker = adapter.build_tracker(
                client=client,
                poll_interval=runtime.tracker.poll_interval,
                logger=logger,
            )
            await tracker.start()
        
        # -------------------------
        # SCHEDULER
        # -------------------------

        scheduler = AdaptiveRpcScheduler(
            client,
            initial_inflight=runtime.scheduler.initial_inflight,
            max_inflight=runtime.scheduler.max_inflight,
            min_inflight=runtime.scheduler.min_inflight,
            latency_target_ms=runtime.scheduler.latency_target_ms,
            logger=logger,
            observability=observability,
        )

        fetcher = adapter.build_fetcher(
            scheduler=scheduler,
            entities=internal_entities,
            logger=logger,
            tracker=tracker,
        )

        # -------------------------
        # CHECKPOINT
        # -------------------------
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
            logger.info(
                "checkpoint.load_started",
                component="checkpoint",
                topic=runtime.checkpoint.topic,
                key=checkpoint_reader.identity.key,
            )
            checkpoint_record = await asyncio.to_thread(checkpoint_reader.load)
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
            state_records = await asyncio.to_thread(state_reader.load)

            if getattr(checkpoint_reader, "schema_missing", False) or getattr(
                state_reader, "schema_missing", False
            ):
                logger.warn(
                    "checkpoint.schema_reset_requested",
                    component="checkpoint",
                    checkpoint_topic=runtime.checkpoint.topic,
                    watermark_state_topic=runtime.checkpoint.watermark_state_topic,
                    schema_registry_url=runtime.kafka.schema_registry_url,
                )
                topic_manager = KafkaTopicManager(
                    producer_config=runtime.kafka.config,
                    logger=logger,
                )
                await asyncio.to_thread(
                    topic_manager.delete_topics,
                    [runtime.checkpoint.topic, runtime.checkpoint.watermark_state_topic],
                )
                bootstrap_kafka_resources(runtime, adapter=adapter, logger=logger)
                resume_cursor = None
                state_records = {}
                checkpoint_reader = None
                state_reader = None

        # -------------------------
        # PROCESSOR
        # -------------------------
        # Load processors dynamically based on the YAML configuration
        processors = adapter.build_processors(entities=internal_entities)
                
        
        # -------------------------
        # KAFKA
        # -------------------------
        producer = Producer(runtime.kafka.config)

        kafka_write = KafkaWriter(
            producer=producer,
            id_calculator=adapter.build_event_id_calculator(),
            time_calculator=adapter.build_event_time_calculator(),
            logger=logger,
            config=runtime.kafka.streaming,
            producer_config=runtime.kafka.config,
            topic_maps=topic_maps,
            protobuf_enabled=runtime.kafka.protobuf_enabled,
            schema_registry_url=runtime.kafka.schema_registry_url,
            protobuf_topic_schemas=adapter.build_protobuf_topic_schemas(
                topic_maps=topic_maps,
                entities=runtime.entities,
            ),
            observability=observability,
            eos_enabled=runtime.kafka.eos_enabled,
            eos_init_timeout_sec=runtime.kafka.eos_init_timeout_sec,
        )

        watermark_manager = WatermarkManager(
            sink=kafka_write,
            topic=runtime.checkpoint.topic,
            state_topic=runtime.checkpoint.watermark_state_topic,
            identity=checkpoint_identity,
            initial_cursor=resume_cursor,
            state_records=state_records,
            state_reader=state_reader,
            flush_interval_ms=runtime.checkpoint.flush_interval_ms,
            commit_batch_size=runtime.checkpoint.commit_batch_size,
            flush_on_advance=not runtime.kafka.eos_enabled,
            logger=logger,
            meter=observability.get_meter("rpcstream.watermark"),
        )

        # -------------------------
        # ENGINE
        # -------------------------
        engine = IngestionEngine(
            fetcher=fetcher,
            processors=processors,
            enricher=adapter.build_enricher(),
            sink=kafka_write,
            topics=main_topics,
            dlq_topic=dlq_topics,
            chain=runtime.chain,
            pipeline=runtime.pipeline,
            max_retry=runtime.client.max_retries,
            concurrency=runtime.engine.concurrency,
            logger=logger,
            observability=observability,
            watermark_manager=watermark_manager,
            checkpoint_reader=checkpoint_reader,
            eos_enabled=runtime.kafka.eos_enabled,
        )
        
        # -------------------------
        # RUN PIPELINE
        # -------------------------
        cursor_source = build_cursor_source(
            runtime,
            tracker,
            observability=observability,
            resume_cursor=resume_cursor,
        )
        
        await engine.run_stream(cursor_source, shutdown_event=shutdown_event)
        
    finally:
        if tracker is not None:
            await tracker.stop()
        elif client is not None:
            await client.close()
        await observability.shutdown()
        if shutdown_event.is_set():
            logger.warn(
                "runtime.shutdown_complete",
                component="runtime",
            )


async def main():
    await run_pipeline()

def cli():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    
if __name__ == "__main__":
    cli()
