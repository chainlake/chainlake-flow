import asyncio
import signal
from contextlib import suppress
from confluent_kafka import Producer

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.runtime.observability.provider import build_observability

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import EvmRpcFetcher
from rpcstream.sinks.kafka.producer import KafkaWriter

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator
from rpcstream.adapters.evm.processor import PROCESSOR_REGISTRY
from rpcstream.sinks.kafka.bootstrap import build_protobuf_topic_schemas

from rpcstream.planner.block_source import build_block_source
from rpcstream.runtime.block_tracker import BlockHeadTracker
from rpcstream.state.checkpoint import (
    CheckpointManager,
    KafkaCheckpointStore,
    build_checkpoint_identity,
)

from rpcstream.utils.logger import JsonLogger
import os


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


async def main():
    # Load config(typed)   
    config_path = os.getenv("PIPELINE_CONFIG", "pipeline.yaml")
    config = load_pipeline_config(config_path)

    # Resolve config
    runtime = resolve(config)
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

    client = None
    tracker = None
    checkpoint_manager = None
    checkpoint_store = None
    resume_cursor = None
    
    try:
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
            tracker = BlockHeadTracker(
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

        # Pass tracker to fetcher
        fetcher = EvmRpcFetcher(scheduler, runtime.entities, logger, tracker)

        # -------------------------
        # CHECKPOINT
        # -------------------------
        if runtime.kafka.eos_enabled and not runtime.checkpoint.enabled:
            raise ValueError("kafka.eos.enabled requires pipeline.checkpoint.enabled=true")

        if runtime.checkpoint.enabled:
            checkpoint_store = KafkaCheckpointStore(
                topic=runtime.checkpoint.topic,
                producer_config=runtime.kafka.config,
                identity=build_checkpoint_identity(runtime),
                logger=logger,
            )
            logger.info(
                "checkpoint.load_started",
                component="checkpoint",
                topic=runtime.checkpoint.topic,
                key=checkpoint_store.identity.key,
            )
            checkpoint_record = await asyncio.to_thread(checkpoint_store.load)
            if checkpoint_record is not None:
                resume_cursor = checkpoint_record.cursor
            if not runtime.kafka.eos_enabled:
                checkpoint_manager = CheckpointManager(
                    store=checkpoint_store,
                    initial_cursor=resume_cursor,
                    flush_interval_ms=runtime.checkpoint.flush_interval_ms,
                    commit_batch_size=runtime.checkpoint.commit_batch_size,
                    logger=logger,
                )

        # -------------------------
        # PROCESSOR
        # -------------------------
        # Load processors dynamically based on the YAML configuration
        processors = {
            entity: PROCESSOR_REGISTRY[entity]
            for entity in runtime.entities
        }
                
        
        # -------------------------
        # KAFKA
        # -------------------------
        producer = Producer(runtime.kafka.config)

        kafka_write = KafkaWriter(
            producer=producer,
            id_calculator=EventIdCalculator(),
            time_calculator=EventTimeCalculator(),
            logger=logger,
            config=runtime.kafka.streaming,
            producer_config=runtime.kafka.config,
            topic_maps=topic_maps,
            protobuf_enabled=runtime.kafka.protobuf_enabled,
            schema_registry_url=runtime.kafka.schema_registry_url,
            protobuf_topic_schemas=build_protobuf_topic_schemas(topic_maps, runtime.entities),
            observability=observability,
            eos_enabled=runtime.kafka.eos_enabled,
            eos_init_timeout_sec=runtime.kafka.eos_init_timeout_sec,
        )

        # -------------------------
        # ENGINE
        # -------------------------
        engine = IngestionEngine(
            fetcher=fetcher,
            processors=processors,
            sink=kafka_write,
            topics=main_topics,
            dlq_topic=dlq_topics,
            chain=runtime.chain,
            pipeline=runtime.pipeline,
            max_retry=runtime.client.max_retries,
            concurrency=runtime.engine.concurrency,
            logger=logger,
            observability=observability,
            checkpoint_manager=checkpoint_manager,
            checkpoint_store=checkpoint_store,
            eos_enabled=runtime.kafka.eos_enabled,
        )
        
        # -------------------------
        # RUN PIPELINE
        # -------------------------
        block_source = build_block_source(
            runtime,
            tracker,
            observability=observability,
            resume_cursor=resume_cursor,
        )
        
        await engine.run_stream(block_source, shutdown_event=shutdown_event)
        
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

def cli():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    
if __name__ == "__main__":
    cli()
