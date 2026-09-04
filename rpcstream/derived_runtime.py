"""Derived pipeline runtime: consumes bsc.raw_envelope, produces entity topics.

Mirrors main.py but replaces:
  - JsonRpcClient + AdaptiveRpcScheduler  →  DerivedEnvelopeFetcher (Kafka)
  - EvmRpcFetcher + entity processors     →  DerivedEnvelopeProcessor (single pass)
  - BackfillCursorSource/RealtimeCursorSource → DerivedEnvelopeFetcher.next_cursor()

No upstream RPC calls; throughput ceiling is Kafka read rate, not RPC rate.
"""
from __future__ import annotations

import asyncio
import signal
from contextlib import suppress
from urllib.parse import urlparse

from confluent_kafka import Producer

from rpcstream.adapters import build_chain_adapter
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.ingestion.derived_consumer import DerivedEnvelopeFetcher, DerivedEnvelopeProcessor
from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.runtime.observability.provider import build_observability
from rpcstream.runtime.topic import build_default_topic_namespace
from rpcstream.sinks.kafka.producer import KafkaWriter
from rpcstream.state.checkpoint import (
    KafkaCheckpointReader,
    KafkaWatermarkStateReader,
    WatermarkManager,
    build_checkpoint_identity,
)
from rpcstream.utils.logger import JsonLogger
from rpcstream.utils.throttle import ThrottledLogger


def _infer_source_topic(cfg, runtime) -> str:
    """Derive bsc.raw_envelope from the config namespace + entity name."""
    namespace = build_default_topic_namespace(cfg)
    return f"{namespace}.raw_envelope"


def _build_group_id(runtime, from_block: int | None, to_block: int | None) -> str:
    base = f"rpcstream-derived-{runtime.chain.network_label}"
    if from_block is not None or to_block is not None:
        lo = from_block if from_block is not None else "start"
        hi = to_block if to_block is not None else "end"
        return f"{base}-{lo}-{hi}"
    return base


def _install_shutdown_handlers(logger) -> asyncio.Event:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown(signal_name: str) -> None:
        if shutdown_event.is_set():
            return
        if logger:
            logger.warn("runtime.shutdown_requested", signal=signal_name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(
                sig,
                lambda _s, _f, name=sig.name: loop.call_soon_threadsafe(
                    request_shutdown, name
                ),
            )
        except (ValueError, RuntimeError):
            with suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, request_shutdown, sig.name)

    return shutdown_event


async def run_derived_pipeline(
    *,
    config_path: str | None = None,
    config=None,
    source_topic: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
) -> None:
    if config is None:
        config_path = config_path or "pipeline.yaml"
        config = load_pipeline_config(config_path)

    runtime = resolve(config)
    adapter = build_chain_adapter(runtime.chain.type)

    effective_source_topic = source_topic or _infer_source_topic(config, runtime)
    group_id = _build_group_id(runtime, from_block, to_block)

    schema_registry_enabled = getattr(
        runtime.kafka, "schema_registry_enabled",
        getattr(runtime.kafka, "protobuf_enabled", False),
    )
    schema_registry_type = getattr(
        runtime.kafka, "schema_registry_type",
        "protobuf" if getattr(runtime.kafka, "protobuf_enabled", False) else None,
    )

    observability = build_observability(
        runtime.observability.config,
        runtime.pipeline.name,
        resource_attributes={"entities": ",".join(sorted(runtime.entities))},
    )
    logger = JsonLogger(
        level=config.logLevel,
        logger_provider=observability.get_logger_provider(),
    )
    logger = ThrottledLogger(logger)

    await observability.start()
    shutdown_event = _install_shutdown_handlers(logger)

    logger.info(
        "derived_runtime.startup",
        source_topic=effective_source_topic,
        group_id=group_id,
        from_block=from_block,
        to_block=to_block,
        entities=runtime.entities,
        pipeline=runtime.pipeline.name,
        mode=runtime.pipeline.mode,
    )

    checkpoint_identity = build_checkpoint_identity(runtime)
    checkpoint_resume_enabled = (
        runtime.pipeline.mode == "backfill"
        or runtime.pipeline.start_cursor == "checkpoint"
    )

    resume_cursor = None
    state_records = {}
    checkpoint_reader = None
    state_reader = None

    try:
        if checkpoint_resume_enabled:
            checkpoint_reader = KafkaCheckpointReader(
                topic=runtime.checkpoint.topic,
                producer_config=runtime.kafka.config,
                identity=checkpoint_identity,
                schema_registry_url=(
                    runtime.kafka.schema_registry_url
                    if schema_registry_enabled else None
                ),
                schema_registry_type=schema_registry_type or "protobuf",
                logger=logger,
            )
            checkpoint_record = await asyncio.to_thread(checkpoint_reader.load)
            if checkpoint_record is not None:
                resume_cursor = checkpoint_record.cursor
                logger.info(
                    "derived_runtime.checkpoint_resume",
                    cursor=resume_cursor,
                )

            state_reader = KafkaWatermarkStateReader(
                topic=runtime.checkpoint.watermark_state_topic,
                producer_config=runtime.kafka.config,
                identity=checkpoint_identity,
                schema_registry_url=(
                    runtime.kafka.schema_registry_url
                    if schema_registry_enabled else None
                ),
                schema_registry_type=schema_registry_type or "protobuf",
                logger=logger,
            )
            state_records = await asyncio.to_thread(state_reader.load)

        # Apply checkpoint resume to from_block: skip already-committed cursors.
        effective_from_block = from_block
        if resume_cursor is not None:
            if effective_from_block is None:
                effective_from_block = resume_cursor + 1
            else:
                effective_from_block = max(effective_from_block, resume_cursor + 1)
            logger.info(
                "derived_runtime.effective_from_block",
                from_block=effective_from_block,
                resume_cursor=resume_cursor,
            )

        derived_fetcher = DerivedEnvelopeFetcher(
            kafka_config=runtime.kafka.config,
            source_topic=effective_source_topic,
            group_id=group_id,
            from_block=effective_from_block,
            to_block=to_block,
            logger=logger,
        )

        # Derived pipeline uses a single internal entity ("block_envelope") to
        # parse ALL output entities in one processor call. This avoids the
        # receipt/log double-parse issue in the normal processor registry.
        processors = {"block_envelope": DerivedEnvelopeProcessor()}

        producer = Producer(runtime.kafka.config)
        kafka_writer = KafkaWriter(
            producer=producer,
            id_calculator=adapter.build_event_id_calculator(),
            time_calculator=adapter.build_event_time_calculator(),
            logger=logger,
            config=runtime.kafka.streaming,
            producer_config=runtime.kafka.config,
            topic_maps=runtime.topic_map,
            protobuf_enabled=runtime.kafka.protobuf_enabled,
            schema_registry_url=runtime.kafka.schema_registry_url,
            schema_registry_type=schema_registry_type,
            protobuf_topic_schemas=adapter.build_protobuf_topic_schemas(
                topic_maps=runtime.topic_map,
                entities=runtime.entities,
            ),
            observability=observability,
            eos_enabled=runtime.kafka.eos_enabled,
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
            flush_on_advance=not runtime.kafka.eos_enabled,
            logger=logger,
            meter=observability.get_meter("rpcstream.watermark"),
        )

        engine = IngestionEngine(
            fetcher=derived_fetcher,
            processors=processors,
            decoder=adapter.build_decoder(client=None),
            enricher=adapter.build_enricher(),
            sink=kafka_writer,
            topics=runtime.topic_map.main,
            dlq_topic=runtime.topic_map.dlq,
            chain=runtime.chain,
            pipeline=runtime.pipeline,
            max_retry=0,
            concurrency=runtime.engine.concurrency,
            max_inflight=runtime.engine.max_inflight,
            sink_failure_timeout_sec=runtime.engine.sink_failure_timeout_sec,
            sink_cooldown_sec=runtime.engine.sink_cooldown_sec,
            sink_inflight_cursors=runtime.engine.sink_inflight_cursors,
            logger=logger,
            observability=observability,
            watermark_manager=watermark_manager,
            checkpoint_reader=checkpoint_reader,
            eos_enabled=runtime.kafka.eos_enabled,
        )

        # DerivedEnvelopeFetcher serves as both cursor_source and fetcher.
        await engine.run_stream(derived_fetcher, shutdown_event=shutdown_event)

    finally:
        if "derived_fetcher" in dir():
            derived_fetcher.close()
        await observability.shutdown()
        if shutdown_event.is_set():
            logger.warn("runtime.shutdown_complete")
