import json
import asyncio
from contextlib import suppress
import time
from rpcstream.client.models import RpcErrorResult
from rpcstream.ingestion.dlq import (
    build_resolved_record,
    build_retry_record,
    build_unified_dlq_record,
    compute_next_retry_at,
)

from opentelemetry.trace import Status, StatusCode

from rpcstream.metrics.engine import EngineMetrics
from rpcstream.runtime.observability.context import ObservabilityContext

class IngestionEngine:
    def __init__(
        self, 
        fetcher, 
        processors, 
        sink, 
        topics, 
        dlq_topic=None,
        dlq_topics=None,
        chain=None,
        pipeline=None,
        max_retry=0,
        concurrency=10, 
        logger=None,
        observability: ObservabilityContext | None = None,
        checkpoint_manager=None,
    ):
        self.fetcher = fetcher
        self.processors = processors # e.g. {"block": BlockProcessor(), "transaction": TransactionProcessor(), ...}
        self.sink = sink
        self.topics = topics
        if dlq_topic is None and dlq_topics is not None:
            if isinstance(dlq_topics, dict):
                dlq_topic = next(iter(dlq_topics.values()), None)
            else:
                dlq_topic = dlq_topics

        self.dlq_topic = dlq_topic
        self.chain = chain
        self.pipeline = pipeline
        self.max_retry = max_retry
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger
        self._latest_processed_block = 0
        self._lag_lock = asyncio.Lock()
        self._active_dlq_record = None
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)
        self.metrics = EngineMetrics(self.observability.get_meter("rpcstream.engine"))
        self.checkpoint_manager = checkpoint_manager
        self._checkpoint_tasks = set()

    async def run_stream(self, block_source, shutdown_event: asyncio.Event | None = None):
        sink_started = False
        checkpoint_started = False
        workers = []
        await self.sink.start()
        sink_started = True
        if self.checkpoint_manager is not None:
            await self.checkpoint_manager.start()
            checkpoint_started = True

        queue = asyncio.Queue(maxsize=1000)

        async def producer():
            try:
                while not self._is_shutdown_requested(shutdown_event):
                    block = await self._next_block_or_shutdown(block_source, shutdown_event)
                    if block is None:
                        break
                    if self.checkpoint_manager is not None:
                        await self.checkpoint_manager.mark_emitted(block)
                    await queue.put(block)
            finally:
                # Signal workers to drain and stop after queued blocks are processed.
                for _ in range(self.concurrency):
                    await queue.put(None)

        async def worker():
            while True:
                block = await queue.get()
                if block is None:
                    break
                success, delivery_futures = await self._run_one(block)
                if self.checkpoint_manager is not None:
                    task = asyncio.create_task(
                        self._finalize_checkpoint(block, success, delivery_futures)
                    )
                    self._checkpoint_tasks.add(task)
                    task.add_done_callback(self._checkpoint_tasks.discard)

        try:
            workers = [
                asyncio.create_task(worker())
                for _ in range(self.concurrency)
            ]

            await producer()
            if self._is_shutdown_requested(shutdown_event) and self.logger:
                self.logger.warn(
                    "engine.shutdown_draining",
                    component="engine",
                    queued_blocks=queue.qsize(),
                    checkpoint_tasks=len(self._checkpoint_tasks),
                )
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warn(
                    "engine.shutdown_cancelled",
                    component="engine",
                )
            for task in workers:
                task.cancel()
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
        finally:
            if sink_started:
                await self.sink.close()
            if self._checkpoint_tasks:
                await asyncio.gather(*self._checkpoint_tasks, return_exceptions=True)
            if self.checkpoint_manager is not None and checkpoint_started:
                status = "eos" if getattr(self.pipeline, "mode", None) == "backfill" else "running"
                await self.checkpoint_manager.stop(status=status)

    def _is_shutdown_requested(self, shutdown_event: asyncio.Event | None) -> bool:
        return shutdown_event is not None and shutdown_event.is_set()

    async def _next_block_or_shutdown(self, block_source, shutdown_event: asyncio.Event | None):
        if shutdown_event is None:
            return await block_source.next_block()

        next_block_task = asyncio.create_task(block_source.next_block())
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        done, pending = await asyncio.wait(
            {next_block_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if shutdown_task in done:
            next_block_task.cancel()
            with suppress(asyncio.CancelledError):
                await next_block_task
            return None

        shutdown_task.cancel()
        with suppress(asyncio.CancelledError):
            await shutdown_task
        return await next_block_task


    async def _run_one(self, block_number):
        start_total = time.time()
        current_entity = "unknown"
        success = True
        delivery_futures = []
        try:
            # =========================
            # Span for the entire streaming process
            # =========================
            with self._tracer.start_as_current_span("streaming.run") as root_span:
                root_span.set_attribute("component", "engine")
                root_span.set_attribute("block_number", block_number)
                    
                self.metrics.INFLIGHT.add(1)
            
                # 1. FETCH
                raw_data = await self.fetcher.fetch(block_number)

                # Process data for each entity
                for entity, processor in self.processors.items():
                    current_entity = entity
                    # HANDLE RPC FAILURE
                    if isinstance(raw_data[entity], RpcErrorResult):
                        error_msg = raw_data[entity].error
                        error_details = raw_data[entity].details.copy()
                        self.metrics.ERROR_COUNTER.add(1, {"stage": "rpc"})
                        
                        if self.logger:
                            self.logger.warn(
                                "engine.rpc_failed",
                                component="engine",
                                entity=entity,
                                block=block_number,
                                error=error_msg,
                                expected=raw_data[entity].expected,
                                **{
                                    key: value
                                    for key, value in error_details.items()
                                    if key != "block"
                                },
                            )
                        await self._send_dlq(
                            entity=entity,
                            block_number=block_number,
                            stage="rpc",
                            error_type="RpcError",
                            error_message=error_msg,
                            payload=None,
                            context={
                                "request": raw_data[entity].meta.extra,
                                "rpc_error": error_details,
                                "expected": raw_data[entity].expected,
                            },
                        )
                        success = False
                        return False, delivery_futures
                    
                    try:
                        value, meta = raw_data[entity]
                        processed_data = processor.process(block_number, value)

                        # 3. LAG
                        latest_block, chain_lag, ingestion_lag = await self._compute_lag(block_number)
                        if chain_lag is not None:
                            self.metrics.CHAIN_LAG.record(chain_lag)
                        if ingestion_lag is not None:
                            self.metrics.INGESTION_LAG.record(ingestion_lag)
                        
                        latency = meta.extra.get("latency_ms", 0)
                        queue_wait = meta.extra.get("queue_wait_ms", 0)
                        
                        self.metrics.BLOCK_COUNTER.add(1, {"entity": entity})
                        self.metrics.BLOCK_LATENCY.record(latency, {"entity": entity})
                        self.metrics.QUEUE_WAIT.record(queue_wait, {"entity": entity})
                        
                        # 4. SINK
                        topic = self.topics.get(entity)
                        rows = processed_data[entity]
                        
                        self.metrics.ROW_COUNTER.add(len(rows), {"entity": entity})
                        if self.logger:
                            self.logger.info(
                                "engine.processed",
                                component="engine",
                                block=block_number,
                                entity=entity,
                                latency_ms=latency,
                                payload=len(rows),
                                ingestion_lag=ingestion_lag,
                                chain_lag=chain_lag,
                            )

                        if not topic:
                            continue
                        delivery_future = await self.sink.send(
                            topic,
                            rows,
                            wait_delivery=self.checkpoint_manager is not None,
                        )
                        if delivery_future is not None:
                            delivery_futures.append(delivery_future)
                        
                        
                    except Exception as e:
                        await self._send_dlq(
                            entity=entity,
                            block_number=block_number,
                            stage="processor",
                            error_type=type(e).__name__,
                            error_message=str(e),
                            payload=value,
                            context={
                                "processor": processor.__class__.__name__,
                                "meta": meta.extra,
                            },
                        )
                        success = False
                            
        except Exception as e:
            # Handle failure in a new span
            with self._tracer.start_as_current_span("engine.error") as error_span:
                error_span.set_status(Status(StatusCode.ERROR))
                error_span.set_attribute("error.message", str(e))
                error_span.set_attribute("entity", current_entity)
                error_span.set_attribute("block_number", block_number)
            
            error_msg = repr(e)
            self.metrics.ERROR_COUNTER.add(1, {"stage": "processor"})
            
            if self.logger:
                self.logger.error(
                    "engine.processor_error",
                    component="engine",
                    entity=current_entity,
                    block=block_number,
                    error=error_msg
                )
            
            await self._send_dlq(
                entity=current_entity,
                block_number=block_number,
                stage="processor",
                error_type=type(e).__name__,
                error_message=str(e),
                payload=None,
                context={},
            )
            success = False
            
        finally:
            self.metrics.INFLIGHT.add(-1)
            total_ms = (time.time() - start_total) * 1000
            self.metrics.TOTAL_TIME.record(total_ms, {"entity": current_entity})
        return success, delivery_futures

    async def _finalize_checkpoint(self, block_number, success, delivery_futures):
        if not success:
            await self.checkpoint_manager.mark_failed(block_number)
            return
        try:
            if delivery_futures:
                await asyncio.gather(*delivery_futures)
            await self.checkpoint_manager.mark_completed(block_number)
        except Exception as exc:
            await self.checkpoint_manager.mark_failed(block_number, error=str(exc))



    async def _send_dlq(
        self,
        entity,
        block_number,
        stage,
        error_type,
        error_message,
        payload=None,
        context=None,
    ):
        topic = self.dlq_topic
        self.metrics.DLQ_COUNTER.add(1, {"entity": entity, "stage": stage})
        
        if not topic:
            if self.logger:
                self.logger.warn(
                    "engine.dlq_missing_topic",
                    component="engine",
                    entity=entity,
                    block=block_number,
                )
            return

        if self._active_dlq_record is not None:
            record = build_retry_record(
                self._active_dlq_record,
                error_type=error_type,
                error_message=error_message,
                payload=payload,
                context=context,
            )
        else:
            record = build_unified_dlq_record(
                chain=getattr(self.chain, "type", "unknown"),
                network=getattr(self.chain, "network_label", "unknown"),
                pipeline=getattr(self.pipeline, "name", "unknown"),
                entity=entity,
                block_number=block_number,
                stage=stage,
                error_type=error_type,
                error_message=error_message,
                payload=payload,
                context=context,
                retry_count=0,
                max_retry=self.max_retry,
                status="pending",
                next_retry_at=compute_next_retry_at(retry_count=1),
            )

        await self.sink.send(topic, [record])

        if self.logger:
            self.logger.warn(
                "engine.dlq_sent",
                component="engine",
                topic=topic,
                entity=entity,
                block=block_number,
                stage=stage,
                error_type=error_type,
                error=error_message,
                status=record["status"],
                retry_count=record["retry_count"],
            )

    async def retry_dlq_record(self, record: dict) -> bool:
        previous = self._active_dlq_record
        self._active_dlq_record = record
        try:
            success, _delivery_futures = await self._run_one(record["block_number"])
            return success
        finally:
            self._active_dlq_record = previous

    async def mark_dlq_resolved(self, record: dict) -> None:
        if not self.dlq_topic:
            return
        await self.sink.send(self.dlq_topic, [build_resolved_record(record)])
            
            
    async def _update_ingestion_lag(self, block_number, latest_block):
        async with self._lag_lock:
            if block_number > self._latest_processed_block:
                self._latest_processed_block = block_number

            ingestion_lag = None
            if latest_block is not None:
                ingestion_lag = latest_block - self._latest_processed_block

            return ingestion_lag
        
    
    async def _compute_lag(self, block_number):
        latest_block = None
        chain_lag = None
        ingestion_lag = None

        tracker = getattr(self.fetcher, "tracker", None)

        if tracker:
            latest_block = tracker.get_latest()

            if latest_block is not None:
                # point-in-time lag
                chain_lag = latest_block - block_number

                # true pipeline lag (protected update)
                ingestion_lag = await self._update_ingestion_lag(
                    block_number,
                    latest_block
                )

        return latest_block, chain_lag, ingestion_lag
