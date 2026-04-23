import json
import asyncio
import time
from rpcstream.client.models import RpcErrorResult

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

tracer = trace.get_tracer(__name__)

from rpcstream.metrics.engine import (
    BLOCK_COUNTER,
    ROW_COUNTER,
    BLOCK_LATENCY,
    QUEUE_WAIT,
    INFLIGHT,
    TOTAL_TIME,
    ERROR_COUNTER,
    DLQ_COUNTER,
    CHAIN_LAG,
    INGESTION_LAG,
)

class IngestionEngine:
    def __init__(
        self, 
        fetcher, 
        processors, 
        sink, 
        topics, 
        dlq_topics=None,
        concurrency=10, 
        logger=None
    ):
        self.fetcher = fetcher
        self.processors = processors # e.g. {"block": BlockProcessor(), "transaction": TransactionProcessor(), ...}
        self.sink = sink
        self.topics = topics
        self.dlq_topics = dlq_topics or {}
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger
        self._latest_processed_block = 0
        self._lag_lock = asyncio.Lock()

    async def run_stream(self, block_source):
        await self.sink.start()

        queue = asyncio.Queue(maxsize=1000)

        async def producer():
            while True:
                block = await block_source.next_block()
                if block is None:
                    break
                await queue.put(block)

            # signal shutdown
            for _ in range(self.semaphore._value):
                await queue.put(None)

        async def worker():
            while True:
                block = await queue.get()
                if block is None:
                    break
                await self._run_one(block)

        workers = [
            asyncio.create_task(worker())
            for _ in range(self.semaphore._value)
        ]

        await producer()
        await asyncio.gather(*workers)
        await self.sink.close()


    async def _run_one(self, block_number):
        try:
            # =========================
            # Span for the entire streaming process
            # =========================
            with tracer.start_as_current_span("streaming.run") as root_span:
                root_span.set_attribute("component", "engine")
                root_span.set_attribute("block_number", block_number)
                    
                INFLIGHT.add(1)
                start_total = time.time()            
            
                # 1. FETCH
                raw_data = await self.fetcher.fetch(block_number)

                # Process data for each entity
                for entity, processor in self.processors.items():
                    # HANDLE RPC FAILURE
                    if isinstance(raw_data[entity], RpcErrorResult):
                        error_msg = result.error
                        ERROR_COUNTER.add(1, {"stage": "rpc"})
                        
                        if self.logger:
                            self.logger.warn(
                                "engine.rpc_failed",
                                component="engine",
                                entity=entity,
                                block=block_number,
                                error=error_msg,
                            )
                        for entity in self.dlq_topics.keys():
                            await self._send_dlq(
                                entity=entity,
                                block_number=block_number,
                                error=error_msg,
                                stage="rpc"
                            )
                        return
                    
                    try:
                        value, meta = raw_data[entity]
                        processed_data = processor.process(block_number, value)

                        # 3. LAG
                        latest_block, chain_lag, ingestion_lag = await self._compute_lag(block_number)
                        if chain_lag is not None:
                            CHAIN_LAG.record(chain_lag)
                        if ingestion_lag is not None:
                            INGESTION_LAG.record(ingestion_lag)
                        
                        latency = meta.extra.get("latency_ms", 0)
                        queue_wait = meta.extra.get("queue_wait_ms", 0)
                        
                        BLOCK_COUNTER.add(1, {"entity": entity})
                        BLOCK_LATENCY.record(latency, {"entity": entity})
                        QUEUE_WAIT.record(queue_wait, {"entity": entity})
                        
                        # 4. SINK
                        topic = self.topics.get(entity)
                        rows = processed_data[entity]
                        
                        ROW_COUNTER.add(len(rows), {"entity": entity})
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
                        await self.sink.send(topic, rows)
                        
                        
                    except Exception as e:
                        await self._send_dlq(entity, block_number, repr(e), "processor")
                            
        except Exception as e:
            # Handle failure in a new span
            with tracer.start_as_current_span("engine.error") as error_span:
                error_span.set_status(Status(StatusCode.ERROR))
                error_span.set_attribute("error.message", str(e))
                error_span.set_attribute("entity", entity)
                error_span.set_attribute("block_number", block_number)
            
            error_msg = repr(e)
            ERROR_COUNTER.add(1, {"stage": "processor"})
            
            if self.logger:
                self.logger.error(
                    "engine.processor_error",
                    component="engine",
                    entity=entity,
                    block=block_number,
                    error=error_msg
                )
            
            # send to all entities DLQ
            for entity in self.dlq_topics.keys():
                await self._send_dlq(
                    entity=entity,
                    block_number=block_number,
                    error=error_msg,
                    stage="processor",
                )
            
            if self.metrics:
                self.metrics.record_error()

        finally:
            INFLIGHT.add(-1)
            total_ms = (time.time() - start_total) * 1000
            TOTAL_TIME.record(total_ms, {"entity": entity})



    async def _send_dlq(self, entity, block_number, error, stage):
        topic  = self.dlq_topics.get(entity)
        DLQ_COUNTER.add(1, {"entity": entity, "stage": stage})
        
        if not topic:
            if self.logger:
                self.logger.warn(
                    "engine.dlq_missing_topic",
                    component="engine",
                    entity=entity,
                    block=block_number,
                )
            return

        payload = {
            "block": block_number,
            "entity": entity,
            "stage": stage,
            "error": error,
        }

        await self.sink.send(topic, [payload])

        if self.logger:
            self.logger.warn(
                "engine.dlq_sent",
                component="engine",
                entity=entity,
                block=block_number,
                stage=stage,
                error=error,
            )
            
            
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