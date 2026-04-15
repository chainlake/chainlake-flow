import json
import asyncio
from rpcstream.client.models import RpcErrorResult

class IngestionEngine:
    def __init__(self, fetcher, processor, sink, topics, metrics=None, concurrency=10, logger=None):
        self.fetcher = fetcher
        self.processor = processor
        self.sink = sink
        self.topics = topics
        self.metrics = metrics
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger

    async def run_batch(self, start, end):
        tasks = [
            asyncio.create_task(self._run_one(block))
            for block in range(start, end + 1)
        ]

        await asyncio.gather(*tasks)

        self.sink.flush()
        
        self._print_summary()

    async def _run_one(self, block_number):
        async with self.semaphore:
            try:
                # -------------------------
                # FETCH
                # -------------------------
                result = await self.fetcher.fetch(block_number)

                # -------------------------
                # HANDLE RPC FAILURE
                # -------------------------
                if isinstance(result, RpcErrorResult):
                    error_msg = result.error
                    
                    if self.logger:
                        self.logger.warn(
                            "engine.rpc_failed",
                            component="engine",
                            pipeline=self.fetcher.pipeline_type,
                            block=block_number,
                            error=error_msg,
                        )
                    
                    # send to ALL DLQ topics (or choose one)
                    for entity in self.dlq_topics.keys():
                        self._send_dlq(
                            entity=entity,
                            block_number=block_number,
                            error=error_msg,
                            stage="rpc",
                        )
                    
                    return
                
                # -------------------------
                # SUCCESS PATH
                # -------------------------
                value, meta = result

                latency = meta.extra.get("latency_ms", 0)
                queue_wait = meta.extra.get("queue_wait_ms", 0)
                
                # -------------------------
                # PROCESS (PIPELINE ROUTING)
                # -------------------------
                pipeline_type = self.fetcher.pipeline_type
                
                parsed = self.processor.process(pipeline_type, block_number, value)

                # -------------------------
                # SINK (GENERIC)
                # -------------------------
                for entity, rows in parsed.items():
                    topic = self.topics.get(entity)
                    if not topic:
                        continue
                    self.sink.send(topic, rows)

                # -------------------------
                # METRICS (RAW only)
                # -------------------------
                if self.metrics:
                    self.metrics.record(
                        latency,
                        queue_wait,
                    )

                # -------------------------
                # LOGGING (entity agnostic)
                # -------------------------
                self.logger.info(
                    "engine.block_processed",
                    component="engine",
                    pipeline=self.fetcher.pipeline_type,
                    block=block_number,
                    latency_ms=latency,
                    queue_wait_ms=queue_wait,
                    # payload_kb=payload_kb
                )
                
                if self.logger and self.logger.isEnabledFor(10):
                    preview = str(value)[:200]
                    self.logger.debug(
                        "engine.block_processed",
                        component="engine",
                        pipeline=self.fetcher.pipeline_type,
                        block=block_number,
                        process_preview=preview
                    )

            except Exception as e:
                error_msg = repr(e)
                
                if self.logger:
                    self.logger.error(
                        "engine.processor_error",
                        component="engine",
                        pipeline=self.fetcher.pipeline_type,
                        block=block_number,
                        error=error_msg
                    )
                
                # send to all entities DLQ
                for entity in self.dlq_topics.keys():
                    self._send_dlq(
                        entity=entity,
                        block_number=block_number,
                        error=error_msg,
                        stage="processor",
                    )
                
                if self.metrics:
                    self.metrics.record_error()


    def _send_dlq(self, entity, block_number, error, stage):
        topic  = self.dlq_topics.get(entity)

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
            "pipeline": self.pipeline_type,
            "stage": stage,
            "error": error,
        }

        self.sink.send(dlq_topic, [payload])

        if self.logger:
            self.logger.warn(
                "engine.dlq_sent",
                component="engine",
                entity=entity,
                block=block_number,
                stage=stage,
                error=error,
            )


    def _print_summary(self):
        if not self.metrics:
            return

        summary = self.metrics.summary()

        print("\n==============================")
        print(" GLOBAL METRICS")
        print("==============================")
        print(f"Elapsed (s)       : {summary['elapsed_sec']:.2f}")
        print(f"Total requests    : {summary['requests']}")
        print(f"Success           : {summary['success']}")
        print(f"Errors            : {summary['errors']}")
        print(f"RPS               : {summary['rps']:.2f}")
        print(f"Avg latency (ms)  : {summary['avg_latency']:.2f}")
        print(f"P95 latency (ms)  : {summary['p95_latency']:.2f}")
        print(f"Avg payload (KB)  : {summary['avg_payload_kb']:.2f}")
        print(f"Avg tx count      : {summary['avg_tx']:.2f}")