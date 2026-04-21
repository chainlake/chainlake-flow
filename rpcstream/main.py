import asyncio
from confluent_kafka import Producer

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import RpcFetcher
from rpcstream.ingestion.processor import EVMProcessor
from rpcstream.sinks.kafka.producer import KafkaWriter

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator

from rpcstream.planner.block_source import RealtimeBlockSource
from rpcstream.runtime.block_tracker import BlockHeadTracker
from rpcstream.runtime.telemetry import init_telemetry

from rpcstream.utils.logger import JsonLogger
import os


async def main():
    # initialize telemetry
    init_telemetry()
    
    # Load config(typed)   
    config_path = os.getenv("PIPELINE_CONFIG", "pipeline.yaml")
    config = load_pipeline_config(config_path)
    
    # Resolve config
    runtime = resolve(config)
    logger = JsonLogger(level=config.logLevel)
    main_topics, dlq_topics = runtime.topic_map
    rpc_conf = config.erpc

    try:

        client = JsonRpcClient(
            base_url=runtime.client.base_url,
            timeout_sec=runtime.client.timeout_sec,
            max_retries=runtime.client.max_retries,
            logger=logger
        )
        
        # -------------------------
        # TRACKER
        # -------------------------
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
        )

        # Pass tracker to fetcher
        fetcher = RpcFetcher(scheduler, runtime.pipeline.type, logger, tracker)

        # -------------------------
        # PROCESSOR
        # -------------------------
        processor = EVMProcessor(logger)
        
        # -------------------------
        # KAFKA
        # -------------------------
        producer = Producer(runtime.kafka.config)

        kafka_write = KafkaWriter(
            producer=producer,
            id_calculator=EventIdCalculator(),
            time_calculator=EventTimeCalculator(),
            logger=logger,
            stream_config=runtime.kafka.streaming,
        )

        # -------------------------
        # ENGINE
        # -------------------------
        engine = IngestionEngine(
            fetcher=fetcher,
            processor=processor,
            sink=kafka_write,
            topics=main_topics,
            dlq_topics=dlq_topics,
            concurrency=runtime.engine.concurrency,
            logger=logger,
        )
        
        # -------------------------
        # RUN PIPELINE
        # -------------------------
        block_source = RealtimeBlockSource(tracker)
        
        await engine.run_stream(block_source)
        
    finally:
        await tracker.stop()
        await client.close()

def cli():
    asyncio.run(main())
    
if __name__ == "__main__":
    asyncio.run(main())