import asyncio
import os

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator

from confluent_kafka import Producer

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import RpcFetcher
from rpcstream.ingestion.processor import EVMProcessor
from rpcstream.sinks.kafka.producer import KafkaSink

from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions


RPC_URL = "http://localhost:30040/main/evm/56"
KAFKA_BROKER = "localhost:30092"

START_BLOCK = 90000099
END_BLOCK = 90000100

CHAIN = "bsc"
BLOCK_TOPIC = f"{CHAIN}.raw_blocks"
TRANSACTION_TOPIC = f"{CHAIN}.raw_transactions"


async def main():

    # -------------------------
    # RPC Layer
    # -------------------------
    client = JsonRpcClient(RPC_URL, timeout_sec=5)

    scheduler = AdaptiveRpcScheduler(
        client,
        initial_inflight=10,
        max_inflight=20,
    )

    fetcher = RpcFetcher(scheduler)

    # -------------------------
    # Processor Layer
    # -------------------------
    processor = EVMProcessor()

    # -------------------------
    # Kafka Layer
    # -------------------------
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    kafka_sink = KafkaSink(
        producer=producer,
        id_calculator=EventIdCalculator(),
        time_calculator=EventTimeCalculator(),
    )

    # -------------------------
    # Engine
    # -------------------------
    engine = IngestionEngine(
        fetcher=fetcher,
        processor=processor,
        sink=kafka_sink,
        topics={
            "blocks": BLOCK_TOPIC,
            "transactions": TRANSACTION_TOPIC
        }
    )
    try:
        # -------------------------
        # RUN (batch or stream)
        # -------------------------
        await engine.run_batch(START_BLOCK, END_BLOCK)

        print("Flushing Kafka...")
        producer.flush(10)
        
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())