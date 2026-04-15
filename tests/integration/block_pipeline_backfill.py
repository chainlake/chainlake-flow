import asyncio

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.ingestion.fetcher import RpcFetcher
from rpcstream.ingestion.processor import EVMProcessor
from rpcstream.sinks.kafka.producer import KafkaSink

from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator

from confluent_kafka import Producer

from rpcstream.utils.config_loader import load_pipeline_config
from rpcstream.utils.topic_builder import build_topic, build_dlq_topic
from rpcstream.utils.logger import JsonLogger

from dotenv import load_dotenv
from pathlib import Path
import os

# go up to repo root
env_path = Path(__file__).resolve().parents[3] / ".env"
load_dotenv()

def build_kafka_config(config):
    kafka_cfg = config["kafka"]

    username = os.getenv(kafka_cfg["auth"]["username_env"])
    password = os.getenv(kafka_cfg["auth"]["password_env"])

    ca_path = os.getenv("KAFKA_CA_PATH")

    return {
        "bootstrap.servers": kafka_cfg["bootstrap_servers"],

        # SASL config
        "security.protocol": kafka_cfg["security"]["protocol"],
        "sasl.mechanism": kafka_cfg["security"]["mechanism"],
        "sasl.username": username,
        "sasl.password": password,

        # producer tuning
        "linger.ms": kafka_cfg["producer"]["linger_ms"],
        "batch.size": kafka_cfg["producer"]["batch_size"],
        
        "ssl.ca.location": ca_path,
    }

async def main():
    # -------------------------
    # LOAD YAML CONFIG
    # -------------------------
    config = load_pipeline_config("block_pipeline_backfill.yaml")

    logger = JsonLogger(level=config["log"]["level"])
    adapter_type = config["adapter"]["type"]
    pipeline_type = config["pipeline"]["type"]
    chain = config["adapter"]["chain"]
    network = config["adapter"]["network"]
    schemas = config["schema"]["entities"]
    rpc_conf = config["rpc"]

    # -------------------------
    # BUILD TOPICS DYNAMICALLY
    # -------------------------
    TOPICS = {
        schema.rstrip("s"): build_topic(
            adapter_type,
            chain,
            network,
            schema
        )
        for schema in schemas
    }

    DLQ_TOPICS = {
        schema.rstrip("s"): build_dlq_topic(
            adapter_type,
            chain,
            network,
            schema
        )
        for schema in schemas
    }
    # -------------------------
    # RPC
    # -------------------------
    client = JsonRpcClient(
        rpc_conf["endpoint"], 
        timeout_sec=rpc_conf["timeout_sec"],
        max_retries=0,
        logger=logger
        )

    scheduler = AdaptiveRpcScheduler(
        client,
        initial_inflight=rpc_conf["inflight"]["initial"],
        max_inflight=rpc_conf["inflight"]["max"],
        latency_target_ms=rpc_conf["inflight"]["target"],
        logger=logger,
    )

    fetcher = RpcFetcher(scheduler, pipeline_type, logger)

    # -------------------------
    # PROCESSOR
    # -------------------------
    processor = EVMProcessor(logger)

    # -------------------------
    # KAFKA
    # -------------------------
    producer = Producer(build_kafka_config(config))

    kafka_sink = KafkaSink(
        producer=producer,
        id_calculator=EventIdCalculator(),
        time_calculator=EventTimeCalculator(),
        logger=logger,
    )

    # -------------------------
    # ENGINE
    # -------------------------
    engine = IngestionEngine(
        fetcher=fetcher,
        processor=processor,
        sink=kafka_sink,
        topics=TOPICS,
        concurrency=config["engine"]["concurrency"],
        logger=logger,
    )

    try:
        await engine.run_batch(
            config["range"]["start_block"],
            config["range"]["end_block"]
        )

    finally:
        kafka_sink.flush()
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())