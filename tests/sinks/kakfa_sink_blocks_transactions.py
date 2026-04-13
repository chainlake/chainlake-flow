import asyncio
import time
import statistics
import json
import os
from confluent_kafka import Producer

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.client.models import RpcErrorResult
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number

from rpcstream.adapters.evm.parser import (
    parse_blocks,
    parse_transactions,
    parse_receipts
)

RPC_URL = "http://localhost:30040/main/evm/56" # eRPC endpoint
KAFKA_BROKER = "localhost:30092"
# Kafka Topics
CHAIN = 'bsc'
BLOCK_TOPIC = f"{CHAIN}.raw_blocks"
TRANSACTION_TOPIC = f"{CHAIN}.raw_transactions"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

def produce_json(topic, rows):
    for r in rows:
        producer.produce(
            topic=topic,
            value=json.dumps(r)
        )
    producer.poll(0)  # trigger delivery

START_BLOCK = 90000096
END_BLOCK = 90000100
BLOCK_BATCH_SIZE = 10
INITIAL_CONCURRENT = 10
MAX_INFLIGHT = 50

# LOG LEVEL: debug / info / stats
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()

def percentile(data, p):
    if not data:
        return 0
    data = sorted(data)
    k = int(len(data) * p / 100)
    k = min(k, len(data) - 1)
    return data[k]


async def main():
    client = JsonRpcClient(RPC_URL, timeout_sec=5)

    scheduler = AdaptiveRpcScheduler(
        client,
        initial_inflight=INITIAL_CONCURRENT,
        max_inflight=MAX_INFLIGHT,
    )

    success_latencies = []
    queue_waits = []
    payload_sizes = []
    receipt_counts = []
    logs_counts = []

    block_details = []
    error_count = 0
    telemetry_snapshots = []

    start_ts = time.time()

    async def task(block_number: int):
        nonlocal error_count

        if LOG_LEVEL == "debug":
            print(f"[Block {block_number}] submitting...")

        try:
            # build eth_getBlockByNumber, include transactions and request_id
            req = build_get_block_by_number(block_number)
            result = await scheduler.submit_request(req)

        except Exception as e:
            print(f"[Block {block_number}] EXCEPTION during submit: {e}")
            error_count += 1
            return

        if isinstance(result, RpcErrorResult):
            print(f"[Block {block_number}] ERROR: {result.error}")
            error_count += 1
            return

        value, meta = result
                
        latency = meta.extra.get("latency_ms", 0)
        queue_wait = meta.extra.get("queue_wait_ms", 0)

        receipt_count = len(value.get("transactions", []))
        logs_count = sum(len(tx.get("logs", [])) for tx in value.get("transactions", []))
        payload_size = len(json.dumps(value))
        payload_kb = payload_size / 1024

        if LOG_LEVEL in ["debug", "info"]:
            print(
                f"[Block {block_number}] "
                f"OK latency={latency:.2f}ms "
                f"queue_wait={queue_wait:.2f}ms "
                f"transactions={receipt_count} "
                f"payload={payload_kb:.1f}KB "
                f"transaction_count={logs_count}"
            )

        # -------------------------
        # UPDATED: PARSE + SINK
        # -------------------------
        try:
            block_row = parse_blocks(value)
            tx_rows = parse_transactions(value)

            produce_json(BLOCK_TOPIC, [block_row])
            produce_json(TRANSACTION_TOPIC, tx_rows)

        except Exception as e:
            print(f"[Block {block_number}] parse error: {e}")

    async def telemetry_sampler():
        while True:
            await asyncio.sleep(0.5)

    try:
        tasks = [asyncio.create_task(task(b)) for b in range(START_BLOCK, END_BLOCK + 1)]
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await client.close()
        producer.flush()

if __name__ == "__main__":
    asyncio.run(main())