import json

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

import asyncio
import time
import statistics
import json
import os

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.client.models import RpcErrorResult
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.adapters.evm.rpc_requests import build_trace_block

from rpcstream.adapters.evm.parser import (
    parse_blocks,
    parse_transactions,
    parse_receipts,
    parse_traces
)

RPC_URL = "http://localhost:30040/main/evm/1" # eRPC endpoint
START_BLOCK = 24867589
END_BLOCK = 24867589
BLOCK_BATCH_SIZE = 10
INITIAL_CONCURRENT = 10
MAX_INFLIGHT = 50

# LOG LEVEL: debug / info / stats
LOG_LEVEL = os.getenv("LOG_LEVEL", "debug").lower()


def percentile(data, p):
    if not data:
        return 0
    data = sorted(data)
    k = int(len(data) * p / 100)
    k = min(k, len(data) - 1)
    return data[k]


async def main():
    client = JsonRpcClient(RPC_URL, timeout_sec=10)

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
            req = build_trace_block(block_number)
            result = await scheduler.submit_request(req)
            # print(f"[Block {block_number}] result = {result}")

        except Exception as e:
            print(f"[Block {block_number}] EXCEPTION during submit: {e}")
            error_count += 1
            return

        if isinstance(result, RpcErrorResult):
            print(f"[Block {block_number}] ERROR: {result.error}")
            error_count += 1
            return

        value, meta = result
        
        # Safety Debug
        if not isinstance(value, (list, dict)):
            print(f"[Block {block_number}] unexpected value type: {type(value)}")
            return
                
        latency = meta.extra.get("latency_ms", 0)
        queue_wait = meta.extra.get("queue_wait_ms", 0)

        if isinstance(value, list):
            receipt_count = len(value)
            logs_count = 0  # trace_block doesn't have logs
        else:
            receipt_count = len(value.get("transactions", []))
            logs_count = sum(len(tx.get("logs", [])) for tx in value.get("transactions", []))
            
        payload_size = len(json.dumps(value))
        payload_kb = payload_size / 1024

        success_latencies.append(latency)
        queue_waits.append(queue_wait)
        payload_sizes.append(payload_size)
        receipt_counts.append(receipt_count)
        logs_counts.append(logs_count)

        block_details.append(
            {
                "block": block_number,
                "latency": latency,
                "queue_wait": queue_wait,
                "transactions": receipt_count,
                "payload_kb": payload_kb,
            }
        )

        if LOG_LEVEL in ["debug", "info"]:
            print(
                f"[Block {block_number}] "
                f"OK latency={latency:.2f}ms "
                f"queue_wait={queue_wait:.2f}ms "
                f"transactions={receipt_count} payload={payload_kb:.1f}KB"
            )

        # -------------------------
        # NEW: PARSE + SINK
        # -------------------------
        try:
            trace_row = parse_traces(value, block_number)
            
            print(f"[Block {block_number}] parsed rows = {len(trace_row)}")
            
            if trace_row:
                write_jsonl("output/traces.jsonl", trace_row)
            else:
                print(f"[Block {block_number}] empty trace rows")

        except Exception as e:
            print(f"[Block {block_number}] parse error: {e}")

    async def telemetry_sampler():
        while True:
            telemetry_snapshots.append(scheduler.telemetry())
            await asyncio.sleep(0.5)

    sampler = asyncio.create_task(telemetry_sampler())

    try:
        tasks = [asyncio.create_task(task(b)) for b in range(START_BLOCK, END_BLOCK + 1)]
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        sampler.cancel()
        await client.close()

    # -------------------------
    # GLOBAL METRICS
    # -------------------------
    elapsed_sec = time.time() - start_ts
    elapsed_ms = elapsed_sec * 1000

    total_requests = len(success_latencies) + error_count
    total_bytes = sum(payload_sizes)
    total_receipts = sum(receipt_counts)
    total_logs = sum(logs_counts)

    rps = total_requests / elapsed_sec
    bps = rps
    receipts_per_sec = total_receipts / elapsed_sec
    logs_per_sec = total_logs / elapsed_sec
    mb_sec = total_bytes / elapsed_sec / 1024 / 1024

    avg_latency = statistics.mean(success_latencies) if success_latencies else 0
    min_latency = min(success_latencies) if success_latencies else 0
    max_latency = max(success_latencies) if success_latencies else 0

    p50 = percentile(success_latencies, 50)
    p95 = percentile(success_latencies, 95)
    p99 = percentile(success_latencies, 99)

    avg_queue = statistics.mean(queue_waits) if queue_waits else 0
    avg_receipts = statistics.mean(receipt_counts) if receipt_counts else 0
    avg_logs = statistics.mean(logs_counts) if logs_counts else 0
    avg_payload_kb = statistics.mean(payload_sizes) / 1024 if payload_sizes else 0

    print("\n==============================")
    print(" GLOBAL METRICS")
    print("==============================")
    print(f"Total requests      : {total_requests}")
    print(f"Success             : {len(success_latencies)}")
    print(f"Errors              : {error_count}")
    print(f"Total elapsed       : {elapsed_ms:.2f} ms ({elapsed_sec:.3f} s)")
    print(f"RPS                 : {rps:.2f}")
    print(f"Blocks/sec          : {bps:.2f}")
    print(f"Transactions/sec    : {receipts_per_sec:.2f}")
    print(f"MB/sec              : {mb_sec:.2f}")
    print("\nLatency (ms)")
    print(f"avg                 : {avg_latency:.2f}")
    print(f"min                 : {min_latency:.2f}")
    print(f"max                 : {max_latency:.2f}")
    print(f"p50                 : {p50:.2f}")
    print(f"p95                 : {p95:.2f}")
    print(f"p99                 : {p99:.2f}")
    print("\nQueue wait")
    print(f"avg queue_wait      : {avg_queue:.2f} ms")
    print("\nPayload")
    print(f"avg transactions    : {avg_receipts:.2f}")
    print(f"avg payload         : {avg_payload_kb:.2f} KB")

    if LOG_LEVEL in ["debug", "info"]:
        print("\n==============================")
        print(" TOP 5 SLOWEST BLOCKS")
        print("==============================")
        slowest = sorted(block_details, key=lambda x: x["latency"], reverse=True)[:5]
        for b in slowest:
            print(b)

        print("\n==============================")
        print(" TOP 5 HEAVIEST PAYLOAD")
        print("==============================")
        heaviest = sorted(block_details, key=lambda x: x["payload_kb"], reverse=True)[:5]
        for b in heaviest:
            print(b)

        print("\n==============================")
        print(" FINAL SCHEDULER TELEMETRY")
        print("==============================")
        print(scheduler.telemetry())

        print("\n==============================")
        print(" FINAL CLIENT TELEMETRY")
        print("==============================")
        print(client.telemetry())


if __name__ == "__main__":
    asyncio.run(main())