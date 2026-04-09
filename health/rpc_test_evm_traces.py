import asyncio
import time
import json
import os
import statistics

from rpcstream.rpc.rpc_client import RpcClient
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.rpc.models import RpcErrorResult
from rpcstream.adapters.evm.rpc_requests import build_trace_block

RPC_URL = "http://localhost:30040/main/evm/56"
START_BLOCK = 90000091
END_BLOCK = 90000100
INITIAL_CONCURRENT = 5
MAX_INFLIGHT = 10
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()


def percentile(data, p):
    if not data:
        return 0
    data = sorted(data)
    k = int(len(data) * p / 100)
    k = min(k, len(data) - 1)
    return data[k]


async def main():
    client = RpcClient(RPC_URL, timeout_sec=30)
    scheduler = AdaptiveRpcScheduler(
        client,
        initial_inflight=INITIAL_CONCURRENT,
        max_inflight=MAX_INFLIGHT,
    )

    success_latencies = []
    queue_waits = []
    payload_sizes = []
    block_details = []
    error_count = 0

    start_ts = time.time()

    async def task(block_number: int):
        nonlocal error_count

        if LOG_LEVEL == "debug":
            print(f"[Block {block_number}] submitting...")

        try:
            req = build_trace_block(block_number)
            result = await scheduler.submit(req)
            
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
        payload_str = json.dumps(value)
        payload_size = len(payload_str)
        payload_kb = payload_size / 1024

        success_latencies.append(latency)
        queue_waits.append(queue_wait)
        payload_sizes.append(payload_size)

        block_details.append(
            {
                "block": block_number,
                "latency": latency,
                "queue_wait": queue_wait,
                "payload_kb": payload_kb,
            }
        )

        # print block level info
        print(
            f"[Block {block_number}] "
            f"OK latency={latency:.2f}ms "
            f"queue_wait={queue_wait:.2f}ms "
            f"payload={payload_kb:.1f}KB "
        )

    # async tasks block
    tasks_list = [asyncio.create_task(task(b)) for b in range(START_BLOCK, END_BLOCK + 1)]
    await asyncio.gather(*tasks_list)
    await client.close()

    # -------------------------
    # GLOBAL METRICS
    # -------------------------
    elapsed_sec = time.time() - start_ts
    elapsed_ms = elapsed_sec * 1000

    total_requests = len(success_latencies) + error_count
    total_bytes = sum(payload_sizes)

    rps = total_requests / elapsed_sec
    mb_sec = total_bytes / elapsed_sec / 1024 / 1024

    avg_latency = statistics.mean(success_latencies) if success_latencies else 0
    min_latency = min(success_latencies) if success_latencies else 0
    max_latency = max(success_latencies) if success_latencies else 0

    p50 = percentile(success_latencies, 50)
    p95 = percentile(success_latencies, 95)
    p99 = percentile(success_latencies, 99)

    avg_queue = statistics.mean(queue_waits) if queue_waits else 0
    avg_payload_kb = statistics.mean(payload_sizes) / 1024 if payload_sizes else 0

    print("\n==============================")
    print(" GLOBAL METRICS")
    print("==============================")
    print(f"Total requests      : {total_requests}")
    print(f"Success             : {len(success_latencies)}")
    print(f"Errors              : {error_count}")
    print(f"Total elapsed       : {elapsed_ms:.2f} ms ({elapsed_sec:.3f} s)")
    print(f"RPS                 : {rps:.2f}")
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
    print(f"avg payload         : {avg_payload_kb:.2f} KB")

    if LOG_LEVEL in ["debug", "info"]:
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