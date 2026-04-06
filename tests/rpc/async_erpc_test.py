import asyncio
import time
from blockchain_ingestion.rpc.erpc_client import ErpcClient, AdaptiveErpcScheduler, RpcErrorResult

ERPC_URL = "http://erpc-service.erpc:4000/main/evm/56"
START_BLOCK = 90000001
END_BLOCK = 90000100
INITIAL_CONCURRENT = 20
MAX_INFLIGHT = 50

async def main():
    # 1️⃣ 初始化客户端和 Scheduler
    client = ErpcClient(ERPC_URL, timeout_sec=5)
    scheduler = AdaptiveErpcScheduler(
        client,
        initial_inflight=INITIAL_CONCURRENT,
        max_inflight=MAX_INFLIGHT,
    )

    success_latencies = []
    error_count = 0
    start_ts = time.time()

    async def task(block_number: int):
        nonlocal error_count
        method = "eth_getBlockByNumber"
        params = [hex(block_number), True]  # BSC 兼容 ETH RPC
        meta_extra = {"block_number": block_number}

        result = await scheduler.submit(method, params, meta_extra)
        if isinstance(result, RpcErrorResult):
            print(f"[Block {block_number}] ERROR: {result.error}")
            error_count += 1
        else:
            value, meta = result
            latency = meta.extra.get("latency_ms", 0)
            success_latencies.append(latency)
            print(f"[Block {block_number}] OK, latency={latency}ms, hash={value.get('hash')}")

    # 2️⃣ 并发调度所有 block
    tasks = [asyncio.create_task(task(b)) for b in range(START_BLOCK, END_BLOCK + 1)]
    await asyncio.gather(*tasks)

    # 3️⃣ 全局统计
    elapsed = max(time.time() - start_ts, 1)
    total_requests = len(success_latencies) + error_count
    rps = total_requests / elapsed
    avg_latency = sum(success_latencies) / len(success_latencies) if success_latencies else 0
    max_latency = max(success_latencies) if success_latencies else 0
    min_latency = min(success_latencies) if success_latencies else 0

    print("\n=== Global Metrics ===")
    print(f"Total requests: {total_requests}")
    print(f"Success: {len(success_latencies)}, Errors: {error_count}")
    print(f"RPS: {rps:.2f}")
    print(f"Latency ms: avg={avg_latency:.2f}, min={min_latency:.2f}, max={max_latency:.2f}")

    print("\n=== Scheduler telemetry ===")
    print(scheduler.telemetry())
    print("\n=== Client telemetry ===")
    print(client.telemetry())

    # 4️⃣ 关闭客户端
    await client.close()

if __name__ == "__main__":
    # asyncio.run(main())
    await main()