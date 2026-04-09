import asyncio
import json
from rpcstream.adapters.sui.rpc_requests import (
    build_get_total_transactions,
    batch_get_checkpoints,
)
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler

# --------------------------
# Dummy RPC client for testing
# --------------------------
class DummySuiClient:
    async def call(self, method, params):
        # Just return a dummy payload with method and params
        return {
            "method": method,
            "params": params,
            "result": f"dummy_result_for_{method}_{params}",
        }

# --------------------------
# Test scheduler
# --------------------------
async def main():
    client = DummySuiClient()
    scheduler = AdaptiveRpcScheduler(client, min_inflight=1, max_inflight=5, latency_target_ms=100)

    # Single request test
    req = build_get_total_transactions()
    result, meta = await scheduler.submit(req)
    print("Single request output:")
    print(json.dumps(result, indent=2))
    print("Meta:", meta)
    print("-----------")

    # Batch request test
    batch = batch_get_checkpoints(100, 105)
    print("Batch request outputs:")
    for req in batch:
        res, meta = await scheduler.submit(req)
        print(json.dumps(res, indent=2))
        print("Meta:", meta)
        print("-----------")

    # 

# --------------------------
# Run the test
# --------------------------
if __name__ == "__main__":
    asyncio.run(main())