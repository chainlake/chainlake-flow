import asyncio
import json
import aiohttp

from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.adapters.sui.rpc_requests import build_get_latest_checkpoint
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.protocol.request import BaseRpcRequest


# RPC_URL = "https://sui.blockpi.network/v1/rpc/public"
RPC_URL = "https://fullnode.mainnet.sui.io:443"
# --------------------------
# Run the test
# --------------------------
async def main():
    client = JsonRpcClient(RPC_URL, timeout_sec=5)
    scheduler = AdaptiveRpcScheduler(client, min_inflight=1, max_inflight=1, latency_target_ms=500)

    try:
      # Build the request
      req: BaseRpcRequest = build_get_latest_checkpoint()

      # Submit the request via scheduler
      result, meta = await scheduler.submit(req)

      # Pretty print
      print("Result:")
      print(json.dumps(result, indent=2))
      print("Meta:", meta)
    except Exception as e:
      print(e)
    finally:
      await client.close()

if __name__ == "__main__":
    asyncio.run(main())
