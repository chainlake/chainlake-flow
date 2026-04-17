import asyncio
from rpcstream.runtime.block_tracker import BlockHeadTracker
from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.utils.logger import JsonLogger


RPC_URL = "http://localhost:30040/main/evm/56"  # erpc endpoint


async def main():
    logger = JsonLogger(name="tracker_test", level="debug")

    client = JsonRpcClient(RPC_URL)

    tracker = BlockHeadTracker(
        client=client,
        poll_interval=0.2,
        logger=logger,
    )

    await tracker.start()

    print("🚀 Tracker started...")

    last_block = None

    for _ in range(20):  # run ~4 seconds
        latest = tracker.get_latest()

        if latest is not None:
            print(f"Latest block: {latest}")

            if last_block is not None:
                if latest < last_block:
                    print("❌ ERROR: block number decreased!")

                elif latest == last_block:
                    print("⏳ no new block yet")

                else:
                    print(f"✅ new block detected (+{latest - last_block})")

            last_block = latest

        else:
            print("⏳ waiting for first block...")

        await asyncio.sleep(0.2)

    await tracker.stop()
    print("🛑 Tracker stopped")


if __name__ == "__main__":
    asyncio.run(main())