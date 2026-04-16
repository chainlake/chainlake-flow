import asyncio

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.planner.block_source import BackfillBlockSource
from rpcstream.utils.logger import JsonLogger


class DummyFetcher:
    def __init__(self):
        self.pipeline_type = "test"

    async def fetch(self, block):
        return (
            {"block": block, "data": f"block-{block}"},
            type("meta", (), {"extra": {"latency_ms": 10, "queue_wait_ms": 0}})(),
        )


class DummyProcessor:
    def process(self, pipeline_type, block, value):
        return {
            "blocks": [
                {"block": block, "value": value}
            ]
        }


class DummySink:
    async def start(self):
        print("sink started")

    async def send(self, topic, rows):
        print(f"[KAFKA] {topic} -> {rows}")

    async def close(self):
        print("sink closed")


async def main():
    source = BackfillBlockSource(start=100, end=105)

    engine = IngestionEngine(
        fetcher=DummyFetcher(),
        processor=DummyProcessor(),
        sink=DummySink(),
        topics={"blocks": "test.topic.blocks"},
        concurrency=5,
        logger=JsonLogger(level="info"),
    )

    await engine.run_stream(source)


if __name__ == "__main__":
    asyncio.run(main())