import asyncio
from collections import defaultdict, deque

from rpcstream.runtime.redis_queue import RedisDLQ, RedisOrderingBuffer
from rpcstream.scheduler.models import SchedulerTask


class FakeRedis:
    """Minimal async Redis double for unit-testing queue semantics."""

    def __init__(self):
        self._data = defaultdict(deque)

    async def rpush(self, key, value):
        self._data[key].append(value)

    async def blpop(self, key, timeout=0):
        if self._data[key]:
            return key, self._data[key].popleft()
        if timeout > 0:
            await asyncio.sleep(0)
        return None

    async def llen(self, key):
        return len(self._data[key])


async def _ordering_buffer_case():
    redis = FakeRedis()
    ordering = RedisOrderingBuffer(redis, key="test:ordering")

    await ordering.push(SchedulerTask(sequence=1, method="m1", params=[]))
    await ordering.push(SchedulerTask(sequence=2, method="m2", params=["x"]))

    assert await ordering.length() == 2

    first = await ordering.pop(timeout_sec=1)
    second = await ordering.pop(timeout_sec=1)

    assert first is not None and first.sequence == 1
    assert second is not None and second.sequence == 2
    assert await ordering.length() == 0


async def _dlq_case():
    redis = FakeRedis()
    dlq = RedisDLQ(redis, key="test:dlq")

    task = SchedulerTask(sequence=10, method="eth_call", params=[{"to": "0x0"}], retries=4)
    await dlq.push(task, "rpc timeout")

    assert await dlq.length() == 1

    item = await dlq.pop(timeout_sec=1)
    assert item is not None
    assert item["sequence"] == 10
    assert item["error"] == "rpc timeout"
    assert await dlq.length() == 0


def test_ordering_buffer_fifo_and_length():
    asyncio.run(_ordering_buffer_case())


def test_dlq_push_pop_and_length():
    asyncio.run(_dlq_case())
