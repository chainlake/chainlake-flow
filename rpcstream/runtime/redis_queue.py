import json
import time
from dataclasses import dataclass
from typing import Any

from rpcstream.scheduler.models import SchedulerTask


@dataclass(slots=True)
class RedisConfig:
    host: str = "127.0.0.1"
    port: int = 6379
    password: str | None = None
    db: int = 0
    decode_responses: bool = False


def build_redis_client(config: RedisConfig) -> Any:
    from redis.asyncio import Redis

    return Redis(
        host=config.host,
        port=config.port,
        password=config.password,
        db=config.db,
        decode_responses=config.decode_responses,
    )


class RedisOrderingBuffer:
    """FIFO queue using Redis List (RPUSH + BLPOP) with consume-delete semantics."""

    def __init__(self, redis: Any, key: str = "rpc:ordering_buffer"):
        self.redis = redis
        self.key = key

    async def push(self, task: SchedulerTask) -> None:
        await self.redis.rpush(self.key, json.dumps(task.to_dict()))

    async def pop(self, timeout_sec: int = 1) -> SchedulerTask | None:
        result = await self.redis.blpop(self.key, timeout=timeout_sec)
        if result is None:
            return None
        _, raw = result
        return SchedulerTask.from_raw(raw)

    async def length(self) -> int:
        return int(await self.redis.llen(self.key))


class RedisDLQ:
    """Dead letter queue for retry-exhausted tasks."""

    def __init__(self, redis: Any, key: str = "rpc:dlq"):
        self.redis = redis
        self.key = key

    async def push(self, task: SchedulerTask, error: str) -> None:
        payload = task.to_dict()
        payload["error"] = error
        payload["failed_at"] = time.time()
        await self.redis.rpush(self.key, json.dumps(payload))

    async def pop(self, timeout_sec: int = 1) -> dict[str, Any] | None:
        result = await self.redis.blpop(self.key, timeout=timeout_sec)
        if result is None:
            return None
        _, raw = result
        return json.loads(raw.decode() if isinstance(raw, bytes) else raw)

    async def length(self) -> int:
        return int(await self.redis.llen(self.key))
