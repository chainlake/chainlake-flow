import asyncio
from types import SimpleNamespace

import pytest

from rpcstream import dlq_retry


class _StopLoop(Exception):
    """Sentinel to unwind run_dlq_retry's `while True` deterministically
    once the fake client's preset messages are exhausted."""


async def _noop_async():
    return None


class FakeMessage:
    def __init__(self, value):
        self.value = value


class FakeDlqClient:
    def __init__(self, messages):
        self._messages = list(messages)
        self.committed = []

    def subscribe(self):
        pass

    def poll(self, timeout):
        if not self._messages:
            raise _StopLoop()
        return self._messages.pop(0)

    def commit(self, message):
        self.committed.append(message)

    def close(self):
        pass


class FakeEngine:
    def __init__(self):
        self.retried = []
        self.resolved = []
        self.sink = SimpleNamespace(start=_noop_async, close=_noop_async)

    async def retry_dlq_record(self, record):
        self.retried.append(record)
        return True

    async def mark_dlq_resolved(self, record):
        self.resolved.append(record)


class FakeStack:
    def __init__(self, engine, entities=("block", "transaction", "log", "token_transfer")):
        self.engine = engine
        self.runtime = SimpleNamespace(
            topic_map=SimpleNamespace(dlq="dlq.ingestion"),
            kafka=SimpleNamespace(config={}, schema_registry_url=None, schema_registry_type=None),
            entities=list(entities),
        )
        self.logger = SimpleNamespace(info=lambda *a, **k: None, warn=lambda *a, **k: None)

    async def start(self):
        return None

    async def close(self):
        return None


def test_dlq_retry_skips_records_owned_by_a_different_shard(monkeypatch):
    """With ingestion sharded by entity (separate Deployments each with a
    disjoint entities: subset), each retry worker mirrors its own shard's
    entity list. Reprocessing a DLQ record for an entity outside that list
    would resink data this worker's engine has no processor for, and would
    complete it against this worker's own watermark identity -- which the
    shard that actually owns that entity never reads, leaving its cursor
    stuck "failed" forever. A record for a foreign shard's entity must be
    left untouched (committed, not retried, not resolved) for that other
    shard's own retry worker to pick up."""
    engine = FakeEngine()
    stack = FakeStack(engine, entities=("block", "transaction"))
    monkeypatch.setattr(dlq_retry, "build_runtime_stack", lambda **kwargs: stack)

    records = [
        {"entity": "log", "cursor": 7, "status": "pending", "retry_count": 0, "max_retry": 3, "next_retry_at": None},
        {"entity": "transaction", "cursor": 8, "status": "pending", "retry_count": 0, "max_retry": 3, "next_retry_at": None},
    ]
    fake_client = FakeDlqClient([FakeMessage(r) for r in records])
    monkeypatch.setattr(dlq_retry, "UnifiedDlqKafkaClient", lambda **kwargs: fake_client)

    with pytest.raises(_StopLoop):
        asyncio.run(dlq_retry.run_dlq_retry(config={}))

    assert [r["cursor"] for r in engine.retried] == [8]
    assert [r["cursor"] for r in engine.resolved] == [8]
    assert len(fake_client.committed) == 2


def test_dlq_retry_skips_already_resolved_cursor(monkeypatch):
    """A cursor can have multiple per-entity DLQ records queued (see
    engine._send_sink_failure_dlq's history: a sink hiccup that timed out
    several entities' deliveries at once used to fan out one DLQ record per
    entity for the same cursor). retry_dlq_record/_run_one always
    reprocesses and resinks the *whole* cursor regardless of which entity a
    record names, so replaying a second record for an already-succeeded
    cursor would just re-produce duplicate Kafka messages for entities that
    already landed. The retry worker must resolve later records for a
    cursor it has already succeeded on without reprocessing them."""
    engine = FakeEngine()
    stack = FakeStack(engine)
    monkeypatch.setattr(dlq_retry, "build_runtime_stack", lambda **kwargs: stack)

    records = [
        {"entity": "block", "cursor": 42, "status": "pending", "retry_count": 0, "max_retry": 3, "next_retry_at": None},
        {"entity": "log", "cursor": 42, "status": "pending", "retry_count": 0, "max_retry": 3, "next_retry_at": None},
    ]
    fake_client = FakeDlqClient([FakeMessage(r) for r in records])
    monkeypatch.setattr(dlq_retry, "UnifiedDlqKafkaClient", lambda **kwargs: fake_client)

    with pytest.raises(_StopLoop):
        asyncio.run(dlq_retry.run_dlq_retry(config={}))

    assert [r["entity"] for r in engine.retried] == ["block"]
    assert [r["entity"] for r in engine.resolved] == ["block", "log"]
    assert len(fake_client.committed) == 2
