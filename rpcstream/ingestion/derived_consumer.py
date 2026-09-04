"""Derived pipeline: reads bsc.raw_envelope, parses all EVM entities.

DerivedEnvelopeFetcher implements both the CursorSource and Fetcher protocols
so it can be passed as both cursor_source and engine.fetcher to IngestionEngine.

Flow per block:
  next_cursor() → asyncio.to_thread(_poll_and_parse_message) →
                  cache (block_json, receipts_json) → return block_number
  fetch(cursor) → retrieve cached payload → {"block_envelope": ((b, r), meta)}
  DerivedEnvelopeProcessor.process() → {"block", "transaction", "receipt", "log"}
  EvmDecoder.decode() → adds "token_transfer"
  EvmEnricher.enrich() → receipt fields on transactions, block context on logs

Throughput note: json.loads on 1–3 MB of raw_envelope JSON is CPU-bound Python
that blocks the event loop for ~100–200 ms/block. Moving poll + json.loads into
a thread via asyncio.to_thread allows the event loop to run decode/enrich/sink
tasks for the previous block while the next block is being parsed. For N×
throughput, use cursor-range sharding: run N derived pods each covering
1/N of the block range (--from-block / --to-block on ingest-derived).
"""
from __future__ import annotations

import asyncio
import json
import time

from confluent_kafka import Consumer, KafkaError

from rpcstream.client.models import RpcTaskMeta
from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions
from rpcstream.adapters.evm.parser.parse_receipts_logs import parse_receipts


# Producer-only librdkafka keys that break the Consumer constructor.
_PRODUCER_ONLY_KEYS = frozenset({
    "linger.ms",
    "batch.size",
    "compression.type",
    "enable.idempotence",
    "acks",
    "retries",
    "max.in.flight.requests.per.connection",
    "queue.buffering.max.messages",
    "queue.buffering.max.kbytes",
    "transaction.timeout.ms",
})


def _build_consumer_config(kafka_config: dict, group_id: str) -> dict:
    config = {k: v for k, v in kafka_config.items()
              if not k.startswith("transactional") and k not in _PRODUCER_ONLY_KEYS}
    config["group.id"] = group_id
    config["auto.offset.reset"] = "earliest"
    config["enable.auto.commit"] = False
    return config


def _poll_and_parse_message(consumer: Consumer, timeout: float) -> tuple:
    """Poll ONE raw_envelope message and fully JSON-decode it.

    Runs in a thread pool (via asyncio.to_thread) so the heavy json.loads calls
    on 1–3 MB of block+receipts JSON do not block the asyncio event loop.
    No asyncio primitives used here — pure synchronous code.

    Return tags:
      ("none",)                                          — poll timeout
      ("eof",)                                           — PARTITION_EOF
      ("kafka_error", err)                               — other Kafka error
      ("parse_error", offset, exc)                       — outer JSON failed
      ("missing_fields", block_number, has_b, has_r)    — envelope incomplete
      ("json_error", block_number, exc)                  — inner JSON failed
      ("ok", block_number, block_json_dict, receipts_list)
    """
    msg = consumer.poll(timeout)
    if msg is None:
        return ("none",)
    if msg.error():
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            return ("eof",)
        return ("kafka_error", err)
    try:
        raw = msg.value()
        if not raw:
            return ("none",)
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return ("parse_error", msg.offset(), exc)

    block_number = payload.get("block_number")
    if block_number is None:
        key = msg.key()
        if key:
            try:
                block_number = int(key.decode("utf-8"))
            except Exception:
                pass
    if block_number is None:
        return ("none",)
    block_number = int(block_number)

    block_json_str = payload.get("block_json")
    receipts_json_str = payload.get("receipts_json")
    if not block_json_str or receipts_json_str is None:
        return (
            "missing_fields",
            block_number,
            bool(block_json_str),
            receipts_json_str is not None,
        )

    try:
        block_json = json.loads(block_json_str)
        receipts_json = json.loads(receipts_json_str)
    except Exception as exc:
        return ("json_error", block_number, exc)

    return ("ok", block_number, block_json, receipts_json)


class DerivedEnvelopeProcessor:
    """Parses a (block_json, receipts_json) tuple into all EVM entity rows.

    Returns receipt rows so EvmEnricher can join them onto transactions.
    Receipt has no topic in the derived topic-map so it stays internal.
    """

    def process(self, cursor: int, value: tuple) -> dict:
        block_json, receipts_json = value
        parsed_block = parse_blocks(block_json)
        txs = parse_transactions(block_json)
        receipts, logs = parse_receipts(receipts_json)
        return {
            "block": [parsed_block],
            "transaction": txs,
            "receipt": receipts,
            "log": logs,
        }


class DerivedEnvelopeFetcher:
    """Kafka consumer-backed cursor source and fetcher for the derived pipeline.

    Pass the same instance as *both* cursor_source and engine.fetcher:

        source = DerivedEnvelopeFetcher(...)
        engine = IngestionEngine(fetcher=source, ...)
        await engine.run_stream(source, ...)

    next_cursor() offloads poll + json.loads to a thread via asyncio.to_thread,
    freeing the event loop during the JSON decode of large raw_envelope messages.
    fetch() is called by engine workers (potentially concurrent) and retrieves
    the already-decoded payload from _pending; dict access is safe because all
    callers share the same event loop (no true concurrency).
    """

    def __init__(
        self,
        kafka_config: dict,
        source_topic: str,
        *,
        group_id: str,
        from_block: int | None = None,
        to_block: int | None = None,
        logger=None,
        poll_timeout_sec: float = 1.0,
    ):
        consumer_config = _build_consumer_config(kafka_config, group_id)
        self._consumer = Consumer(consumer_config)
        self._source_topic = source_topic
        self._from_block = from_block
        self._to_block = to_block
        self._logger = logger
        self._poll_timeout_sec = poll_timeout_sec
        # block_number → (block_json, receipts_json, RpcTaskMeta)
        # Bounded by engine.sink_inflight_cursors (≤ 20 entries).
        self._pending: dict[int, tuple] = {}
        self._subscribed = False
        # IngestionEngine inspects these via getattr() for lag / circuit-breaker.
        self.scheduler = None
        self.tracker = None

    def _subscribe(self) -> None:
        self._consumer.subscribe([self._source_topic])
        self._subscribed = True
        if self._logger:
            self._logger.info(
                "derived_consumer.subscribed",
                topic=self._source_topic,
                from_block=self._from_block,
                to_block=self._to_block,
            )

    async def next_cursor(self) -> int | None:
        """Consume the next in-range message; return its block_number as cursor.

        Poll + json.loads run in a thread pool to avoid blocking the event loop
        during the expensive decode of large raw_envelope payloads.
        Returns None when to_block is reached (bounded/backfill mode ends).
        """
        if not self._subscribed:
            self._subscribe()

        while True:
            result = await asyncio.to_thread(
                _poll_and_parse_message, self._consumer, self._poll_timeout_sec
            )
            kind = result[0]

            if kind == "none":
                await asyncio.sleep(0)
                continue

            if kind == "eof":
                if self._to_block is not None:
                    await asyncio.sleep(0.05)
                continue

            if kind == "kafka_error":
                if self._logger:
                    self._logger.error(
                        "derived_consumer.kafka_error",
                        error=str(result[1]),
                        code=result[1].code(),
                        topic=self._source_topic,
                    )
                continue

            if kind == "parse_error":
                if self._logger:
                    self._logger.warn(
                        "derived_consumer.parse_error",
                        offset=result[1],
                        error=repr(result[2]),
                    )
                continue

            if kind == "missing_fields":
                _, block_number, has_block, has_receipts = result
                if self._logger:
                    self._logger.warn(
                        "derived_consumer.missing_fields",
                        block_number=block_number,
                        has_block=has_block,
                        has_receipts=has_receipts,
                    )
                continue

            if kind == "json_error":
                _, block_number, exc = result
                if self._logger:
                    self._logger.warn(
                        "derived_consumer.json_error",
                        block_number=block_number,
                        error=repr(exc),
                    )
                continue

            # kind == "ok"
            _, block_number, block_json, receipts_json = result

            if self._from_block is not None and block_number < self._from_block:
                continue
            if self._to_block is not None and block_number > self._to_block:
                try:
                    self._consumer.commit(asynchronous=False)
                except Exception:
                    pass
                return None

            meta = RpcTaskMeta(
                task_id=block_number,
                submit_ts=time.time(),
                extra={
                    "latency_ms": 0.0,
                    "queue_wait_ms": 0.0,
                    "inflight": 0,
                    "rpc_calls": 0,
                    "source": "kafka_envelope",
                },
            )
            self._pending[block_number] = (block_json, receipts_json, meta)
            return block_number

    async def fetch(self, cursor: int) -> dict:
        """Return raw data for the engine's processor loop.

        next_cursor() always populates _pending before the engine worker calls
        fetch(), so the spin-wait exits immediately in the common case.
        """
        cursor = int(cursor)
        while cursor not in self._pending:
            await asyncio.sleep(0)
        block_json, receipts_json, meta = self._pending.pop(cursor)
        return {"block_envelope": ((block_json, receipts_json), meta)}

    def close(self) -> None:
        try:
            self._consumer.commit(asynchronous=False)
        except Exception:
            pass
        try:
            self._consumer.close()
        except Exception:
            pass
