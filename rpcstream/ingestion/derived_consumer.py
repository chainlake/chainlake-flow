"""Derived pipeline: reads bsc.raw_envelope, parses all EVM entities.

DerivedEnvelopeFetcher implements both the CursorSource and Fetcher protocols
so it can be passed as both cursor_source and engine.fetcher to IngestionEngine.

Flow per block:
  _prefetch_loop() polls Kafka in background, enqueues ("ok", block_number, payload)
  next_cursor() reads from _prefetch_q (no thread overhead) -> returns block_number
  fetch(cursor) -> retrieve cached payload -> {"block_envelope": (payload, meta)}
  DerivedEnvelopeProcessor.process() -> {"block", "transaction", "receipt", "log"}
  EvmDecoder.decode() -> adds "token_transfer"
  EvmEnricher.enrich() -> receipt fields on transactions, block context on logs

Parsing path (fast, when chainlake_avro Rust extension is available):
  _poll_and_parse_message calls chainlake_avro.parse_block_envelope(str, str)
  which runs serde_json inside py.allow_threads() -- GIL released, true parallel
  execution across all engine worker threads (~1-2 ms/block vs ~300 ms Python).

Fallback path (if Rust extension unavailable):
  json.loads inside thread + Python parse_blocks/parse_transactions/parse_receipts
  in DerivedEnvelopeProcessor.process() (original behavior).
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

try:
    import chainlake_avro as _chainlake_avro
    _RUST_PARSER = hasattr(_chainlake_avro, "parse_block_envelope")
    _RUST_ENCODE = _RUST_PARSER and hasattr(_chainlake_avro, "parse_and_encode_block_envelope")
except ImportError:
    _chainlake_avro = None  # type: ignore[assignment]
    _RUST_PARSER = False
    _RUST_ENCODE = False

# entity -> (schema_id: int, topic: str) -- set by derived_runtime after schema
# registry warmup via set_rust_encode_config(). None means encode path inactive.
_RUST_ENCODE_CONFIG: dict | None = None


def set_rust_encode_config(entity_schema_ids: dict) -> None:
    """Activate the GIL-free parse+encode path with the given schema_id map."""
    global _RUST_ENCODE_CONFIG
    _RUST_ENCODE_CONFIG = entity_schema_ids


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


def _encode_block_envelope_sync(
    block_json_str: str, receipts_json_str: str, config: dict
) -> dict:
    """Call parse_and_encode_block_envelope from a thread pool worker.

    Called via asyncio.to_thread from DerivedEnvelopeFetcher.fetch() so that
    each engine worker encodes its own block concurrently. The Rust function
    releases the GIL inside allow_threads(), so 8 workers encode on 8 OS
    threads in true parallel -- throughput scales with CPU cores.
    """
    return _chainlake_avro.parse_and_encode_block_envelope(
        block_json_str, receipts_json_str, config
    )


def _poll_and_parse_message(consumer: Consumer, timeout: float) -> tuple:
    """Poll ONE raw_envelope message and return its raw JSON strings.

    Runs in a thread pool (via asyncio.to_thread). No asyncio primitives used.

    For the Rust encode path (_RUST_ENCODE), this function is deliberately
    lightweight -- it only polls Kafka and parses the outer envelope JSON.
    The heavy work (JSON parse + Avro encode) is deferred to fetch() so that
    each of the 8 engine workers encodes its own block concurrently instead
    of the single producer coroutine serializing all encodes.

    Return tags:
      ("none",)                                           -- poll timeout
      ("eof",)                                            -- PARTITION_EOF
      ("kafka_error", err)                                -- other Kafka error
      ("parse_error", offset, exc)                        -- outer JSON failed
      ("missing_fields", block_number, has_b, has_r)     -- envelope incomplete
      ("json_error", block_number, exc)                   -- inner parse failed
      ("ok", block_number, payload)
          payload is (block_json_str, receipts_json_str)  -- Rust encode path
                                                            (encode deferred to fetch)
          payload is dict {"block","transaction",...}      -- Rust parse path
          payload is (block_json_dict, receipts_list)      -- Python fallback
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

    if _RUST_ENCODE and _RUST_ENCODE_CONFIG:
        # Return raw JSON strings. Encoding is deferred to fetch() so each
        # engine worker encodes its own block concurrently in a thread pool
        # (8 workers x GIL-free Rust = true parallel encode on all cores).
        return ("ok", block_number, (block_json_str, receipts_json_str))

    if _RUST_PARSER:
        try:
            parsed = _chainlake_avro.parse_block_envelope(block_json_str, receipts_json_str)
        except Exception as exc:
            return ("json_error", block_number, exc)
        return ("ok", block_number, parsed)

    try:
        block_json = json.loads(block_json_str)
        receipts_json = json.loads(receipts_json_str)
    except Exception as exc:
        return ("json_error", block_number, exc)
    return ("ok", block_number, (block_json, receipts_json))


class DerivedEnvelopeProcessor:
    """Parses a raw_envelope payload into all EVM entity rows.

    Three payload forms:
    - parse_and_encode_block_envelope (Rust encode path): dict whose values are
      list[(key_bytes, avro_bytes)] -- pre-encoded Avro, returned as-is.
    - parse_block_envelope (Rust parser path): dict {"block", "transaction",
      "receipt", "log", "token_transfer"} of Row dicts -- returned as-is.
    - Python fallback: tuple (block_json_dict, receipts_list) -- parsed here.
    """

    def process(self, cursor: int, value) -> dict:
        if isinstance(value, dict):
            return value
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

    A background asyncio Task (_prefetch_loop) runs consumer.poll() continuously
    in a thread pool and enqueues valid ("ok") messages into _prefetch_q.
    next_cursor() reads from _prefetch_q without thread overhead, so the engine
    producer coroutine never blocks waiting for a Kafka poll round-trip.

    With raw_envelope partitioned across N partitions, librdkafka's internal
    fetch thread pre-fetches from all N partitions in parallel, so consumer.poll()
    returns with near-zero latency when the topic has a backlog -- _prefetch_loop
    keeps _prefetch_q full and next_cursor() drains it at full engine speed.
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
        prefetch_size: int = 32,
    ):
        consumer_config = _build_consumer_config(kafka_config, group_id)
        self._consumer = Consumer(consumer_config)
        self._source_topic = source_topic
        self._from_block = from_block
        self._to_block = to_block
        self._logger = logger
        self._poll_timeout_sec = poll_timeout_sec
        self._subscribed = False
        # block_number -> (payload, RpcTaskMeta)
        # payload is (block_json_str, receipts_json_str) strings (Rust encode path,
        # encode deferred to fetch()), dict {"block",...} (Rust parse path), or
        # tuple (block_json_dict, receipts_list) (Python fallback).
        # Bounded by engine.sink_inflight_cursors (<= max_inflight entries).
        self._pending: dict[int, tuple] = {}
        # Prefetch queue: _prefetch_loop enqueues ("ok", block_number, payload)
        # tuples so next_cursor() never blocks on a Kafka poll round-trip.
        # maxsize caps memory: each entry is ~0.5-1MB (raw JSON strings).
        self._prefetch_q: asyncio.Queue[tuple] = asyncio.Queue(maxsize=prefetch_size)
        self._prefetch_task: asyncio.Task | None = None
        self._stopped = False
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

    async def _prefetch_loop(self) -> None:
        """Continuously poll Kafka and enqueue valid blocks into _prefetch_q.

        Handles all error/skip cases inline so next_cursor() only ever sees
        ("ok", block_number, payload) tuples. Blocks on _prefetch_q.put() when
        the queue is full, providing natural backpressure against the engine.
        """
        while not self._stopped:
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

            # kind == "ok" -- enqueue; blocks here if engine is processing slowly.
            await self._prefetch_q.put(result)

    async def next_cursor(self) -> int | None:
        """Return the next in-range block_number without blocking on Kafka I/O.

        Starts _prefetch_loop on first call. Reads pre-fetched ("ok", ...) tuples
        from _prefetch_q; skips blocks outside [from_block, to_block].
        Returns None when to_block is reached (bounded/backfill mode ends).
        """
        if not self._subscribed:
            self._subscribe()
        if self._prefetch_task is None:
            self._prefetch_task = asyncio.create_task(self._prefetch_loop())

        while True:
            result = await self._prefetch_q.get()
            _, block_number, payload = result

            if self._from_block is not None and block_number < self._from_block:
                continue
            if self._to_block is not None and block_number > self._to_block:
                self._stopped = True
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
            self._pending[block_number] = (payload, meta)
            return block_number

    async def fetch(self, cursor: int) -> dict:
        """Return parsed/encoded data for the engine's processor loop.

        For the Rust encode path, payload is (block_json_str, receipts_json_str)
        raw strings. Encoding runs here via asyncio.to_thread so that the 8
        engine workers each encode their own block concurrently -- GIL is released
        inside Rust's allow_threads(), giving true parallelism on all CPU cores.

        next_cursor() always populates _pending before the engine worker calls
        fetch(), so the spin-wait exits immediately in the common case.
        """
        cursor = int(cursor)
        while cursor not in self._pending:
            await asyncio.sleep(0)
        payload, meta = self._pending.pop(cursor)

        # Rust encode path: payload is (block_json_str: str, receipts_json_str: str).
        # Distinguished from Python fallback (block_json_dict, receipts_list) by
        # the element type -- raw strings vs parsed Python objects.
        if (
            _RUST_ENCODE
            and _RUST_ENCODE_CONFIG is not None
            and isinstance(payload, tuple)
            and len(payload) == 2
            and isinstance(payload[0], str)
        ):
            config = _RUST_ENCODE_CONFIG  # snapshot before releasing event loop
            try:
                encoded = await asyncio.to_thread(
                    _encode_block_envelope_sync, payload[0], payload[1], config
                )
            except Exception as exc:
                raise RuntimeError(
                    f"parse_and_encode_block_envelope failed for cursor {cursor}: {exc}"
                ) from exc
            return {"block_envelope": (encoded, meta)}

        return {"block_envelope": (payload, meta)}

    def close(self) -> None:
        self._stopped = True
        if self._prefetch_task is not None:
            self._prefetch_task.cancel()
        try:
            self._consumer.commit(asynchronous=False)
        except Exception:
            pass
        try:
            self._consumer.close()
        except Exception:
            pass
