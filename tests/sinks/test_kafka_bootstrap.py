import asyncio
import threading
from types import SimpleNamespace

import pytest

from rpcstream.sinks.kafka.producer import KafkaWriter
from rpcstream.sinks.kafka.bootstrap import bootstrap_kafka_resources
from rpcstream.sinks.kafka.schema import (
    EntitySchema,
    FieldSchema,
    build_topic_schemas,
)


def test_build_protobuf_topic_schemas_includes_main_and_system_topics():
    topic_maps = SimpleNamespace(
        main={
            "block": "bsc.raw_block",
            "trace": "bsc.raw_trace",
        },
        dlq="dlq.ingestion",
        checkpoint="bsc.commit_watermark",
        watermark_state="bsc.cursor_state",
    )
    entity_schemas = {
        "block": EntitySchema(
            entity="block",
            message_name="TestBlock",
            package="rpcstream.test",
            fields=(FieldSchema("id", "string"),),
        ),
        "trace": EntitySchema(
            entity="trace",
            message_name="TestTrace",
            package="rpcstream.test",
            fields=(FieldSchema("id", "string"),),
        ),
    }

    schemas = build_topic_schemas(topic_maps, entity_schemas, ["block", "trace"])

    assert set(schemas) == {
        "bsc.raw_block",
        "bsc.raw_trace",
        "dlq.ingestion",
        "bsc.commit_watermark",
        "bsc.cursor_state",
    }


def test_build_protobuf_topic_schemas_uses_enriched_transaction_topic():
    topic_maps = SimpleNamespace(
        main={
            "transaction": "bsc.enriched_transaction",
        },
        dlq="dlq.ingestion",
        checkpoint="bsc.commit_watermark",
        watermark_state="bsc.cursor_state",
    )
    entity_schemas = {
        "transaction": EntitySchema(
            entity="transaction",
            message_name="TestTx",
            package="rpcstream.test",
            fields=(FieldSchema("id", "string"),),
        )
    }

    schemas = build_topic_schemas(topic_maps, entity_schemas, ["transaction"])

    assert set(schemas) == {
        "bsc.enriched_transaction",
        "dlq.ingestion",
        "bsc.commit_watermark",
        "bsc.cursor_state",
    }


def test_kafka_writer_start_runs_protobuf_warmup():
    class DummyProducer:
        def poll(self, _timeout):
            return None

        def flush(self):
            return None

    class WarmupRegistry:
        def __init__(self):
            self.started = False
            self.schema_registry_url = "https://registry.example.com"
            self.topic_schemas = {"topic-a": object(), "dlq.ingestion": object()}

        def start(self):
            self.started = True

    writer = KafkaWriter(
        producer=DummyProducer(),
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: "evt-1"),
        time_calculator=SimpleNamespace(calculate_ingest_timestamp=lambda: 1),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=10, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )
    writer.protobuf_registry = WarmupRegistry()

    async def run():
        await writer.start()
        await writer.close()

    asyncio.run(run())
    assert writer.protobuf_registry.started is True


def test_kafka_writer_serializes_protobuf_lazily():
    class DummyProducer:
        pass

    class LazyRegistry:
        def __init__(self):
            self.serialized = []

        def serialize(self, topic, row):
            self.serialized.append((topic, row.copy()))
            return b"protobuf-payload"

    writer = KafkaWriter(
        producer=DummyProducer(),
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: "evt-1"),
        time_calculator=SimpleNamespace(calculate_ingest_timestamp=lambda: 1),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=10, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )
    writer.protobuf_registry = LazyRegistry()

    payload = writer._serialize("topic-a", {"id": "evt-1"})

    assert payload == b"protobuf-payload"
    assert writer.protobuf_registry.serialized == [("topic-a", {"id": "evt-1"})]


def test_kafka_writer_wait_delivery_future_resolves_after_callback():
    class Message:
        def topic(self):
            return "topic-a"

        def partition(self):
            return 0

        def offset(self):
            return 1

    class DummyProducer:
        def __init__(self):
            self.callbacks = []

        def produce(self, **kwargs):
            self.callbacks.append(kwargs["callback"])

        def poll(self, _timeout):
            while self.callbacks:
                self.callbacks.pop(0)(None, Message())

        def flush(self):
            self.poll(0)

    producer = DummyProducer()
    writer = KafkaWriter(
        producer=producer,
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: row["id"]),
        time_calculator=SimpleNamespace(
            calculate_event_timestamp_ms=lambda _row: 1,
            calculate_ingest_timestamp=lambda: 1,
        ),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=1, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )

    async def run():
        await writer.start()
        future = await writer.send("topic-a", [{"id": "evt-1"}], wait_delivery=True)
        await writer.close()
        result = await asyncio.wait_for(future, timeout=1)
        return future.done(), result

    done, result = asyncio.run(run())
    assert done is True
    assert result["message_count"] == 1
    assert result["topic_counts"] == {"topic-a": 1}


def test_kafka_writer_send_transaction_commits_business_and_checkpoint():
    class DummyProducer:
        def __init__(self):
            self.events = []

        def init_transactions(self, timeout=None):
            self.events.append(("init", timeout))

        def begin_transaction(self):
            self.events.append(("begin",))

        def produce(self, topic, key, value, callback=None):
            self.events.append(("produce", topic, key, value))
            if callback:
                callback(None, SimpleNamespace(topic=lambda: topic, partition=lambda: 0, offset=lambda: 1))

        def poll(self, _timeout):
            return None

        def commit_transaction(self):
            self.events.append(("commit",))

        def abort_transaction(self):
            self.events.append(("abort",))

        def flush(self):
            return None

    producer = DummyProducer()
    writer = KafkaWriter(
        producer=producer,
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: row["id"]),
        time_calculator=SimpleNamespace(
            calculate_event_timestamp_ms=lambda _row: 1,
            calculate_ingest_timestamp=lambda: 1,
        ),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=1, queue_maxsize=10),
        producer_config={
            "bootstrap.servers": "localhost:9092",
            "transactional.id": "tx-1",
        },
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
        eos_enabled=True,
        eos_init_timeout_sec=12,
    )

    async def run():
        await writer.start()
        await writer.send_transaction(
            [
                ("topic-a", [{"id": "evt-1", "type": "block"}]),
                ("checkpoint-topic", [{"id": "checkpoint-key", "cursor": 1}]),
            ],
        )
        await writer.close()

    asyncio.run(run())

    assert ("init", 12) in producer.events
    assert producer.events[1] == ("begin",)
    assert ("produce", "topic-a", "evt-1", '{"id":"evt-1","type":"block","ingest_timestamp":1}') in producer.events
    assert ("produce", "checkpoint-topic", "checkpoint-key", '{"id":"checkpoint-key","cursor":1,"ingest_timestamp":1}') in producer.events
    assert ("commit",) in producer.events
    assert ("abort",) not in producer.events


def test_kafka_writer_send_checkpoint_uses_common_message_envelope():
    class Message:
        def topic(self):
            return "checkpoint-topic"

        def partition(self):
            return 0

        def offset(self):
            return 1

    class DummyProducer:
        def __init__(self):
            self.events = []

        def produce(self, topic, key, value, callback=None):
            self.events.append(("produce", topic, key, value))
            if callback:
                callback(None, Message())

        def poll(self, _timeout):
            return None

        def flush(self):
            return None

    producer = DummyProducer()
    writer = KafkaWriter(
        producer=producer,
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: row["id"]),
        time_calculator=SimpleNamespace(
            calculate_event_timestamp_ms=lambda _row: 1,
            calculate_ingest_timestamp=lambda: 1,
        ),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=1, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )

    async def run():
        await writer.start()
        future = await writer.send_checkpoint(
            "checkpoint-topic",
            {"id": "checkpoint-key", "cursor": 1, "kafka_partition_key": "checkpoint-key"},
            wait_delivery=True,
        )
        await writer.close()
        return await asyncio.wait_for(future, timeout=1)

    result = asyncio.run(run())
    assert result["message_count"] == 1
    assert result["topic_counts"] == {"checkpoint-topic": 1}
    assert producer.events == [
        ("produce", "checkpoint-topic", "checkpoint-key", '{"id":"checkpoint-key","cursor":1,"ingest_timestamp":1}')
    ]


def test_bootstrap_kafka_resources_provisions_schema_registry_topic(monkeypatch):
    captured = {}

    class DummyTopicManager:
        def __init__(self, producer_config, logger=None):
            captured["producer_config"] = producer_config
            captured["logger"] = logger

        def ensure_topics(self, topics):
            captured.setdefault("ensure_topics", []).append(list(topics))

        def ensure_compacted_topics(self, topics):
            captured.setdefault("ensure_compacted_topics", []).append(list(topics))

    class DummyRegistry:
        def __init__(self, *, schema_registry_url, producer_config, topic_schemas, logger=None):
            captured["schema_registry_url"] = schema_registry_url
            captured["topic_schemas"] = topic_schemas
            captured["registry_logger"] = logger

        def start(self):
            captured["registry_started"] = True

    class DummyAdapter:
        def build_protobuf_topic_schemas(self, *, topic_maps, entities):
            captured["adapter_topic_maps"] = topic_maps
            captured["adapter_entities"] = list(entities)
            return {"bsc.raw_block": object()}

    runtime = SimpleNamespace(
        kafka=SimpleNamespace(
            config={"bootstrap.servers": "localhost:9092"},
            protobuf_enabled=True,
            schema_registry_url="http://registry:8081",
        ),
        topic_map=SimpleNamespace(
            main={"block": "bsc.raw_block"},
            dlq="dlq.ingestion",
            checkpoint="checkpoint-topic",
            watermark_state="watermark-state",
        ),
        checkpoint=SimpleNamespace(
            topic="checkpoint-topic",
            watermark_state_topic="watermark-state",
        ),
        chain=SimpleNamespace(type="evm", name="bsc", network="mainnet"),
        entities=["block"],
    )

    monkeypatch.setattr("rpcstream.sinks.kafka.bootstrap.KafkaTopicManager", DummyTopicManager)
    monkeypatch.setattr("rpcstream.sinks.kafka.bootstrap.ProtobufSerializerRegistry", DummyRegistry)

    bootstrap_kafka_resources(runtime, adapter=DummyAdapter())

    assert captured["ensure_topics"] == [
        ["bsc.raw_block"],
        ["dlq.ingestion", "watermark-state"],
    ]
    assert captured["ensure_compacted_topics"] == [["checkpoint-topic", "watermark-state"], ["_schemas"]]
    assert captured["schema_registry_url"] == "http://registry:8081"
    assert captured["registry_started"] is True


def test_bootstrap_kafka_resources_skips_protected_schema_topic(monkeypatch):
    captured = {}

    class DummyTopicManager:
        def __init__(self, producer_config, logger=None):
            captured["producer_config"] = producer_config
            captured["logger"] = logger

        def ensure_topics(self, topics):
            captured.setdefault("ensure_topics", []).append(list(topics))

        def ensure_compacted_topics(self, topics):
            topic_list = list(topics)
            captured.setdefault("ensure_compacted_topics", []).append(topic_list)
            if topic_list == ["_schemas"]:
                raise RuntimeError(
                    "KafkaError{code=TOPIC_AUTHORIZATION_FAILED,val=29,str=\"Not authorized to alter_configs or topic is protected by 'kafka_nodelete_topics' or 'kafka_noproduce_topics'\"}"
                )

    class DummyRegistry:
        def __init__(self, *, schema_registry_url, producer_config, topic_schemas, logger=None):
            captured["schema_registry_url"] = schema_registry_url
            captured["topic_schemas"] = topic_schemas
            captured["registry_logger"] = logger

        def start(self):
            captured["registry_started"] = True

    class DummyAdapter:
        def build_protobuf_topic_schemas(self, *, topic_maps, entities):
            captured["adapter_entities"] = list(entities)
            return {"bsc.raw_block": object()}

    runtime = SimpleNamespace(
        kafka=SimpleNamespace(
            config={"bootstrap.servers": "localhost:9092"},
            protobuf_enabled=True,
            schema_registry_url="http://registry:8081",
        ),
        topic_map=SimpleNamespace(
            main={"block": "bsc.raw_block"},
            dlq="dlq.ingestion",
            checkpoint="checkpoint-topic",
            watermark_state="watermark-state",
        ),
        checkpoint=SimpleNamespace(
            topic="checkpoint-topic",
            watermark_state_topic="watermark-state",
        ),
        chain=SimpleNamespace(type="evm", name="bsc", network="mainnet"),
        entities=["block"],
    )

    monkeypatch.setattr("rpcstream.sinks.kafka.bootstrap.KafkaTopicManager", DummyTopicManager)
    monkeypatch.setattr("rpcstream.sinks.kafka.bootstrap.ProtobufSerializerRegistry", DummyRegistry)

    bootstrap_kafka_resources(runtime, adapter=DummyAdapter())

    assert captured["ensure_topics"] == [["bsc.raw_block"], ["dlq.ingestion", "watermark-state"]]
    assert captured["ensure_compacted_topics"] == [["checkpoint-topic", "watermark-state"], ["_schemas"]]
    assert captured["registry_started"] is True


class _RecordingLogger:
    def __init__(self):
        self.records = []

    def _record(self, level, message, **kwargs):
        self.records.append((level, message, kwargs))

    def debug(self, message, **kwargs):
        self._record("debug", message, **kwargs)

    def info(self, message, **kwargs):
        self._record("info", message, **kwargs)

    def warn(self, message, **kwargs):
        self._record("warn", message, **kwargs)

    def error(self, message, **kwargs):
        self._record("error", message, **kwargs)


def _make_writer(producer, logger=None, **overrides):
    kwargs = dict(
        producer=producer,
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: row["id"]),
        time_calculator=SimpleNamespace(
            calculate_event_timestamp_ms=lambda _row: 1,
            calculate_ingest_timestamp=lambda: 1,
        ),
        logger=logger,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=1, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )
    kwargs.update(overrides)
    return KafkaWriter(**kwargs)


def test_kafka_writer_waits_out_producer_backpressure():
    """BufferError is backpressure, not failure: more retries than the old
    10-attempt cap must still succeed instead of killing the sink worker."""

    class BackpressuredProducer:
        def __init__(self, failures):
            self.failures = failures
            self.produced = []
            self.polls = 0

        def produce(self, **kwargs):
            if self.failures > 0:
                self.failures -= 1
                raise BufferError("Local: Queue full")
            self.produced.append(kwargs)

        def poll(self, _timeout):
            self.polls += 1
            return 0

    producer = BackpressuredProducer(failures=25)
    writer = _make_writer(producer)

    from opentelemetry import trace

    async def run():
        await writer._flush_batch(
            [
                (
                    "topic-a",
                    {"id": "evt-1"},
                    trace.get_current_span().get_span_context(),
                    None,
                )
            ]
        )

    asyncio.run(run())

    assert len(producer.produced) == 1
    assert producer.polls >= 25


def test_kafka_writer_logs_sustained_backpressure():
    class BackpressuredProducer:
        def __init__(self):
            self.polls = 0

        def produce(self, **kwargs):
            if self.polls < 3:
                raise BufferError("Local: Queue full")

        def poll(self, _timeout):
            self.polls += 1
            return 0

    producer = BackpressuredProducer()
    logger = _RecordingLogger()
    writer = _make_writer(producer, logger=logger)
    writer.buffer_full_log_interval_sec = 0.0

    from opentelemetry import trace

    async def run():
        await writer._flush_batch(
            [
                (
                    "topic-a",
                    {"id": "evt-1"},
                    trace.get_current_span().get_span_context(),
                    None,
                )
            ]
        )

    asyncio.run(run())

    warnings = [r for r in logger.records if r[1] == "kafka.producer_backpressure"]
    assert warnings


def test_kafka_writer_flush_batch_does_not_block_event_loop():
    """_prepare_message does synchronous Avro/schema-registry serialization.
    Calling it inline on the event loop stalled every other coroutine --
    including all 20 "concurrent" RPC-fetch workers -- for the duration of
    the whole batch. Confirmed live via py-spy: after enabling the "log"
    entity pushed per-block message counts into the hundreds, ingestion
    throughput collapsed to near-serial despite an open scheduler window.
    A slow _prepare_message must not stop another coroutine from making
    progress concurrently."""
    import time

    class DummyProducer:
        def __init__(self):
            self.produced = []

        def produce(self, **kwargs):
            self.produced.append(kwargs)

        def poll(self, _timeout):
            return 0

    writer = _make_writer(DummyProducer())

    def slow_prepare_message(topic, row):
        time.sleep(0.2)
        return row["id"], b"{}", 1, 1

    writer._prepare_message = slow_prepare_message

    from opentelemetry import trace

    ctx = trace.get_current_span().get_span_context()
    other_task_completed_at = None

    async def other_task(start):
        nonlocal other_task_completed_at
        await asyncio.sleep(0.01)
        other_task_completed_at = asyncio.get_running_loop().time() - start

    async def run():
        start = asyncio.get_running_loop().time()
        task = asyncio.create_task(other_task(start))
        await writer._flush_batch([("topic-a", {"id": "evt-1"}, ctx, None)])
        await task

    asyncio.run(run())

    assert writer.producer.produced
    assert other_task_completed_at is not None
    # The blocking sleep is 0.2s; if the event loop were stalled on it, the
    # other coroutine's 0.01s sleep couldn't finish until after it. Give a
    # generous margin (0.1s) so this isn't flaky under CI scheduling jitter.
    assert other_task_completed_at < 0.1


def test_kafka_writer_serialize_executor_is_dedicated_not_shared():
    """The first attempt at this fix used run_in_executor(None, ...) --
    asyncio's shared default executor -- which put message serialization on
    the same thread pool as WatermarkManager's periodic
    asyncio.to_thread(state_reader.load()) call. Under this pod's 2-core CPU
    limit the combined thread count exceeded the CFS quota and caused a full
    production livelock (high CPU, zero forward progress), confirmed live
    via py-spy. Serialization must use its own small dedicated pool that
    asyncio.to_thread can never touch."""
    writer = _make_writer(_DummyProducerNoop())

    assert writer._serialize_executor is not None
    assert writer._serialize_executor._max_workers <= 4

    default_executor_threads = set()

    async def capture_default_executor_thread():
        default_executor_threads.add(
            (await asyncio.get_running_loop().run_in_executor(None, threading.get_ident))
        )

    serialize_executor_threads = set()

    def capture_serialize_thread(topic, row):
        serialize_executor_threads.add(threading.get_ident())
        return row["id"], b"{}", 1, 1

    writer._prepare_message = capture_serialize_thread

    from opentelemetry import trace

    ctx = trace.get_current_span().get_span_context()

    async def run():
        await capture_default_executor_thread()
        await writer._flush_batch([("topic-a", {"id": "evt-1"}, ctx, None)])

    asyncio.run(run())

    assert serialize_executor_threads
    assert serialize_executor_threads.isdisjoint(default_executor_threads)


def test_kafka_writer_serialization_unaffected_by_busy_default_executor():
    """Reproduces the actual production incident: a slow asyncio.to_thread()
    call (WatermarkManager's periodic state_reader.load(), reloading a
    13,000+ record cursor_state topic) occupying the shared default executor
    must not delay Kafka message serialization. That's exactly what happened
    when the first fix attempt used run_in_executor(None, ...) and shared
    the pool with that call: under the pod's 2-core CPU limit, both users'
    combined thread count exceeded the CFS quota and the whole pipeline
    livelocked."""
    import time

    writer = _make_writer(_DummyProducerNoop())

    from opentelemetry import trace

    ctx = trace.get_current_span().get_span_context()

    def slow_default_executor_job():
        time.sleep(0.3)

    async def run():
        loop = asyncio.get_running_loop()
        # Saturate the *default* executor the same way asyncio.to_thread does.
        # run_in_executor submits to the pool immediately (it returns a
        # Future, already running), so no create_task is needed here.
        busy_future = loop.run_in_executor(None, slow_default_executor_job)
        await asyncio.sleep(0.05)  # let the busy job actually start

        start = loop.time()
        await writer._flush_batch([("topic-a", {"id": "evt-1"}, ctx, None)])
        elapsed = loop.time() - start

        await busy_future
        return elapsed

    elapsed = asyncio.run(run())
    assert elapsed < 0.2


class _DummyProducerNoop:
    def produce(self, **kwargs):
        pass

    def poll(self, _timeout):
        return 0


def test_kafka_writer_isolates_oversized_message_from_sink_worker():
    """A KafkaException like MSG_SIZE_TOO_LARGE is specific to one message,
    not the worker or the broker connection: it must fail just that row's
    delivery and let the rest of the batch through, not permanently kill
    the sink the way a bare BufferError used to. This happened live -- a
    single oversized "log" entity row killed the worker and ingestion
    stayed paused until the pod was manually restarted."""
    from confluent_kafka import KafkaError, KafkaException

    class OneOversizedMessageProducer:
        def __init__(self):
            self.produced = []

        def produce(self, **kwargs):
            if kwargs["key"] == "too-big":
                raise KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
            self.produced.append(kwargs)

        def poll(self, _timeout):
            return 0

    logger = _RecordingLogger()
    writer = _make_writer(OneOversizedMessageProducer(), logger=logger)

    from opentelemetry import trace

    ctx = trace.get_current_span().get_span_context()

    async def run():
        await writer._flush_batch(
            [
                ("topic-a", {"id": "too-big"}, ctx, None),
                ("topic-a", {"id": "evt-2"}, ctx, None),
            ]
        )

    asyncio.run(run())

    assert [p["key"] for p in writer.producer.produced] == ["evt-2"]
    failures = [r for r in logger.records if r[1] == "kafka.produce_failed"]
    assert failures
    assert failures[0][2]["error_type"] == "KafkaException"


def test_kafka_sink_worker_crash_is_logged():
    """A crashed sink worker used to die silently: the engine keeps a strong
    reference to the task, so asyncio never prints 'Task exception was never
    retrieved' and the sink stops draining with no trace in the logs."""

    class ExplodingProducer:
        def produce(self, **kwargs):
            raise RuntimeError("boom")

        def poll(self, _timeout):
            return 0

    logger = _RecordingLogger()
    writer = _make_writer(ExplodingProducer(), logger=logger)

    async def run():
        await writer.start()
        await writer.send("topic-a", [{"id": "evt-1"}])
        for _ in range(100):
            if writer._worker_task.done():
                break
            await asyncio.sleep(0.01)
        # let the done callback run
        await asyncio.sleep(0.05)

    asyncio.run(run())

    assert writer._worker_task.done()
    crashes = [r for r in logger.records if r[1] == "kafka.sink_worker_crashed"]
    assert crashes
    level, _message, fields = crashes[0]
    assert level == "error"
    assert fields["error_type"] == "RuntimeError"
    assert "boom" in fields["error"]


def test_kafka_writer_enqueue_waits_longer_than_a_blip():
    """The enqueue timeout used to be hardcoded at 0.1s, so sub-second sink
    backpressure failed the batch (and the cursor) instead of waiting."""

    class DummyProducer:
        def produce(self, **kwargs):
            return None

        def poll(self, _timeout):
            return 0

    writer = _make_writer(
        DummyProducer(),
        config=SimpleNamespace(
            batch_size=10,
            flush_interval_ms=1,
            queue_maxsize=1,
            enqueue_timeout_ms=2000,
        ),
    )

    from opentelemetry import trace

    ctx = trace.get_current_span().get_span_context()

    async def run():
        # Fill the only slot so send() has to wait for room.
        writer.queue.put_nowait(("topic-a", [{"id": "blocker"}], ctx, None))

        async def free_slot_soon():
            await asyncio.sleep(0.3)  # longer than the old 0.1s timeout
            writer.queue.get_nowait()

        free_task = asyncio.create_task(free_slot_soon())
        await writer.send("topic-a", [{"id": "evt-1"}])
        await free_task

    asyncio.run(run())

    assert writer.queue.qsize() == 1


def test_kafka_writer_send_fails_fast_when_worker_is_dead():
    """A dead worker never drains the queue, so waiting out the timeout would
    just look like slow Kafka instead of reporting the real problem."""

    class ExplodingProducer:
        def produce(self, **kwargs):
            raise RuntimeError("boom")

        def poll(self, _timeout):
            return 0

    writer = _make_writer(ExplodingProducer())

    async def run():
        await writer.start()
        await writer.send("topic-a", [{"id": "evt-1"}])
        for _ in range(100):
            if writer._worker_task.done():
                break
            await asyncio.sleep(0.01)
        with pytest.raises(RuntimeError, match="sink worker is no longer running"):
            await writer.send("topic-a", [{"id": "evt-2"}])

    asyncio.run(run())


def test_kafka_streaming_defaults_widen_the_sink_buffer():
    from rpcstream.config.schema import KafkaStreaming

    streaming = KafkaStreaming()
    assert streaming.queue_maxsize > 100
    assert streaming.enqueue_timeout_ms > 100
