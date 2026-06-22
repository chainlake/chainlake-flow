from types import SimpleNamespace


from rpcstream.app_runtime import build_runtime_stack


def test_runtime_uses_eos_even_without_checkpoint(monkeypatch):
    fake_runtime = SimpleNamespace(
        observability=SimpleNamespace(config=SimpleNamespace()),
        pipeline=SimpleNamespace(name="pipe", mode="realtime"),
        client=SimpleNamespace(base_url="http://localhost", timeout_sec=10, max_retries=1),
        tracker=SimpleNamespace(poll_interval=1.0),
        scheduler=SimpleNamespace(
            initial_inflight=1,
            max_inflight=1,
            min_inflight=1,
            latency_target_ms=1000,
        ),
        entities=["trace"],
        kafka=SimpleNamespace(
            config={
                "bootstrap.servers": "localhost:9092",
                "transactional.id": "tx-1",
                "transaction.timeout.ms": 60000,
            },
            streaming=SimpleNamespace(batch_size=1, flush_interval_ms=1, queue_maxsize=1),
            protobuf_enabled=False,
            schema_registry_url=None,
            eos_enabled=True,
            eos_init_timeout_sec=12,
        ),
        topic_map=SimpleNamespace(main={"trace": "topic-a"}, dlq="dlq.ingestion"),
        checkpoint=SimpleNamespace(
            topic="checkpoint-topic",
            watermark_state_topic="watermark-state-topic",
            flush_interval_ms=100,
            commit_batch_size=100,
        ),
        chain=SimpleNamespace(uid="evm:56", type="evm", network="mainnet"),
        engine=SimpleNamespace(concurrency=1),
    )
    fake_config = SimpleNamespace(logLevel="info")

    class DummyLogger:
        pass

    class DummyClient:
        async def close(self):
            return None

    class DummyProducer:
        def __init__(self, config):
            self.config = config

    class DummyWriter:
        def __init__(self, **kwargs):
            self.producer_config = kwargs["producer_config"]
            self.eos_enabled = kwargs["eos_enabled"]

    class DummyCheckpointReader:
        def __init__(self, **kwargs):
            self.topic = kwargs["topic"]
            self.identity = kwargs["identity"]

        def load(self):
            return None

    class DummyTracker:
        async def start(self):
            return None

        async def stop(self):
            return None

    class DummyAdapter:
        def build_tracker(self, **_kwargs):
            return DummyTracker()

        def build_fetcher(self, **_kwargs):
            return object()

        def build_processors(self, **_kwargs):
            return {"trace": object()}

        def build_enricher(self):
            return object()

        def build_decoder(self, **_kwargs):
            return object()

        def build_event_id_calculator(self):
            return object()

        def build_event_time_calculator(self):
            return object()

        def build_protobuf_topic_schemas(self, *, topic_maps, entities):
            return {}

    monkeypatch.setattr("rpcstream.app_runtime.load_pipeline_config", lambda _path: fake_config)
    monkeypatch.setattr("rpcstream.app_runtime.resolve", lambda _cfg, adapter=None: fake_runtime)
    monkeypatch.setattr(
        "rpcstream.app_runtime.build_observability",
        lambda *_args, **_kwargs: SimpleNamespace(
            start=lambda: None,
            shutdown=lambda: None,
            get_logger_provider=lambda: None,
            get_meter=lambda _name: None,
            get_tracer=lambda _name: None,
        ),
    )
    monkeypatch.setattr("rpcstream.app_runtime.JsonLogger", lambda **_kwargs: DummyLogger())
    monkeypatch.setattr("rpcstream.app_runtime.JsonRpcClient", lambda **_kwargs: DummyClient())
    monkeypatch.setattr("rpcstream.app_runtime.AdaptiveRpcScheduler", lambda *args, **kwargs: object())
    monkeypatch.setattr("rpcstream.app_runtime.build_chain_adapter", lambda _chain_type: DummyAdapter())
    monkeypatch.setattr("rpcstream.app_runtime.Producer", DummyProducer)
    monkeypatch.setattr("rpcstream.app_runtime.KafkaWriter", lambda **kwargs: DummyWriter(**kwargs))
    monkeypatch.setattr("rpcstream.app_runtime.KafkaCheckpointReader", DummyCheckpointReader)
    monkeypatch.setattr("rpcstream.app_runtime.KafkaWatermarkStateReader", DummyCheckpointReader)
    monkeypatch.setattr("rpcstream.app_runtime.IngestionEngine", lambda **kwargs: kwargs)

    stack = build_runtime_stack(config_path="pipeline.yaml", with_tracker=False)

    assert stack.engine["eos_enabled"] is True
    assert stack.engine["sink"].eos_enabled is True
    assert stack.engine["sink"].producer_config["transactional.id"] == "tx-1"
    assert stack.engine["sink"].producer_config["transaction.timeout.ms"] == 60000
    assert stack.engine["watermark_manager"] is not None
    assert stack.engine["watermark_manager"].flush_on_advance is False
