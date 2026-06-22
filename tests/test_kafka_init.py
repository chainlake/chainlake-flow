from types import SimpleNamespace

from rpcstream import kafka_init


def test_kafka_init_logs_schema_registry_mode(monkeypatch):
    captured = {"info": []}

    class DummyLogger:
        def info(self, *args, **kwargs):
            captured["info"].append((args, kwargs))

    fake_runtime = SimpleNamespace(
        pipeline=SimpleNamespace(name="pipe"),
        kafka=SimpleNamespace(
            schema_registry_url="http://registry:8081",
            schema_registry_enabled=True,
            schema_registry_type="avro",
            protobuf_enabled=False,
        ),
        checkpoint=SimpleNamespace(
            topic="checkpoint-topic",
            watermark_state_topic="watermark-topic",
        ),
    )

    monkeypatch.setattr(kafka_init, "load_pipeline_config", lambda _path: SimpleNamespace(
        chain=SimpleNamespace(type="evm"),
        logLevel="info",
    ))
    monkeypatch.setattr(kafka_init, "build_chain_adapter", lambda _chain_type: object())
    monkeypatch.setattr(kafka_init, "resolve", lambda _config, adapter=None: fake_runtime)
    monkeypatch.setattr(kafka_init, "JsonLogger", lambda **_kwargs: DummyLogger())
    monkeypatch.setattr(kafka_init, "bootstrap_kafka_resources", lambda *args, **kwargs: None)
    monkeypatch.setenv("PIPELINE_CONFIG", "pipeline.yaml")

    kafka_init.main()

    first_call_kwargs = captured["info"][0][1]
    second_call_kwargs = captured["info"][1][1]

    assert first_call_kwargs["schema_registry_enabled"] is True
    assert first_call_kwargs["schema_registry_mode"] == "avro"
    assert second_call_kwargs["schema_registry_enabled"] is True
    assert second_call_kwargs["schema_registry_mode"] == "avro"
