from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import rpcstream.main as main_mod


class AbortAfterStartupLog(RuntimeError):
    pass


def test_startup_context_no_longer_includes_protobuf_enabled(monkeypatch):
    fake_runtime = SimpleNamespace(
        observability=SimpleNamespace(config=SimpleNamespace()),
        pipeline=SimpleNamespace(name="pipe", mode="realtime", start_cursor="chainhead"),
        client=SimpleNamespace(base_url="http://localhost", timeout_sec=10, max_retries=1),
        tracker=SimpleNamespace(poll_interval=1.0, websocket_url=None),
        scheduler=SimpleNamespace(
            initial_inflight=1,
            max_inflight=1,
            min_inflight=1,
            latency_target_ms=1000,
        ),
        entities=["trace"],
        kafka=SimpleNamespace(
            schema_registry_url="http://registry:8081",
            schema_registry_type="avro",
            schema_registry_enabled=True,
            protobuf_enabled=False,
            eos_enabled=False,
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

    monkeypatch.setattr(main_mod, "load_pipeline_config", lambda _path: SimpleNamespace(logLevel="info"))
    monkeypatch.setattr(main_mod, "resolve", lambda _config: fake_runtime)
    monkeypatch.setattr(
        main_mod,
        "build_observability",
        lambda *_args, **_kwargs: SimpleNamespace(
            start=lambda: asyncio.sleep(0),
            get_logger_provider=lambda: None,
            get_meter=lambda _name: None,
            get_tracer=lambda _name: None,
        ),
    )
    monkeypatch.setattr(main_mod, "build_chain_adapter", lambda _chain_type: object())

    captured = {}

    class DummyLogger:
        def info(self, message, **kwargs):
            captured["message"] = message
            captured["kwargs"] = kwargs
            raise AbortAfterStartupLog

    monkeypatch.setattr(main_mod, "JsonLogger", lambda **_kwargs: DummyLogger())

    with pytest.raises(AbortAfterStartupLog):
        asyncio.run(main_mod.run_pipeline(config_path="pipeline.yaml"))

    assert captured["message"] == "runtime.startup_context"
    assert "protobuf_enabled" not in captured["kwargs"]
    assert captured["kwargs"]["schema_registry_type"] == "avro"
    assert captured["kwargs"]["schema_registry_url"] == "http://registry:8081"
