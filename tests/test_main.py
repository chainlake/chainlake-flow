from __future__ import annotations

import asyncio
import os
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


def test_configure_local_proxy_bypass_adds_exact_runtime_hosts(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "localhost,127.0.0.1,192.168.122.0/24")
    monkeypatch.setenv("no_proxy", "localhost,127.0.0.1,192.168.122.0/24")

    runtime = SimpleNamespace(
        client=SimpleNamespace(base_url="http://clh001:30041/main/evm/56"),
        kafka=SimpleNamespace(
            schema_registry_url="http://192.168.122.50:30081",
            config={"bootstrap.servers": "192.168.122.50:30092"},
        ),
    )

    appended = main_mod.configure_local_proxy_bypass(runtime)

    assert appended == ["clh001", "192.168.122.50"]
    no_proxy = os.environ["NO_PROXY"].split(",")
    lower_no_proxy = os.environ["no_proxy"].split(",")
    assert "192.168.122.50" in no_proxy
    assert "clh001" in no_proxy
    assert "192.168.122.50" in lower_no_proxy
    assert "clh001" in lower_no_proxy
