import os

from rpcstream.sinks.kafka.protobuf import _ensure_schema_registry_host_bypasses_proxy


def test_schema_registry_host_is_added_to_no_proxy(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "localhost,127.0.0.1,192.168.122.0/24")
    monkeypatch.setenv("no_proxy", "localhost,127.0.0.1,192.168.122.0/24")

    _ensure_schema_registry_host_bypasses_proxy("http://192.168.122.50:30081")

    assert "192.168.122.50" in os.environ["NO_PROXY"].split(",")
    assert "192.168.122.50" in os.environ["no_proxy"].split(",")


def test_schema_registry_no_proxy_update_is_idempotent(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "localhost,192.168.122.50")
    monkeypatch.delenv("no_proxy", raising=False)

    _ensure_schema_registry_host_bypasses_proxy("http://192.168.122.50:30081")
    _ensure_schema_registry_host_bypasses_proxy("http://192.168.122.50:30081")

    assert os.environ["NO_PROXY"].split(",").count("192.168.122.50") == 1
    assert os.environ["no_proxy"] == "192.168.122.50"
