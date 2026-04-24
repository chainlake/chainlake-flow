from types import SimpleNamespace

from rpcstream.config.builder import build_kafka_config, build_schema_registry_url, build_topic_maps


def test_build_kafka_config_enables_compression(monkeypatch):
    monkeypatch.setenv("KAFKA_USERNAME", "user")
    monkeypatch.setenv("KAFKA_PASSWORD", "pass")
    monkeypatch.setenv("KAFKA_CA_PATH", "/tmp/ca.pem")
    monkeypatch.setenv("KAFKA_COMPRESSION_TYPE", "lz4")

    cfg = SimpleNamespace(
        kafka=SimpleNamespace(
            connection=SimpleNamespace(
                bootstrap_servers="localhost:9092",
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                auth=SimpleNamespace(
                    username_env="KAFKA_USERNAME",
                    password_env="KAFKA_PASSWORD",
                ),
                ssl=SimpleNamespace(ca_path_env="KAFKA_CA_PATH"),
            ),
            common=SimpleNamespace(),
            producer=SimpleNamespace(linger_ms=50, batch_size=1024, compression_type="zstd"),
        ),
        chain=SimpleNamespace(type="evm", name="bsc", network="mainnet"),
        entities=["block"],
    )

    result = build_kafka_config(cfg)

    assert result["bootstrap.servers"] == "localhost:9092"
    assert result["compression.type"] == "lz4"
    assert result["security.protocol"] == "SASL_SSL"
    assert result["sasl.mechanism"] == "PLAIN"
    assert result["sasl.username"] == "user"
    assert result["sasl.password"] == "pass"
    assert result["ssl.ca.location"] == "/tmp/ca.pem"


def test_build_schema_registry_url_supports_existing_env_name(monkeypatch):
    monkeypatch.setenv("KAFAK_SCHEMA_REGISTRY", "registry.example.com:8081")

    assert build_schema_registry_url() == "https://registry.example.com:8081"


def test_build_topic_maps_only_includes_main_and_dlq_topics():
    cfg = SimpleNamespace(
        kafka=SimpleNamespace(
            common=SimpleNamespace(
                topic_template="{type}.{chain}.{network}.{kind}_{entity}"
            )
        ),
        chain=SimpleNamespace(type="evm", name="bsc", network="mainnet"),
        entities=["block", "trace"],
    )

    topic_maps = build_topic_maps(cfg)

    assert topic_maps.main["block"] == "evm.bsc.mainnet.raw_block"
    assert topic_maps.dlq == "dlq.ingestion"
