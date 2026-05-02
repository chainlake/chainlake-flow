from types import SimpleNamespace

from rpcstream.config.builder import build_kafka_config, build_schema_registry_url, build_topic_maps


def test_build_kafka_config_enables_compression(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
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
            eos=SimpleNamespace(enabled=False, transactional_id_template="unused"),
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

    cfg = SimpleNamespace(
        kafka=SimpleNamespace(
            protobuf=SimpleNamespace(schema_registry_url="http://localhost:30081")
        )
    )

    assert build_schema_registry_url(cfg) == "https://registry.example.com:8081"


def test_build_schema_registry_url_falls_back_to_pipeline_config():
    cfg = SimpleNamespace(
        kafka=SimpleNamespace(
            protobuf=SimpleNamespace(schema_registry_url="http://localhost:30081")
        )
    )

    assert build_schema_registry_url(cfg) == "http://localhost:30081"


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
    assert topic_maps.main["trace"] == "evm.bsc.mainnet.raw_trace"
    assert topic_maps.dlq == "dlq.ingestion"
    assert topic_maps.checkpoint == "evm.bsc.mainnet.commit_watermark"
    assert topic_maps.watermark_state == "evm.bsc.mainnet.cursor_state"


def test_build_topic_maps_uses_enriched_topic_for_transactions():
    cfg = SimpleNamespace(
        kafka=SimpleNamespace(
            common=SimpleNamespace(
                topic_template="{type}.{chain}.{network}.{kind}_{entity}"
            )
        ),
        chain=SimpleNamespace(type="evm", name="bsc", network="mainnet"),
        entities=["transaction"],
    )

    topic_maps = build_topic_maps(cfg)

    assert topic_maps.main["transaction"] == "evm.bsc.mainnet.enriched_transaction"


def test_build_topic_maps_supports_token_transfer_topic():
    cfg = SimpleNamespace(
        kafka=SimpleNamespace(
            common=SimpleNamespace(
                topic_template="{type}.{chain}.{network}.{kind}_{entity}"
            )
        ),
        chain=SimpleNamespace(type="evm", name="bsc", network="mainnet"),
        entities=["token_transfer"],
    )

    topic_maps = build_topic_maps(cfg)

    assert topic_maps.main["token_transfer"] == "evm.bsc.mainnet.token_transfer"


def test_build_topic_maps_supports_custom_checkpoint_topic():
    cfg = SimpleNamespace(
        pipeline=SimpleNamespace(checkpoint=SimpleNamespace(topic="custom.checkpoints")),
        kafka=SimpleNamespace(
            common=SimpleNamespace(
                topic_template="{type}.{chain}.{network}.{kind}_{entity}"
            )
        ),
        chain=SimpleNamespace(type="sui", name="sui", network="mainnet"),
        entities=["checkpoint"],
    )

    topic_maps = build_topic_maps(cfg)

    assert topic_maps.checkpoint == "custom.checkpoints"
    assert topic_maps.watermark_state == "custom.checkpoints.cursor_state"


def test_build_kafka_config_enables_eos_transactional_settings(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "host-a")
    monkeypatch.setattr("rpcstream.config.builder.os.getpid", lambda: 12345)

    cfg = SimpleNamespace(
        kafka=SimpleNamespace(
            connection=SimpleNamespace(
                bootstrap_servers="localhost:9092",
                security_protocol=None,
                sasl_mechanism=None,
                auth=SimpleNamespace(username_env=None, password_env=None),
                ssl=SimpleNamespace(ca_path_env=None),
            ),
            common=SimpleNamespace(),
            producer=SimpleNamespace(linger_ms=50, batch_size=1024, compression_type="zstd"),
            eos=SimpleNamespace(
                enabled=True,
                transactional_id_template="{pipeline}.{chain_uid}.{entities}.{hostname}.{pid}",
                transaction_timeout_ms=60000,
            ),
        ),
        pipeline=SimpleNamespace(name="pipe", mode="realtime"),
        chain=SimpleNamespace(uid="evm:56", type="evm", name="bsc", network="mainnet"),
        entities=["transaction", "block"],
    )

    result = build_kafka_config(cfg)

    assert result["enable.idempotence"] is True
    assert result["acks"] == "all"
    assert result["transaction.timeout.ms"] == 60000
    assert result["transactional.id"] == "pipe.evm:56.block,transaction.host-a.12345"
