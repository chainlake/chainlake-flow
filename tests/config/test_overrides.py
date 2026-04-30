from rpcstream.config.overrides import apply_runtime_overrides
from rpcstream.config.schema import PipelineConfig


def make_config() -> PipelineConfig:
    return PipelineConfig(
        logLevel="info",
        pipeline={
            "name": "base_pipe",
            "from": "checkpoint",
        },
        chain={
            "uid": "evm:56",
            "type": "evm",
            "name": "bsc",
            "network": "mainnet",
        },
        entities=["block", "transaction"],
        erpc={
            "project_id": "main",
            "base_url": "http://localhost:30040",
            "timeout_sec": 10,
            "max_retries": 1,
            "inflight": {
                "min_inflight": 1,
                "max_inflight": 10,
                "initial_inflight": 5,
                "latency_target_ms": 100,
            },
        },
        engine={"concurrency": 1},
        kafka={
            "connection": {"bootstrap_servers": "localhost:9092"},
            "common": {"topic_template": "{type}.{chain}.{network}.{kind}_{entity}"},
            "producer": {
                "linger_ms": 50,
                "batch_size": 65536,
                "compression_type": "lz4",
            },
            "streaming": {
                "batch_size": 100,
                "flush_interval_ms": 50,
                "queue_maxsize": 100,
            },
        },
    )


def test_apply_runtime_overrides_updates_backfill_fields_and_pipeline_name():
    config = make_config()

    effective = apply_runtime_overrides(
        config,
        mode="backfill",
        from_value=10,
        to_value=20,
        entities=["trace"],
    )

    assert effective.pipeline.mode == "backfill"
    assert effective.pipeline.start_block == 10
    assert effective.pipeline.end_block == 20
    assert effective.pipeline.name == "bsc_mainnet_backfill_10_20"
    assert effective.entities == ["trace"]


def test_apply_runtime_overrides_clears_end_block_for_realtime():
    config = make_config()
    backfill = apply_runtime_overrides(config, mode="backfill", from_value=10, to_value=20)

    effective = apply_runtime_overrides(backfill, mode="realtime", from_value="chainhead")

    assert effective.pipeline.mode == "realtime"
    assert effective.pipeline.start_block == "chainhead"
    assert effective.pipeline.end_block is None
    assert effective.pipeline.name == "bsc_mainnet_realtime_chainhead"


def test_apply_runtime_overrides_can_disable_eos():
    config = make_config()

    effective = apply_runtime_overrides(
        config,
        eos_enabled=False,
    )

    assert effective.kafka.eos.enabled is False
