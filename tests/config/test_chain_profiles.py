from types import SimpleNamespace

import pytest

from rpcstream.config.builder import build_erpc_endpoint
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.naming import build_pipeline_name
from rpcstream.config.resolver import resolve


def test_load_pipeline_config_resolves_chain_profile_fields(tmp_path):
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(
        "\n".join(
            [
                "logLevel: info",
                "pipeline:",
                "  from: checkpoint",
                "tracker:",
                "  poll_interval: 0.5",
                "chain:",
                "  name: bsc",
                "  network: mainnet",
                "entities:",
                "  - block",
                "erpc:",
                "  project_id: main",
                "  base_url: http://localhost:30040",
                "  timeout_sec: 10",
                "  max_retries: 1",
                "  inflight:",
                "    min_inflight: 1",
                "    max_inflight: 5",
                "    initial_inflight: 1",
                "    latency_target_ms: 1000",
                "engine:",
                "  concurrency: 1",
                "kafka:",
                "  connection:",
                "    bootstrap_servers: localhost:9092",
                "  common: {}",
                "  producer:",
                "    linger_ms: 50",
                "    batch_size: 1024",
                "  streaming: {}",
            ]
        )
    )

    config = load_pipeline_config(str(config_file))

    assert config.chain.uid == "evm:56"
    assert config.chain.type == "evm"
    assert config.chain.name == "bsc"
    assert config.chain.network == "mainnet"
    assert config.pipeline.name == "bsc_mainnet_realtime_checkpointed_latest"

    runtime = resolve(config)
    assert runtime.tracker.poll_interval == pytest.approx(0.225)


def test_build_pipeline_name_covers_realtime_and_backfill_variants():
    assert build_pipeline_name(
        chain_name="bsc",
        network="mainnet",
        mode="realtime",
        from_value="latest",
    ) == "bsc_mainnet_realtime_latest"

    assert build_pipeline_name(
        chain_name="bsc",
        network="mainnet",
        mode="realtime",
        from_value="checkpoint",
    ) == "bsc_mainnet_realtime_checkpointed_latest"

    assert build_pipeline_name(
        chain_name="bsc",
        network="mainnet",
        mode="backfill",
        from_value=100,
        to_value=200,
    ) == "bsc_mainnet_backfill_100_200"


def test_tracker_poll_interval_scales_with_chain_block_time(tmp_path):
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(
        "\n".join(
            [
                "logLevel: info",
                "pipeline:",
                "  from: checkpoint",
                "chain:",
                "  name: ethereum",
                "  network: mainnet",
                "entities:",
                "  - block",
                "erpc:",
                "  project_id: main",
                "  base_url: http://localhost:30040",
                "  timeout_sec: 10",
                "  max_retries: 1",
                "  inflight:",
                "    min_inflight: 1",
                "    max_inflight: 5",
                "    initial_inflight: 1",
                "    latency_target_ms: 1000",
                "tracker:",
                "  poll_interval: 0.5",
                "engine:",
                "  concurrency: 1",
                "kafka:",
                "  connection:",
                "    bootstrap_servers: localhost:9092",
                "  common: {}",
                "  producer:",
                "    linger_ms: 50",
                "    batch_size: 1024",
                "  streaming: {}",
            ]
        )
    )

    config = load_pipeline_config(str(config_file))
    runtime = resolve(config)

    assert runtime.tracker.poll_interval == pytest.approx(6.0)


def test_tracker_poll_interval_defaults_when_section_is_omitted(tmp_path):
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(
        "\n".join(
            [
                "logLevel: info",
                "pipeline:",
                "  from: checkpoint",
                "chain:",
                "  name: bsc",
                "  network: mainnet",
                "entities:",
                "  - block",
                "erpc:",
                "  project_id: main",
                "  base_url: http://localhost:30040",
                "  timeout_sec: 10",
                "  max_retries: 1",
                "  inflight:",
                "    min_inflight: 1",
                "    max_inflight: 5",
                "    initial_inflight: 1",
                "    latency_target_ms: 1000",
                "engine:",
                "  concurrency: 1",
                "kafka:",
                "  connection:",
                "    bootstrap_servers: localhost:9092",
                "  common: {}",
                "  producer:",
                "    linger_ms: 50",
                "    batch_size: 1024",
                "  streaming: {}",
            ]
        )
    )

    config = load_pipeline_config(str(config_file))
    runtime = resolve(config)

    assert config.pipeline.name == "bsc_mainnet_realtime_checkpointed_latest"
    assert runtime.tracker.poll_interval == pytest.approx(0.225)


def test_build_erpc_endpoint_uses_chain_profile_lookup():
    cfg = SimpleNamespace(
        chain=SimpleNamespace(name="bsc", network="mainnet"),
        erpc=SimpleNamespace(
            base_url="http://localhost:30040",
            project_id="main",
        ),
    )

    assert build_erpc_endpoint(cfg) == "http://localhost:30040/main/evm/56"
