from typer.testing import CliRunner

from rpcstream.cli import _infer_ingest_mode, app
from rpcstream.config.schema import PipelineConfig

runner = CliRunner()


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


def test_config_print_outputs_effective_overridden_yaml(monkeypatch):
    monkeypatch.setattr("rpcstream.cli.load_pipeline_config", lambda _path: make_config())

    result = runner.invoke(
        app,
        [
            "config",
            "print",
            "--config",
            "pipeline.yaml",
            "--from",
            "10",
            "--to",
            "20",
            "--entity",
            "trace,log",
        ],
    )

    assert result.exit_code == 0
    assert "mode: backfill" in result.stdout
    assert "from: 10" in result.stdout
    assert "to: 20" in result.stdout
    assert "- trace" in result.stdout
    assert "- log" in result.stdout
    assert "name: bsc_mainnet_backfill_10_20" in result.stdout


def test_ingest_backfill_invokes_existing_runner_with_effective_config(monkeypatch):
    captured = {}

    monkeypatch.setattr("rpcstream.cli.load_pipeline_config", lambda _path: make_config())

    async def fake_run_pipeline(*, config_path=None, config=None):
        captured["config_path"] = config_path
        captured["config"] = config

    monkeypatch.setattr("rpcstream.cli.run_pipeline", fake_run_pipeline)

    result = runner.invoke(
        app,
        [
            "--config",
            "pipeline.yaml",
            "--from",
            "100",
            "--to",
            "110",
            "--entity",
            "transaction,trace",
            "--entity",
            "block",
        ],
    )

    assert result.exit_code == 0
    assert captured["config_path"] == "pipeline.yaml"
    assert captured["config"].pipeline.mode == "backfill"
    assert captured["config"].pipeline.start_block == 100
    assert captured["config"].pipeline.end_block == 110
    assert captured["config"].entities == ["transaction", "trace", "block"]


def test_dlq_replay_invokes_existing_runner(monkeypatch):
    captured = {}

    monkeypatch.setattr("rpcstream.cli.load_pipeline_config", lambda _path: make_config())

    async def fake_run_dlq_replay(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("rpcstream.cli.run_dlq_replay", fake_run_dlq_replay)

    result = runner.invoke(
        app,
        [
            "dlq",
            "replay",
            "--config",
            "pipeline.yaml",
            "--group-id",
            "replay-group",
            "--entity",
            "trace",
            "--status",
            "pending",
            "--stage",
            "processor",
            "--max-records",
            "5",
        ],
    )

    assert result.exit_code == 0
    assert captured["config_path"] == "pipeline.yaml"
    assert captured["group_id"] == "replay-group"
    assert captured["entity"] == "trace"
    assert captured["status"] == "pending"
    assert captured["stage"] == "processor"
    assert captured["max_records"] == 5


def test_parse_entities_supports_commas_and_deduplicates():
    from rpcstream.cli import _parse_entities

    assert _parse_entities(["block,transaction", "transaction", " log "]) == [
        "block",
        "transaction",
        "log",
    ]


def test_infer_ingest_mode_matches_from_to_combination():
    assert _infer_ingest_mode(from_value=None, to_value=None) == "realtime"
    assert _infer_ingest_mode(from_value="100", to_value=None) == "realtime"
    assert _infer_ingest_mode(from_value="100", to_value=200) == "backfill"


def test_root_command_invokes_realtime_when_only_from_is_set(monkeypatch):
    captured = {}

    monkeypatch.setattr("rpcstream.cli.load_pipeline_config", lambda _path: make_config())

    async def fake_run_pipeline(*, config_path=None, config=None):
        captured["config_path"] = config_path
        captured["config"] = config

    monkeypatch.setattr("rpcstream.cli.run_pipeline", fake_run_pipeline)

    result = runner.invoke(
        app,
        [
            "--config",
            "pipeline.yaml",
            "--from",
            "100",
            "--entity",
            "block,transaction",
        ],
    )

    assert result.exit_code == 0
    assert captured["config"].pipeline.mode == "realtime"
    assert captured["config"].pipeline.start_block == "100"
    assert captured["config"].pipeline.end_block is None
    assert captured["config"].entities == ["block", "transaction"]


def test_root_command_uses_explicit_latest_without_disabling_eos(monkeypatch):
    captured = {}

    monkeypatch.setattr("rpcstream.cli.load_pipeline_config", lambda _path: make_config())

    async def fake_run_pipeline(*, config_path=None, config=None):
        captured["config_path"] = config_path
        captured["config"] = config

    monkeypatch.setattr("rpcstream.cli.run_pipeline", fake_run_pipeline)

    result = runner.invoke(
        app,
        [
            "--config",
            "pipeline.yaml",
            "--from",
            "latest",
            "--entity",
            "block",
        ],
    )

    assert result.exit_code == 0
    assert captured["config"].pipeline.mode == "realtime"
    assert captured["config"].pipeline.start_block == "latest"
    assert captured["config"].kafka.eos.enabled is False


def test_root_help_lists_only_dlq_and_config_commands():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "dlq" in result.stdout
    assert "config" in result.stdout
    assert "│ ingest" not in result.stdout
