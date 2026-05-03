import json

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
                "max_inflight": 10,
                "latency_target_ms": 100,
            },
        },
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
    monkeypatch.setattr("rpcstream.cli.common.load_pipeline_config", lambda _path: make_config())

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

    monkeypatch.setattr("rpcstream.cli.common.load_pipeline_config", lambda _path: make_config())

    async def fake_run_pipeline(*, config_path=None, config=None):
        captured["config_path"] = config_path
        captured["config"] = config

    monkeypatch.setattr("rpcstream.cli.ingest.run_pipeline", fake_run_pipeline)

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
    assert captured["config"].pipeline.from_ == 100
    assert captured["config"].pipeline.to == 110
    assert captured["config"].entities == ["transaction", "trace", "block"]


def test_dlq_replay_invokes_existing_runner(monkeypatch):
    captured = {}

    monkeypatch.setattr("rpcstream.cli.common.load_pipeline_config", lambda _path: make_config())

    async def fake_run_dlq_replay(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("rpcstream.cli.dlq.run_dlq_replay", fake_run_dlq_replay)

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

    monkeypatch.setattr("rpcstream.cli.common.load_pipeline_config", lambda _path: make_config())

    async def fake_run_pipeline(*, config_path=None, config=None):
        captured["config_path"] = config_path
        captured["config"] = config

    monkeypatch.setattr("rpcstream.cli.ingest.run_pipeline", fake_run_pipeline)

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
    assert captured["config"].pipeline.from_ == "100"
    assert captured["config"].pipeline.to is None
    assert captured["config"].entities == ["block", "transaction"]


def test_root_command_uses_explicit_chainhead_without_disabling_eos(monkeypatch):
    captured = {}

    monkeypatch.setattr("rpcstream.cli.common.load_pipeline_config", lambda _path: make_config())

    async def fake_run_pipeline(*, config_path=None, config=None):
        captured["config_path"] = config_path
        captured["config"] = config

    monkeypatch.setattr("rpcstream.cli.ingest.run_pipeline", fake_run_pipeline)

    result = runner.invoke(
        app,
        [
            "--config",
            "pipeline.yaml",
            "--from",
            "chainhead",
            "--entity",
            "block",
        ],
    )

    assert result.exit_code == 0
    assert captured["config"].pipeline.mode == "realtime"
    assert captured["config"].pipeline.from_ == "chainhead"
    assert captured["config"].kafka.eos.enabled is False


def test_root_help_lists_only_dlq_and_config_commands():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "benchmark" in result.stdout
    assert "init" in result.stdout
    assert "dlq" in result.stdout
    assert "config" in result.stdout
    assert "│ ingest" not in result.stdout


def test_benchmark_command_invokes_runner(monkeypatch):
    captured = {}

    async def fake_run_benchmark_async(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("rpcstream.cli.benchmark._run_benchmark_async", fake_run_benchmark_async)

    result = runner.invoke(
        app,
        [
            "benchmark",
            "--config",
            "pipeline.yaml",
            "--sink",
            "blackhole",
            "--window",
            "100",
            "--entity",
            "block,transaction",
            "--output-file",
            "benchmark.json",
        ],
    )

    assert result.exit_code == 0
    assert captured["config_path"] == "pipeline.yaml"
    assert captured["sink"] == "blackhole"
    assert captured["mode"] == "backfill"
    assert captured["eos_enabled"] is False
    assert captured["window"] == 100
    assert captured["entity"] == ["block,transaction"]
    assert captured["output_file"] == "benchmark.json"


def test_benchmark_command_accepts_explicit_eos_enabled(monkeypatch):
    captured = {}

    async def fake_run_benchmark_async(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("rpcstream.cli.benchmark._run_benchmark_async", fake_run_benchmark_async)

    result = runner.invoke(
        app,
        [
            "benchmark",
            "--config",
            "pipeline.yaml",
            "--mode",
            "realtime",
            "--sink",
            "kafka",
            "--eos-enabled",
            "true",
        ],
    )

    assert result.exit_code == 0
    assert captured["mode"] == "realtime"
    assert captured["eos_enabled"] is True


def test_benchmark_output_file_helper_writes_json(tmp_path):
    class FakeSummary:
        def to_dict(self):
            return {
                "chain_name": "bsc",
                "network": "mainnet",
                "sink": "blackhole",
                "eos_enabled": False,
                "mode": "concurrent",
                "start_cursor": 1,
                "end_cursor": 2,
                "total_cursors": 2,
                "total_messages": 3,
                "total_elapsed_sec": 1.5,
                "blocks_per_sec": 1.333,
                "messages_per_sec": 2.0,
                "latency_ms": {
                    "avg": 1.0,
                    "min": 0.5,
                    "max": 1.5,
                    "p50": 1.0,
                    "p95": 1.4,
                    "p99": 1.49,
                },
            }

    output_file = tmp_path / "benchmark"
    from rpcstream.cli.benchmark import _write_benchmark_output_file

    written = _write_benchmark_output_file(FakeSummary(), str(output_file))
    assert written == output_file.with_suffix(".json")
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["chain_name"] == "bsc"
    assert payload["mode"] == "concurrent"
    assert "samples" not in payload


def test_init_command_invokes_kafka_init_with_config_path(monkeypatch):
    captured = {}

    def fake_run_kafka_init():
        import os

        captured["config_path"] = os.environ.get("PIPELINE_CONFIG")

    monkeypatch.setattr("rpcstream.cli.init.run_kafka_init", fake_run_kafka_init)

    result = runner.invoke(
        app,
        [
            "init",
            "--config",
            "pipeline.yaml",
        ],
    )

    assert result.exit_code == 0
    assert captured["config_path"] == "pipeline.yaml"
