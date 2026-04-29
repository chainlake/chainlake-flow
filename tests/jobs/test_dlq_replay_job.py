from types import SimpleNamespace

from rpcstream.adapters.evm.jobs.dlq_replay_job import (
    LOCAL_DEFAULT_CONFIG_PATH,
    ReplayJobOptions,
    build_parser,
    build_replay_job_manifest,
)


def test_build_replay_job_manifest_uses_runtime_metadata_and_env_filters():
    runtime = SimpleNamespace(
        chain=SimpleNamespace(name="bsc", type="evm"),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpointed_latest"),
    )
    options = ReplayJobOptions(
        entity="trace",
        status="pending",
        stage="processor",
        max_records=1,
    )

    manifest = build_replay_job_manifest(runtime, options)

    assert manifest["metadata"]["name"] == "rpcstream-dlq-replay"
    assert manifest["metadata"]["namespace"] == "ingestion"
    assert manifest["metadata"]["labels"]["chain"] == "bsc"
    assert (
        manifest["metadata"]["labels"]["pipeline"]
        == "bsc_mainnet_realtime_checkpointed_latest"
    )

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert container["command"] == [
        "python",
        "-m",
        "rpcstream.adapters.evm.jobs.dlq_replay_job",
        "run",
    ]
    env_map = {item["name"]: item["value"] for item in container["env"] if "value" in item}
    assert env_map["PIPELINE_CONFIG"] == "/config/pipeline.yaml"
    assert env_map["DLQ_REPLAY_GROUP_ID"] == "rpcstream-dlq-replay-manual"
    assert env_map["DLQ_REPLAY_OFFSET"] == "earliest"
    assert env_map["DLQ_REPLAY_ENTITY"] == "trace"
    assert env_map["DLQ_REPLAY_STATUS"] == "pending"
    assert env_map["DLQ_REPLAY_STAGE"] == "processor"
    assert env_map["DLQ_REPLAY_MAX_RECORDS"] == "1"


def test_build_replay_job_manifest_skips_optional_filters_when_unset():
    runtime = SimpleNamespace(
        chain=SimpleNamespace(name="bsc", type="evm"),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpointed_latest"),
    )
    options = ReplayJobOptions(entity=None, stage=None, max_records=None)

    manifest = build_replay_job_manifest(runtime, options)
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    env_names = {item["name"] for item in container["env"]}

    assert "DLQ_REPLAY_ENTITY" not in env_names
    assert "DLQ_REPLAY_STAGE" not in env_names
    assert "DLQ_REPLAY_MAX_RECORDS" not in env_names


def test_parser_defaults_to_local_replay_mode_without_subcommand():
    args = build_parser().parse_args(["--entity", "trace", "--status", "pending"])

    assert args.command is None
    assert args.config == LOCAL_DEFAULT_CONFIG_PATH
    assert args.group_id.startswith("rpcstream-dlq-replay-local-")
    assert args.offset == "earliest"
    assert args.entity == "trace"
    assert args.status == "pending"
