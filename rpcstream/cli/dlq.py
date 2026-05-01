from __future__ import annotations

import typer

from rpcstream.cli.common import default_config_path, fail, load_effective_config, run_async
from rpcstream.dlq_replay import DEFAULT_REPLAY_GROUP
from rpcstream.dlq_retry import DEFAULT_RETRY_GROUP
from rpcstream.dlq_replay import run_dlq_replay
from rpcstream.dlq_retry import run_dlq_retry

dlq_app = typer.Typer(help="Run DLQ workflows.", no_args_is_help=True)


@dlq_app.command("retry")
def dlq_retry(
    config_path: str = typer.Option(
        None,
        "--config",
        help="Path to pipeline.yaml.",
    ),
    group_id: str = typer.Option(
        DEFAULT_RETRY_GROUP,
        "--group-id",
        help="Consumer group id used by the retry worker.",
    ),
) -> None:
    config_path = config_path or default_config_path()
    try:
        config = load_effective_config(config_path=config_path)
    except Exception as exc:
        fail(exc)
    run_async(run_dlq_retry(config_path=config_path, config=config, group_id=group_id))


@dlq_app.command("replay")
def dlq_replay(
    config_path: str = typer.Option(
        None,
        "--config",
        help="Path to pipeline.yaml.",
    ),
    group_id: str = typer.Option(
        DEFAULT_REPLAY_GROUP,
        "--group-id",
        help="Consumer group id used by the replay worker.",
    ),
    entity: str | None = typer.Option(
        None,
        "--entity",
        help="Optional entity filter.",
    ),
    status: str = typer.Option(
        "failed",
        "--status",
        help="DLQ status filter.",
    ),
    stage: str | None = typer.Option(
        None,
        "--stage",
        help="Optional DLQ stage filter.",
    ),
    max_records: int | None = typer.Option(
        None,
        "--max-records",
        help="Maximum number of DLQ records to replay.",
    ),
) -> None:
    config_path = config_path or default_config_path()
    try:
        config = load_effective_config(config_path=config_path)
    except Exception as exc:
        fail(exc)
    run_async(
        run_dlq_replay(
            config_path=config_path,
            config=config,
            group_id=group_id,
            entity=entity,
            status=status,
            stage=stage,
            max_records=max_records,
        )
    )
