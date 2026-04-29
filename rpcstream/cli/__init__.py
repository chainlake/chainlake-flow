from __future__ import annotations

import asyncio
import os

import typer
import yaml

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.overrides import apply_runtime_overrides
from rpcstream.dlq_replay import DEFAULT_REPLAY_GROUP, run_dlq_replay
from rpcstream.dlq_retry import DEFAULT_RETRY_GROUP, run_dlq_retry
from rpcstream.main import run_pipeline

app = typer.Typer(
    help=(
        "Run realtime or backfill ingestion from the root command. "
        "Use explicit subcommands only for DLQ and config workflows."
    ),
    epilog=(
        "Ingestion rules:\n\n"
        "1. No --from and no --to:\n"
        "   realtime mode; resume from checkpoint when enabled, otherwise latest.\n\n"
        "2. --from latest:\n"
        "   realtime mode; ignore any saved checkpoint and start from latest.\n\n"
        "3. --from <cursor> only:\n"
        "   realtime mode; start from that cursor and continue streaming.\n\n"
        "4. --from <cursor> and --to <cursor>:\n"
        "   backfill mode; run a bounded cursor range.\n\n"
        "Examples:\n\n"
        "rpcstream --entity block,transaction\n\n"
        "rpcstream --from latest --entity block\n\n"
        "rpcstream --from 95000000 --entity block,transaction\n\n"
        "rpcstream --from 95000000 --to 95000100 --entity block,transaction\n\n"
        "rpcstream dlq retry\n\n"
        "rpcstream config print"
    ),
    invoke_without_command=True,
)
dlq_app = typer.Typer(help="Run DLQ workflows.", no_args_is_help=True)
config_app = typer.Typer(help="Validate and inspect effective config.", no_args_is_help=True)

app.add_typer(dlq_app, name="dlq")
app.add_typer(config_app, name="config")


def _default_config_path() -> str:
    return os.getenv("PIPELINE_CONFIG", "pipeline.yaml")


def _load_effective_config(
    *,
    config_path: str,
    mode: str | None = None,
    from_value: str | int | None = None,
    to_value: int | None = None,
    entities: list[str] | None = None,
    eos_enabled: bool | None = None,
):
    config = load_pipeline_config(config_path)
    if (
        mode is None
        and from_value is None
        and to_value is None
        and entities is None
        and eos_enabled is None
    ):
        return config
    return apply_runtime_overrides(
        config,
        mode=mode,
        from_value=from_value,
        to_value=to_value,
        entities=entities,
        eos_enabled=eos_enabled,
    )


def _dump_config(config) -> str:
    return yaml.safe_dump(
        config.model_dump(by_alias=True),
        sort_keys=False,
        allow_unicode=False,
    )


def _parse_entities(values: list[str] | None) -> list[str] | None:
    if not values:
        return None

    entities: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in str(value).split(","):
            entity = part.strip()
            if not entity or entity in seen:
                continue
            seen.add(entity)
            entities.append(entity)
    return entities or None

def _infer_ingest_mode(*, from_value: str | int | None, to_value: int | None) -> str:
    if from_value is None and to_value is None:
        return "realtime"
    if from_value is not None and to_value is None:
        return "realtime"
    if from_value is not None and to_value is not None:
        return "backfill"
    raise ValueError("--to requires --from")


def _run_inferred_ingest(
    *,
    config_path: str,
    from_value: str | int | None,
    to_value: int | None,
    entity: list[str] | None,
) -> None:
    entities = _parse_entities(entity)
    effective_from = "checkpoint" if from_value is None else from_value
    effective_to = to_value
    effective_eos_enabled = None

    if _infer_ingest_mode(from_value=from_value, to_value=to_value) == "backfill":
        normalized_from = str(effective_from).strip().lower()
        if normalized_from in {"checkpoint", "latest"}:
            raise ValueError("--from must be a numeric cursor when --to is provided")

    config = _load_effective_config(
        config_path=config_path,
        from_value=effective_from,
        to_value=effective_to,
        entities=entities,
        eos_enabled=effective_eos_enabled,
    )
    _run_async(run_pipeline(config_path=config_path, config=config))


def _run_async(coro) -> None:
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        raise typer.Exit(130) from None


def _fail(exc: Exception) -> None:
    typer.secho(str(exc), fg=typer.colors.RED, err=True)
    raise typer.Exit(1) from exc


@app.callback()
def main(
    ctx: typer.Context,
    config_path: str = typer.Option(
        None,
        "--config",
        help="Path to pipeline.yaml.",
    ),
    from_value: str | None = typer.Option(
        None,
        "--from",
        help=(
            "Realtime start cursor or special value. Use checkpoint, latest, or a "
            "numeric cursor. With --to it becomes bounded backfill."
        ),
    ),
    to_value: int | None = typer.Option(
        None,
        "--to",
        help="Optional inclusive end cursor. Requires --from and enables bounded backfill.",
    ),
    entity: list[str] | None = typer.Option(
        None,
        "--entity",
        help="Override entities using comma-separated values or repeated --entity flags.",
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    config_path = config_path or _default_config_path()
    try:
        _run_inferred_ingest(
            config_path=config_path,
            from_value=from_value,
            to_value=to_value,
            entity=entity,
        )
    except Exception as exc:
        _fail(exc)


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
    config_path = config_path or _default_config_path()
    try:
        config = _load_effective_config(config_path=config_path)
    except Exception as exc:
        _fail(exc)
    _run_async(run_dlq_retry(config_path=config_path, config=config, group_id=group_id))


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
    config_path = config_path or _default_config_path()
    try:
        config = _load_effective_config(config_path=config_path)
    except Exception as exc:
        _fail(exc)
    _run_async(
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


@config_app.command("validate")
def config_validate(
    config_path: str = typer.Option(
        None,
        "--config",
        help="Path to pipeline.yaml.",
    ),
    from_value: str | None = typer.Option(
        None,
        "--from",
        help="Optional from cursor override.",
    ),
    to_value: int | None = typer.Option(
        None,
        "--to",
        help="Optional to cursor override.",
    ),
    entity: list[str] | None = typer.Option(
        None,
        "--entity",
        help="Optional entity override using comma-separated values or repeated --entity flags.",
    ),
) -> None:
    config_path = config_path or _default_config_path()
    try:
        entities = _parse_entities(entity)
        config = _load_effective_config(
            config_path=config_path,
            from_value=from_value,
            to_value=to_value,
            entities=entities,
        )
    except Exception as exc:
        _fail(exc)
    typer.echo(
        f"Config valid: pipeline={config.pipeline.name} mode={config.pipeline.mode} "
        f"entities={','.join(config.entities)}"
    )


@config_app.command("print")
def config_print(
    config_path: str = typer.Option(
        None,
        "--config",
        help="Path to pipeline.yaml.",
    ),
    from_value: str | None = typer.Option(
        None,
        "--from",
        help="Optional from cursor override.",
    ),
    to_value: int | None = typer.Option(
        None,
        "--to",
        help="Optional to cursor override.",
    ),
    entity: list[str] | None = typer.Option(
        None,
        "--entity",
        help="Optional entity override using comma-separated values or repeated --entity flags.",
    ),
) -> None:
    config_path = config_path or _default_config_path()
    try:
        entities = _parse_entities(entity)
        config = _load_effective_config(
            config_path=config_path,
            from_value=from_value,
            to_value=to_value,
            entities=entities,
        )
    except Exception as exc:
        _fail(exc)
    typer.echo(_dump_config(config), nl=False)


def cli() -> None:
    app()
