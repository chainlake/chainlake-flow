from __future__ import annotations

import typer

from rpcstream.cli.common import default_config_path, dump_config, fail, load_effective_config, parse_entities

config_app = typer.Typer(help="Validate and inspect effective config.", no_args_is_help=True)


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
    config_path = config_path or default_config_path()
    try:
        entities = parse_entities(entity)
        config = load_effective_config(
            config_path=config_path,
            from_value=from_value,
            to_value=to_value,
            entities=entities,
        )
    except Exception as exc:
        fail(exc)
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
    config_path = config_path or default_config_path()
    try:
        entities = parse_entities(entity)
        config = load_effective_config(
            config_path=config_path,
            from_value=from_value,
            to_value=to_value,
            entities=entities,
        )
    except Exception as exc:
        fail(exc)
    typer.echo(dump_config(config), nl=False)
