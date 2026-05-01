from __future__ import annotations

import typer

from rpcstream.cli.benchmark import benchmark as benchmark_command
from rpcstream.cli.common import default_config_path, fail
from rpcstream.cli.common import infer_ingest_mode as _infer_ingest_mode
from rpcstream.cli.common import parse_entities as _parse_entities
from rpcstream.cli.config import config_app
from rpcstream.cli.dlq import dlq_app
from rpcstream.cli.ingest import run_ingest
from rpcstream.cli.init import kafka_init as kafka_init_command

app = typer.Typer(
    help=(
        "Run realtime or backfill ingestion from the root command. "
        "Use explicit subcommands only for DLQ and config workflows."
    ),
    epilog=(
        "Ingestion rules:\n\n"
        "1. No --from and no --to:\n"
        "   realtime mode; resume from checkpoint when available, otherwise start from chainhead.\n\n"
        "2. --from chainhead:\n"
        "   realtime mode; ignore any saved checkpoint and start from the current chainhead.\n\n"
        "3. --from <cursor> only:\n"
        "   realtime mode; start from that cursor and continue streaming.\n\n"
        "4. --from <cursor> and --to <cursor>:\n"
        "   backfill mode; run a bounded cursor range.\n\n"
        "Examples:\n\n"
        "rpcstream --entity block,transaction\n\n"
        "rpcstream --from chainhead --entity block\n\n"
        "rpcstream --from 95000000 --entity block,transaction\n\n"
        "rpcstream --from 95000000 --to 95000100 --entity block,transaction\n\n"
        "rpcstream benchmark --mode concurrent --sink blackhole --output-file benchmark.json\n\n"
        "rpcstream dlq retry\n\n"
        "rpcstream config print"
    ),
    invoke_without_command=True,
)

app.add_typer(dlq_app, name="dlq")
app.add_typer(config_app, name="config")
app.command("benchmark", help="Benchmark ingestion latency and throughput.")(benchmark_command)


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
            "Realtime start cursor or special value. Use checkpoint, chainhead, or a "
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

    config_path = config_path or default_config_path()
    try:
        run_ingest(
            config_path=config_path,
            from_value=from_value,
            to_value=to_value,
            entity=entity,
        )
    except Exception as exc:
        fail(exc)


app.command("init", help="Provision Kafka topics and pre-register adapter schemas.")(kafka_init_command)


def cli() -> None:
    app()

__all__ = [
    "_infer_ingest_mode",
    "_parse_entities",
    "app",
    "cli",
    "config_app",
    "dlq_app",
]
