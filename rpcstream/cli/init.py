from __future__ import annotations

import os

import typer

from rpcstream.cli.common import default_config_path, fail
from rpcstream.kafka_init import main as run_kafka_init


def kafka_init(
    config_path: str = typer.Option(
        None,
        "--config",
        help="Path to pipeline.yaml.",
    ),
) -> None:
    config_path = config_path or default_config_path()
    previous = os.environ.get("PIPELINE_CONFIG")
    os.environ["PIPELINE_CONFIG"] = config_path
    try:
        run_kafka_init()
    except Exception as exc:
        fail(exc)
    finally:
        if previous is None:
            os.environ.pop("PIPELINE_CONFIG", None)
        else:
            os.environ["PIPELINE_CONFIG"] = previous


__all__ = [
    "kafka_init",
]
