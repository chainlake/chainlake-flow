import os

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.sinks.kafka.bootstrap import bootstrap_kafka_resources
from rpcstream.utils.logger import JsonLogger


def main() -> None:
    config_path = os.getenv("PIPELINE_CONFIG", "pipeline.yaml")
    config = load_pipeline_config(config_path)
    runtime = resolve(config)
    logger = JsonLogger(level=config.logLevel)

    logger.info(
        "kafka.bootstrap_started",
        component="sink",
        config_path=config_path,
        pipeline=runtime.pipeline.name,
    )
    bootstrap_kafka_resources(runtime, logger=logger)


def cli() -> None:
    main()


if __name__ == "__main__":
    main()
