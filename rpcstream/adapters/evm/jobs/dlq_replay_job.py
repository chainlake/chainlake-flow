from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import yaml

from rpcstream.app_runtime import build_runtime_stack
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.planner.dlq_replay import DlqReplayBlockSource
from rpcstream.sinks.kafka.dlq import UnifiedDlqKafkaClient

DEFAULT_REPLAY_GROUP = "rpcstream-dlq-replay"
DEFAULT_NAMESPACE = "ingestion"
DEFAULT_JOB_NAME = "rpcstream-dlq-replay"
DEFAULT_CONFIG_MAP_NAME = "rpcstream-dlq"
DEFAULT_CONFIG_PATH = "/config/pipeline.yaml"
DEFAULT_IMAGE = "rpcstream:dev"
DEFAULT_IMAGE_PULL_POLICY = "IfNotPresent"
DEFAULT_KAFKA_CREDENTIALS_SECRET = "kafka-credentials"
DEFAULT_KAFKA_CA_SECRET = "kafka-ca-secret"
DEFAULT_KAFKA_CA_PATH = "/etc/ssl/certs/ca.pem"
DEFAULT_RESTART_POLICY = "Never"
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
LOCAL_DEFAULT_CONFIG_PATH = os.path.join(REPO_ROOT, "rpcstream", "pipeline.yaml")


class NoAliasSafeDumper(yaml.SafeDumper):
    def ignore_aliases(self, _data):
        return True


@dataclass
class ReplayJobOptions:
    job_name: str = DEFAULT_JOB_NAME
    namespace: str = DEFAULT_NAMESPACE
    image: str = DEFAULT_IMAGE
    image_pull_policy: str = DEFAULT_IMAGE_PULL_POLICY
    config_map_name: str = DEFAULT_CONFIG_MAP_NAME
    config_mount_path: str = "/config"
    kafka_credentials_secret: str = DEFAULT_KAFKA_CREDENTIALS_SECRET
    kafka_ca_secret: str = DEFAULT_KAFKA_CA_SECRET
    kafka_ca_mount_path: str = "/etc/ssl/certs"
    kafka_ca_path: str = DEFAULT_KAFKA_CA_PATH
    group_id: str = f"{DEFAULT_REPLAY_GROUP}-manual"
    offset: str = "earliest"
    entity: str | None = None
    status: str = "failed"
    stage: str | None = None
    max_records: int | None = None
    backoff_limit: int = 0
    termination_grace_period_seconds: int = 5
    request_cpu: str = "200m"
    request_memory: str = "128Mi"
    limit_cpu: str = "500m"
    limit_memory: str = "512Mi"


def default_local_group_id() -> str:
    configured = os.getenv("DLQ_REPLAY_GROUP_ID")
    if configured:
        return configured
    return f"{DEFAULT_REPLAY_GROUP}-local-{int(time.time())}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a DLQ replay locally or render a generic Kubernetes Job manifest.",
    )
    subparsers = parser.add_subparsers(dest="command")

    add_shared_args(parser, include_config=True, local_mode=True)

    render_parser = subparsers.add_parser(
        "render",
        help="Render a Kubernetes Job manifest for DLQ replay.",
    )
    add_shared_args(render_parser, include_config=True, local_mode=False)
    render_parser.add_argument(
        "--output",
        default="-",
        help="Write manifest to this path. Use '-' for stdout.",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run the replay worker inside a Kubernetes Job.",
    )
    add_shared_args(run_parser, include_config=False, local_mode=False)
    return parser


def add_shared_args(parser: argparse.ArgumentParser, *, include_config: bool, local_mode: bool) -> None:
    if include_config:
        parser.add_argument(
            "--config",
            default=os.getenv("PIPELINE_CONFIG", LOCAL_DEFAULT_CONFIG_PATH),
            help="Path to pipeline.yaml used for replay runtime and manifest metadata.",
        )

    parser.add_argument("--job-name", default=os.getenv("DLQ_REPLAY_JOB_NAME", DEFAULT_JOB_NAME))
    parser.add_argument("--namespace", default=os.getenv("K8S_NAMESPACE", DEFAULT_NAMESPACE))
    parser.add_argument("--image", default=os.getenv("DLQ_REPLAY_IMAGE", DEFAULT_IMAGE))
    parser.add_argument(
        "--image-pull-policy",
        default=os.getenv("DLQ_REPLAY_IMAGE_PULL_POLICY", DEFAULT_IMAGE_PULL_POLICY),
    )
    parser.add_argument(
        "--config-map-name",
        default=os.getenv("DLQ_REPLAY_CONFIG_MAP_NAME", DEFAULT_CONFIG_MAP_NAME),
    )
    parser.add_argument(
        "--config-mount-path",
        default=os.getenv("DLQ_REPLAY_CONFIG_MOUNT_PATH", "/config"),
    )
    parser.add_argument(
        "--kafka-credentials-secret",
        default=os.getenv(
            "DLQ_REPLAY_KAFKA_CREDENTIALS_SECRET",
            DEFAULT_KAFKA_CREDENTIALS_SECRET,
        ),
    )
    parser.add_argument(
        "--kafka-ca-secret",
        default=os.getenv("DLQ_REPLAY_KAFKA_CA_SECRET", DEFAULT_KAFKA_CA_SECRET),
    )
    parser.add_argument(
        "--kafka-ca-mount-path",
        default=os.getenv("DLQ_REPLAY_KAFKA_CA_MOUNT_PATH", "/etc/ssl/certs"),
    )
    parser.add_argument(
        "--kafka-ca-path",
        default=os.getenv("KAFKA_CA_PATH", DEFAULT_KAFKA_CA_PATH),
    )
    parser.add_argument(
        "--group-id",
        default=default_local_group_id() if local_mode else os.getenv(
            "DLQ_REPLAY_GROUP_ID",
            f"{DEFAULT_REPLAY_GROUP}-manual",
        ),
    )
    parser.add_argument(
        "--offset",
        choices=("earliest", "latest"),
        default=os.getenv("DLQ_REPLAY_OFFSET", "earliest"),
    )
    parser.add_argument("--entity", default=os.getenv("DLQ_REPLAY_ENTITY"))
    parser.add_argument("--status", default=os.getenv("DLQ_REPLAY_STATUS", "failed"))
    parser.add_argument("--stage", default=os.getenv("DLQ_REPLAY_STAGE"))
    parser.add_argument("--max-records", type=int, default=_env_optional_int("DLQ_REPLAY_MAX_RECORDS"))
    parser.add_argument(
        "--backoff-limit",
        type=int,
        default=int(os.getenv("DLQ_REPLAY_BACKOFF_LIMIT", "0")),
    )
    parser.add_argument(
        "--termination-grace-period-seconds",
        type=int,
        default=int(os.getenv("DLQ_REPLAY_TERMINATION_GRACE_PERIOD_SECONDS", "5")),
    )
    parser.add_argument(
        "--request-cpu",
        default=os.getenv("DLQ_REPLAY_REQUEST_CPU", "200m"),
    )
    parser.add_argument(
        "--request-memory",
        default=os.getenv("DLQ_REPLAY_REQUEST_MEMORY", "128Mi"),
    )
    parser.add_argument(
        "--limit-cpu",
        default=os.getenv("DLQ_REPLAY_LIMIT_CPU", "500m"),
    )
    parser.add_argument(
        "--limit-memory",
        default=os.getenv("DLQ_REPLAY_LIMIT_MEMORY", "512Mi"),
    )


def _env_optional_int(name: str) -> int | None:
    value = os.getenv(name)
    if value in (None, ""):
        return None
    return int(value)


def options_from_args(args: argparse.Namespace) -> ReplayJobOptions:
    return ReplayJobOptions(
        job_name=args.job_name,
        namespace=args.namespace,
        image=args.image,
        image_pull_policy=args.image_pull_policy,
        config_map_name=args.config_map_name,
        config_mount_path=args.config_mount_path,
        kafka_credentials_secret=args.kafka_credentials_secret,
        kafka_ca_secret=args.kafka_ca_secret,
        kafka_ca_mount_path=args.kafka_ca_mount_path,
        kafka_ca_path=args.kafka_ca_path,
        group_id=args.group_id,
        offset=args.offset,
        entity=args.entity,
        status=args.status,
        stage=args.stage,
        max_records=args.max_records,
        backoff_limit=args.backoff_limit,
        termination_grace_period_seconds=args.termination_grace_period_seconds,
        request_cpu=args.request_cpu,
        request_memory=args.request_memory,
        limit_cpu=args.limit_cpu,
        limit_memory=args.limit_memory,
    )


def build_replay_job_manifest(runtime, options: ReplayJobOptions) -> dict[str, Any]:
    labels = {
        "app": options.job_name,
        "chain": runtime.chain.name,
        "pipeline": runtime.pipeline.name,
    }

    container_env = [
        {"name": "PIPELINE_CONFIG", "value": f"{options.config_mount_path}/pipeline.yaml"},
        {"name": "DLQ_REPLAY_GROUP_ID", "value": options.group_id},
        {"name": "DLQ_REPLAY_OFFSET", "value": options.offset},
        {"name": "DLQ_REPLAY_STATUS", "value": options.status},
        {
            "name": "POD_UID",
            "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}},
        },
        {
            "name": "POD_NAME",
            "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
        },
        {
            "name": "KAFKA_USERNAME",
            "valueFrom": {
                "secretKeyRef": {
                    "name": options.kafka_credentials_secret,
                    "key": "KAFKA_USERNAME",
                }
            },
        },
        {
            "name": "KAFKA_PASSWORD",
            "valueFrom": {
                "secretKeyRef": {
                    "name": options.kafka_credentials_secret,
                    "key": "KAFKA_PASSWORD",
                }
            },
        },
        {"name": "KAFKA_CA_PATH", "value": options.kafka_ca_path},
        {
            "name": "KAFKA_BOOTSTRAP_SERVERS",
            "valueFrom": {
                "secretKeyRef": {
                    "name": options.kafka_credentials_secret,
                    "key": "KAFKA_BOOTSTRAP_SERVERS",
                }
            },
        },
        {
            "name": "KAFAK_SCHEMA_REGISTRY",
            "valueFrom": {
                "secretKeyRef": {
                    "name": options.kafka_credentials_secret,
                    "key": "KAFAK_SCHEMA_REGISTRY",
                }
            },
        },
    ]
    _append_optional_env(container_env, "DLQ_REPLAY_ENTITY", options.entity)
    _append_optional_env(container_env, "DLQ_REPLAY_STAGE", options.stage)
    _append_optional_env(container_env, "DLQ_REPLAY_MAX_RECORDS", options.max_records)

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": options.job_name,
            "namespace": options.namespace,
            "labels": labels,
        },
        "spec": {
            "backoffLimit": options.backoff_limit,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": DEFAULT_RESTART_POLICY,
                    "terminationGracePeriodSeconds": options.termination_grace_period_seconds,
                    "containers": [
                        {
                            "name": options.job_name,
                            "image": options.image,
                            "imagePullPolicy": options.image_pull_policy,
                            "command": [
                                "python",
                                "-m",
                                "rpcstream.adapters.evm.jobs.dlq_replay_job",
                                "run",
                            ],
                            "env": container_env,
                            "volumeMounts": [
                                {
                                    "name": "config-volume",
                                    "mountPath": options.config_mount_path,
                                },
                                {
                                    "name": "kafka-ca-volume",
                                    "mountPath": options.kafka_ca_mount_path,
                                },
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": options.request_cpu,
                                    "memory": options.request_memory,
                                },
                                "limits": {
                                    "cpu": options.limit_cpu,
                                    "memory": options.limit_memory,
                                },
                            },
                        }
                    ],
                    "volumes": [
                        {
                            "name": "config-volume",
                            "configMap": {"name": options.config_map_name},
                        },
                        {
                            "name": "kafka-ca-volume",
                            "secret": {"secretName": options.kafka_ca_secret},
                        },
                    ],
                },
            },
        },
    }


def _append_optional_env(env: list[dict[str, Any]], name: str, value: Any) -> None:
    if value is None:
        return
    env.append({"name": name, "value": str(value)})


def render_manifest(args: argparse.Namespace) -> int:
    config = load_pipeline_config(args.config)
    runtime = resolve(config)
    options = options_from_args(args)
    manifest = build_replay_job_manifest(runtime, options)
    text = yaml.dump(manifest, Dumper=NoAliasSafeDumper, sort_keys=False)
    if args.output == "-":
        sys.stdout.write(text)
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
    return 0


async def run_replay(args: argparse.Namespace) -> None:
    config_path = getattr(args, "config", None) or os.getenv("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH)
    stack = build_runtime_stack(config_path=config_path, with_tracker=False)
    client = UnifiedDlqKafkaClient(
        topic=stack.runtime.topic_map.dlq,
        producer_config=stack.runtime.kafka.config,
        schema_registry_url=stack.runtime.kafka.schema_registry_url,
        group_id=args.group_id,
        logger=stack.logger,
        auto_offset_reset=args.offset,
    )
    source = DlqReplayBlockSource(
        client,
        entity=args.entity,
        status=args.status,
        stage=args.stage,
        pipeline=stack.runtime.pipeline.name,
        chain=stack.runtime.chain.type,
        max_records=args.max_records,
        logger=stack.logger,
    )

    await stack.start()
    await stack.engine.sink.start()
    try:
        stack.logger.info(
            "dlq.replay_started",
            component="dlq",
            topic=stack.runtime.topic_map.dlq,
            entity=args.entity,
            status=args.status,
            stage=args.stage,
            max_records=args.max_records,
            group_id=args.group_id,
            offset=args.offset,
        )
        resolved_records = 0
        replayed_blocks = 0
        while True:
            block_number = await source.next_block()
            if block_number is None:
                break

            success, _delivery_futures = await stack.engine._run_one(block_number)
            if not success:
                stack.logger.warn(
                    "dlq.replay_block_failed",
                    component="dlq",
                    block_number=block_number,
                )
                continue

            replayed_blocks += 1
            for record in source.records_for_block(block_number):
                await stack.engine.mark_dlq_resolved(record)
                resolved_records += 1

            stack.logger.info(
                "dlq.replay_block_resolved",
                component="dlq",
                block_number=block_number,
                resolved_records=len(source.records_for_block(block_number)),
            )

        stack.logger.info(
            "dlq.replay_completed",
            component="dlq",
            replayed_blocks=replayed_blocks,
            resolved_records=resolved_records,
        )
    finally:
        client.close()
        await stack.engine.sink.close()
        await stack.close()


def cli() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "render":
        raise SystemExit(render_manifest(args))

    if args.command in (None, "run"):
        asyncio.run(run_replay(args))
        return

    raise SystemExit(f"unknown command: {args.command}")


if __name__ == "__main__":
    cli()
