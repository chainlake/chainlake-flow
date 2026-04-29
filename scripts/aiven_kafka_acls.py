#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rpcstream.config.loader import load_pipeline_config  # noqa: E402
from rpcstream.config.resolver import resolve  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Kafka-native ACLs in Aiven for rpcstream pipelines."
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "rpcstream" / "pipeline.yaml"),
        help="Path to rpcstream pipeline.yaml.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create missing ACLs. Without this flag, only prints the planned ACLs.",
    )
    return parser.parse_args()


def aiven_request(method: str, url: str, token: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read().decode("utf-8")
            return response.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8")
        try:
            parsed = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            parsed = {"message": payload}
        return exc.code, parsed


def require_env(name: str) -> str:
    import os

    value = os.getenv(name)
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def desired_acls(runtime) -> list[dict]:
    principal = f"User:{require_env('KAFKA_USERNAME')}"
    topic_prefixes = sorted(
        {
            _prefix_for_topic(topic)
            for topic in runtime.topic_map.main.values()
        }
    )

    acls: list[dict] = []

    def add(operation: str, resource_type: str, resource_name: str, pattern_type: str) -> None:
        acls.append(
            {
                "permission_type": "ALLOW",
                "principal": principal,
                "operation": operation,
                "resource_type": resource_type,
                "resource_name": resource_name,
                "pattern_type": pattern_type,
                "host": "*",
            }
        )

    if runtime.kafka.eos_enabled:
        tx_id = runtime.kafka.transactional_id
        if not tx_id:
            raise SystemExit("kafka.eos.enabled=true but transactional.id was not resolved")
        add("Describe", "TransactionalId", tx_id, "LITERAL")
        add("Write", "TransactionalId", tx_id, "LITERAL")
        add("All", "TransactionalId", tx_id, "LITERAL")
        add("IdempotentWrite", "Cluster", "kafka-cluster", "LITERAL")

    for prefix in topic_prefixes:
        add("Describe", "Topic", prefix, "PREFIXED")
        add("Write", "Topic", prefix, "PREFIXED")

    dlq_topic = runtime.topic_map.dlq
    if dlq_topic:
        add("Describe", "Topic", dlq_topic, "LITERAL")
        add("Write", "Topic", dlq_topic, "LITERAL")

    add("Describe", "Topic", runtime.checkpoint.topic, "LITERAL")
    add("Read", "Topic", runtime.checkpoint.topic, "LITERAL")
    add("Write", "Topic", runtime.checkpoint.topic, "LITERAL")
    add("Read", "Group", "checkpoint-loader-", "PREFIXED")
    add("Describe", "Group", "checkpoint-loader-", "PREFIXED")

    return _dedupe_acls(acls)


def _prefix_for_topic(topic: str) -> str:
    parts = topic.rsplit(".", 1)
    if len(parts) == 1:
        return topic
    return f"{parts[0]}."


def _acl_key(acl: dict) -> tuple:
    return (
        acl.get("permission_type"),
        acl.get("principal"),
        acl.get("operation"),
        acl.get("resource_type"),
        acl.get("resource_name"),
        acl.get("pattern_type"),
        acl.get("host", "*"),
    )


def _dedupe_acls(acls: list[dict]) -> list[dict]:
    seen = set()
    result = []
    for acl in acls:
        key = _acl_key(acl)
        if key in seen:
            continue
        seen.add(key)
        result.append(acl)
    return result


def acl_label(acl: dict) -> str:
    return (
        f"{acl['resource_type']} {acl['resource_name']} "
        f"{acl['operation']} {acl['pattern_type']}"
    )


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    runtime = resolve(load_pipeline_config(str(config_path)))
    acls = desired_acls(runtime)

    print(f"config={config_path}")
    print(f"aiven_project={require_env('AIVEN_PROJECT')}")
    print(f"aiven_service={require_env('AIVEN_SERVICE')}")
    if runtime.kafka.eos_enabled:
        print(f"transactional_id={runtime.kafka.transactional_id}")

    if not args.apply:
        print("dry_run=true")
        for acl in acls:
            print(f"plan {acl_label(acl)}")
        return 0

    base = (
        f"https://api.aiven.io/v1/project/{require_env('AIVEN_PROJECT')}"
        f"/service/{require_env('AIVEN_SERVICE')}/kafka/acl"
    )
    token = require_env("AIVEN_TOKEN")
    status, payload = aiven_request("GET", base, token)
    if status >= 400:
        raise SystemExit(f"list ACLs failed {status}: {payload}")

    existing = {
        _acl_key(item)
        for item in payload.get("kafka_acl", []) or []
    }

    created = 0
    skipped = 0
    for acl in acls:
        label = acl_label(acl)
        if _acl_key(acl) in existing:
            print(f"exists {label}")
            skipped += 1
            continue

        status, payload = aiven_request("POST", base, token, acl)
        if status in (200, 201):
            print(f"created {label}")
            created += 1
        elif status == 409:
            print(f"exists {label}")
            skipped += 1
        else:
            raise SystemExit(f"create ACL failed {status} {label}: {payload}")

    print(f"summary created={created} existing={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
