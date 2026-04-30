# build runtime configs (kafka, rpc)
import os
import socket

from rpcstream.adapters import build_chain_adapter
from rpcstream.runtime.topic import (
    TopicMaps,
    build_checkpoint_topic,
    build_topics,
    build_unified_dlq_topic,
    build_watermark_state_topic,
    normalize_entity,
)

from .schema import PipelineConfig
from .naming import build_pipeline_name
from .profiles.store import get_chain_profile


def build_kafka_config(cfg: PipelineConfig) -> dict:
    kafka = cfg.kafka
    connection = kafka.connection

    result = {
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            connection.bootstrap_servers,
        ),
    }
    if connection.security_protocol:
        result["security.protocol"] = os.getenv(
            "KAFKA_SECURITY_PROTOCOL",
            connection.security_protocol,
        )

    if connection.sasl_mechanism:
        result["sasl.mechanism"] = os.getenv(
            "KAFKA_SASL_MECHANISM",
            connection.sasl_mechanism,
        )

    username_env = connection.auth.username_env
    password_env = connection.auth.password_env
    if username_env:
        username = os.getenv(username_env)
        if username:
            result["sasl.username"] = username
    if password_env:
        password = os.getenv(password_env)
        if password:
            result["sasl.password"] = password

    ca_path_env = connection.ssl.ca_path_env
    if ca_path_env:
        ca_path = os.getenv(ca_path_env)
        if ca_path:
            result["ssl.ca.location"] = ca_path

    # -------------------------
    # Producer tuning
    # -------------------------
    result["linger.ms"] = kafka.producer.linger_ms
    result["batch.size"] = kafka.producer.batch_size
    result["compression.type"] = os.getenv(
        "KAFKA_COMPRESSION_TYPE",
        kafka.producer.compression_type,
    )

    if kafka.eos.enabled:
        result["enable.idempotence"] = True
        result["acks"] = "all"
        result["transactional.id"] = build_transactional_id(cfg)
        result["transaction.timeout.ms"] = kafka.eos.transaction_timeout_ms

    return result


def build_transactional_id(cfg: PipelineConfig) -> str:
    template = cfg.kafka.eos.transactional_id_template
    entities = ",".join(sorted(cfg.entities))
    chain_uid = getattr(cfg.chain, "uid", None)
    if not chain_uid:
        chain_uid = get_chain_profile(cfg.chain.name, cfg.chain.network).chain_uid
    pipeline_name = getattr(cfg.pipeline, "name", None) or build_pipeline_name(
        chain_name=cfg.chain.name,
        network=cfg.chain.network,
        mode=cfg.pipeline.mode,
        from_value=cfg.pipeline.from_,
        to_value=cfg.pipeline.to,
    )
    return template.format(
        pipeline=pipeline_name,
        chain_uid=chain_uid,
        chain_type=cfg.chain.type,
        chain_name=cfg.chain.name,
        network=cfg.chain.network,
        mode=cfg.pipeline.mode,
        entities=entities,
        hostname=os.getenv("HOSTNAME", socket.gethostname()),
        pid=os.getpid(),
        pod_uid=os.getenv("POD_UID", ""),
    )


def build_schema_registry_url(cfg: PipelineConfig) -> str | None:
    raw = (
        os.getenv("KAFAK_SCHEMA_REGISTRY")
        or os.getenv("KAFKA_SCHEMA_REGISTRY")
        or cfg.kafka.protobuf.schema_registry_url
    )
    if not raw:
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    return f"https://{raw}"


def build_topic_maps(cfg, adapter=None) -> TopicMaps:
    """
    Convert TopicSet → engine-compatible maps
    """
    adapter = adapter or build_chain_adapter(cfg.chain.type)

    topics = {}

    for entity in adapter.resolve_sink_entities(cfg.entities):
        normalized = normalize_entity(entity)
        topic_set = build_topics(cfg, normalized, adapter=adapter)

        topics[normalized] = topic_set.main

    return TopicMaps(
        main=topics,
        dlq=build_unified_dlq_topic(cfg),
        checkpoint=build_checkpoint_topic(cfg),
        watermark_state=build_watermark_state_topic(cfg),
    )


def build_erpc_endpoint(cfg) -> str:
    profile = get_chain_profile(cfg.chain.name, cfg.chain.network)
    chain_type = profile.chain_type
    chain_id = profile.chain_uid.split(":")[-1]

    return (
        f"{cfg.erpc.base_url}/"
        f"{cfg.erpc.project_id}/"
        f"{chain_type}/{chain_id}"
    )
