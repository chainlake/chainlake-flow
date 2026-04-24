# build runtime configs (kafka, rpc)
import os
from .schema import PipelineConfig
from rpcstream.runtime.topic import TopicMaps, build_topics, build_unified_dlq_topic, normalize_entity

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

    return result


def build_schema_registry_url() -> str | None:
    raw = (
        os.getenv("KAFAK_SCHEMA_REGISTRY")
        or os.getenv("KAFKA_SCHEMA_REGISTRY")
    )
    if not raw:
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    return f"https://{raw}"


def build_topic_maps(cfg) -> TopicMaps:
    """
    Convert TopicSet → engine-compatible maps
    """

    topics = {}

    for entity in cfg.entities:
        normalized = normalize_entity(entity)
        topic_set = build_topics(cfg, normalized)

        topics[normalized] = topic_set.main

    return TopicMaps(
        main=topics,
        dlq=build_unified_dlq_topic(cfg),
    )


def build_erpc_endpoint(cfg) -> str:
    chain_type = cfg.chain.type
    chain_id = cfg.chain.uid.split(":")[-1]

    return (
        f"{cfg.erpc.base_url}/"
        f"{cfg.erpc.project_id}/"
        f"{chain_type}/{chain_id}"
    )
