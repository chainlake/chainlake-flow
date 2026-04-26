from __future__ import annotations

from collections.abc import Iterable

TOPIC_TIMESTAMP_CONFIG = "message.timestamp.type"
TOPIC_TIMESTAMP_VALUE = "LogAppendTime"
TOPIC_CLEANUP_POLICY_CONFIG = "cleanup.policy"
TOPIC_COMPACT_POLICY_VALUE = "compact"


class KafkaTopicManager:
    def __init__(self, producer_config: dict, logger=None):
        self.producer_config = producer_config
        self.logger = logger

    def ensure_topics(self, topics: Iterable[str]) -> None:
        self._ensure_topics(
            topics,
            config={TOPIC_TIMESTAMP_CONFIG: TOPIC_TIMESTAMP_VALUE},
        )

    def ensure_compacted_topics(self, topics: Iterable[str]) -> None:
        self._ensure_topics(
            topics,
            config={
                TOPIC_TIMESTAMP_CONFIG: TOPIC_TIMESTAMP_VALUE,
                TOPIC_CLEANUP_POLICY_CONFIG: TOPIC_COMPACT_POLICY_VALUE,
            },
        )
        self._ensure_compaction(topics)

    def _ensure_topics(self, topics: Iterable[str], config: dict[str, str]) -> None:
        from confluent_kafka.admin import NewTopic

        admin = self._admin_client()
        unique_topics = sorted({topic for topic in topics if topic})
        if not unique_topics:
            return

        futures = admin.create_topics(
            [
                NewTopic(
                    topic=topic,
                    num_partitions=-1,
                    replication_factor=-1,
                    config=config,
                )
                for topic in unique_topics
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                if self.logger:
                    self.logger.info(
                        "kafka.topic_created",
                        component="sink",
                        topic=topic,
                        config=config,
                    )
            except Exception as exc:
                if "TOPIC_ALREADY_EXISTS" in str(exc):
                    continue
                raise

        self._ensure_log_append_time(admin, unique_topics)

    def _ensure_compaction(self, topics: Iterable[str]) -> None:
        from confluent_kafka.admin import (
            AlterConfigOpType,
            ConfigEntry,
            ConfigResource,
            RESOURCE_TOPIC,
        )

        admin = self._admin_client()
        unique_topics = sorted({topic for topic in topics if topic})
        resources = [
            ConfigResource(RESOURCE_TOPIC, topic)
            for topic in unique_topics
        ]
        described = admin.describe_configs(resources)

        updates = []
        for resource, future in described.items():
            config = future.result()
            current_value = self._config_entry_value(config.get(TOPIC_CLEANUP_POLICY_CONFIG))
            if current_value == TOPIC_COMPACT_POLICY_VALUE:
                continue

            update = ConfigResource(RESOURCE_TOPIC, resource.name)
            update.add_incremental_config(
                ConfigEntry(
                    TOPIC_CLEANUP_POLICY_CONFIG,
                    TOPIC_COMPACT_POLICY_VALUE,
                    incremental_operation=AlterConfigOpType.SET,
                )
            )
            updates.append(update)

        if not updates:
            return

        altered = admin.incremental_alter_configs(updates)
        for resource, future in altered.items():
            future.result()
            if self.logger:
                self.logger.info(
                    "kafka.topic_compaction_updated",
                    component="sink",
                    topic=resource.name,
                    cleanup_policy=TOPIC_COMPACT_POLICY_VALUE,
                )

    def _ensure_log_append_time(self, admin, topics: list[str]) -> None:
        from confluent_kafka.admin import (
            AlterConfigOpType,
            ConfigEntry,
            ConfigResource,
            RESOURCE_TOPIC,
        )

        resources = [
            ConfigResource(RESOURCE_TOPIC, topic)
            for topic in topics
        ]
        described = admin.describe_configs(resources)

        updates = []
        for resource, future in described.items():
            config = future.result()
            current_value = self._config_entry_value(config.get(TOPIC_TIMESTAMP_CONFIG))
            if current_value == TOPIC_TIMESTAMP_VALUE:
                continue

            update = ConfigResource(RESOURCE_TOPIC, resource.name)
            update.add_incremental_config(
                ConfigEntry(
                    TOPIC_TIMESTAMP_CONFIG,
                    TOPIC_TIMESTAMP_VALUE,
                    incremental_operation=AlterConfigOpType.SET,
                )
            )
            updates.append(update)

        if not updates:
            return

        altered = admin.incremental_alter_configs(updates)
        for resource, future in altered.items():
            future.result()
            if self.logger:
                self.logger.info(
                    "kafka.topic_timestamp_updated",
                    component="sink",
                    topic=resource.name,
                    message_timestamp_type=TOPIC_TIMESTAMP_VALUE,
                )

    def _config_entry_value(self, entry):
        if entry is None:
            return None
        return getattr(entry, "value", entry)

    def _admin_client(self):
        from confluent_kafka.admin import AdminClient

        return AdminClient(self._admin_config())

    def _admin_config(self) -> dict:
        allowed_prefixes = (
            "bootstrap.servers",
            "security.protocol",
            "sasl.",
            "ssl.",
        )
        return {
            key: value
            for key, value in self.producer_config.items()
            if any(key.startswith(prefix) for prefix in allowed_prefixes)
        }
