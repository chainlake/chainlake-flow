from __future__ import annotations

from collections.abc import Iterable
import time

TOPIC_TIMESTAMP_CONFIG = "message.timestamp.type"
TOPIC_TIMESTAMP_VALUE = "LogAppendTime"
TOPIC_CLEANUP_POLICY_CONFIG = "cleanup.policy"
TOPIC_COMPACT_POLICY_VALUE = "compact"
TOPIC_COMPACT_DELETE_POLICY_VALUE = "compact,delete"


class KafkaTopicManager:
    def __init__(self, producer_config: dict, logger=None):
        self.producer_config = producer_config
        self.logger = logger

    def ensure_topics(self, topics: Iterable[str]) -> None:
        self._ensure_topics(
            topics,
            config={TOPIC_TIMESTAMP_CONFIG: TOPIC_TIMESTAMP_VALUE},
        )
        self._wait_for_topics(topics)

    def ensure_compacted_topics(self, topics: Iterable[str]) -> None:
        self._ensure_topics(
            topics,
            config={
                TOPIC_TIMESTAMP_CONFIG: TOPIC_TIMESTAMP_VALUE,
                TOPIC_CLEANUP_POLICY_CONFIG: TOPIC_COMPACT_DELETE_POLICY_VALUE,
            },
        )
        self._wait_for_topics(topics)
        self._ensure_compaction(topics)

    def delete_topics(self, topics: Iterable[str]) -> None:
        from confluent_kafka.admin import AdminClient

        admin = self._admin_client()
        unique_topics = sorted({topic for topic in topics if topic})
        if not unique_topics:
            return

        futures = admin.delete_topics(unique_topics)
        for topic, future in futures.items():
            try:
                future.result()
                if self.logger:
                    self.logger.info(
                        "kafka.topic_deleted",
                        topic=topic,
                    )
            except Exception as exc:
                if "UNKNOWN_TOPIC_OR_PARTITION" in str(exc) or "UNKNOWN_TOPIC" in str(exc):
                    continue
                raise

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
            if current_value == TOPIC_COMPACT_DELETE_POLICY_VALUE:
                continue

            update = ConfigResource(RESOURCE_TOPIC, resource.name)
            update.add_incremental_config(
                ConfigEntry(
                    TOPIC_CLEANUP_POLICY_CONFIG,
                    TOPIC_COMPACT_DELETE_POLICY_VALUE,
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
                    topic=resource.name,
                    cleanup_policy=TOPIC_COMPACT_DELETE_POLICY_VALUE,
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
                    topic=resource.name,
                    message_timestamp_type=TOPIC_TIMESTAMP_VALUE,
                )

    def _wait_for_topics(
        self,
        topics: Iterable[str],
        *,
        timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        unique_topics = sorted({topic for topic in topics if topic})
        if not unique_topics:
            return

        admin = self._admin_client()
        deadline = time.monotonic() + timeout_seconds
        last_error = None

        while time.monotonic() < deadline:
            try:
                metadata = admin.list_topics(timeout=min(5.0, timeout_seconds))
                missing = [
                    topic
                    for topic in unique_topics
                    if not self._topic_is_visible(metadata, topic)
                ]
                if not missing:
                    return
                last_error = RuntimeError(
                    f"topics not visible yet: {', '.join(missing)}"
                )
            except Exception as exc:
                last_error = exc
                if "UNKNOWN_TOPIC_OR_PARTITION" not in str(exc) and "UNKNOWN_TOPIC" not in str(exc):
                    # Any other failure is likely a transient cluster startup issue; keep polling
                    # until the timeout so a fresh cluster can settle after cleanup.
                    pass

            time.sleep(poll_interval_seconds)

        message = f"timed out waiting for topics to become visible: {', '.join(unique_topics)}"
        if self.logger:
            self.logger.warning(
                "kafka.topic_visibility_timeout",
                topics=unique_topics,
                timeout_seconds=timeout_seconds,
            )
        raise TimeoutError(message) from last_error

    def _topic_is_visible(self, metadata, topic: str) -> bool:
        topic_meta = getattr(metadata, "topics", {}).get(topic)
        if topic_meta is None:
            return False

        error = getattr(topic_meta, "error", None)
        if error is None:
            return True

        code = getattr(error, "code", None)
        if callable(code):
            code = code()
        return code in (None, 0)

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
