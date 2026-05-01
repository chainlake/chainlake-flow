from types import SimpleNamespace

from rpcstream.sinks.kafka.admin import KafkaTopicManager
from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator


class _Future:
    def __init__(self, value=None):
        self._value = value

    def result(self):
        return self._value


class _AdminStub:
    def __init__(self):
        self.incremental_updates = []
        self.deleted_topics = []

    def describe_configs(self, resources):
        return {
            resources[0]: _Future({"message.timestamp.type": SimpleNamespace(value="CreateTime")}),
            resources[1]: _Future({"message.timestamp.type": SimpleNamespace(value="LogAppendTime")}),
        }

    def incremental_alter_configs(self, resources):
        self.incremental_updates = resources
        return {resource: _Future(None) for resource in resources}

    def delete_topics(self, topics):
        self.deleted_topics = list(topics)
        return {topic: _Future(None) for topic in topics}


def test_config_entry_value_supports_objects_and_plain_values():
    manager = KafkaTopicManager(producer_config={})

    assert manager._config_entry_value(SimpleNamespace(value="LogAppendTime")) == "LogAppendTime"
    assert manager._config_entry_value("CreateTime") == "CreateTime"
    assert manager._config_entry_value(None) is None


def test_ensure_log_append_time_updates_only_mismatched_topics():
    manager = KafkaTopicManager(producer_config={})
    admin = _AdminStub()

    manager._ensure_log_append_time(admin, ["raw_topic", "dlq_topic"])

    assert len(admin.incremental_updates) == 1
    assert admin.incremental_updates[0].name == "raw_topic"


def test_event_id_calculator_uses_enriched_transaction_identity():
    calculator = EventIdCalculator()

    event_id = calculator.calculate_event_id(
        {
            "type": "transaction",
            "block_hash": "0xabc",
            "transaction_index": 50,
        }
    )

    assert event_id == "enriched_transaction_0xabc_50"


def test_ensure_compacted_topics_uses_compact_delete_policy():
    manager = KafkaTopicManager(producer_config={})
    captured = {}

    manager._ensure_topics = lambda topics, config: captured.update(
        {"topics": list(topics), "config": config}
    )
    manager._ensure_compaction = lambda topics: captured.update(
        {"compaction_topics": list(topics)}
    )

    manager.ensure_compacted_topics(["checkpoint-topic"])

    assert captured["topics"] == ["checkpoint-topic"]
    assert captured["compaction_topics"] == ["checkpoint-topic"]
    assert captured["config"]["cleanup.policy"] == "compact,delete"


def test_delete_topics_uses_admin_delete_topics():
    manager = KafkaTopicManager(producer_config={})
    admin = _AdminStub()
    manager._admin_client = lambda: admin

    manager.delete_topics(["checkpoint-topic", "cursor-state-topic"])

    assert admin.deleted_topics == ["checkpoint-topic", "cursor-state-topic"]
