from __future__ import annotations

import json
import warnings

from rpcstream.adapters.evm.schema import EntitySchema, FieldSchema


DLQ_SCHEMA = EntitySchema(
    entity="dlq",
    message_name="UnifiedDlqRecord",
    fields=(
        FieldSchema("chain", "string"),
        FieldSchema("network", "string"),
        FieldSchema("pipeline", "string"),
        FieldSchema("entity", "string"),
        FieldSchema("block_number", "int64"),
        FieldSchema("stage", "string"),
        FieldSchema("error_type", "string"),
        FieldSchema("error_message", "string"),
        FieldSchema("payload", "string"),
        FieldSchema("context", "string"),
        FieldSchema("retry_count", "int64"),
        FieldSchema("max_retry", "int64"),
        FieldSchema("status", "string"),
        FieldSchema("first_seen_at", "int64"),
        FieldSchema("last_attempt_at", "int64"),
        FieldSchema("next_retry_at", "int64"),
        FieldSchema("id", "string"),
        FieldSchema("ingest_timestamp", "int64"),
    ),
)


TYPE_MAP = {
    "string": "string",
    "int64": "int64",
    "bool": "bool",
}


class ProtobufSerializerRegistry:
    def __init__(
        self,
        schema_registry_url: str,
        producer_config: dict,
        topic_schemas: dict[str, EntitySchema],
        auto_register_schemas: bool = True,
        logger=None,
    ):
        self.schema_registry_url = schema_registry_url
        self.producer_config = producer_config
        self.topic_schemas = topic_schemas
        self.auto_register_schemas = auto_register_schemas
        self.logger = logger
        self._serializers = {}
        self._started = False

    def prepare(self) -> None:
        for topic, schema in self.topic_schemas.items():
            if topic in self._serializers:
                continue
            self._serializers[topic] = self._build_serializer(topic, schema)

    def start(self) -> None:
        if self._started:
            return
        self.prepare()
        for topic, schema in self.topic_schemas.items():
            self._serializers[topic]["serializer"](
                self._serializers[topic]["message_class"](),
                self._serialization_context(topic),
            )
            if self.logger:
                self.logger.info(
                    "kafka.protobuf_schema_ready",
                    component="sink",
                    topic=topic,
                    message_name=schema.message_name,
                    schema_registry=self.schema_registry_url,
                )
        self._started = True

    def serialize(self, topic: str, row: dict) -> bytes:
        entry = self._serializers.get(topic)
        if entry is None:
            schema = self.topic_schemas.get(topic)
            if schema is None:
                raise KeyError(f"missing protobuf serializer for topic {topic}")
            entry = self._build_serializer(topic, schema)
            self._serializers[topic] = entry

        message = entry["message_class"]()
        self._populate_message(message, entry["schema"], row)
        return entry["serializer"](message, self._serialization_context(topic))

    def _build_serializer(self, topic: str, schema: EntitySchema) -> dict:
        SchemaRegistryClient, ProtobufSerializer = _import_schema_registry_components()

        client = SchemaRegistryClient(self._schema_registry_conf())
        message_class = build_message_class(schema)
        serializer = ProtobufSerializer(
            message_class,
            client,
            conf={"auto.register.schemas": self.auto_register_schemas},
        )
        return {
            "schema": schema,
            "message_class": message_class,
            "serializer": serializer,
        }

    def _schema_registry_conf(self) -> dict:
        username = self.producer_config.get("sasl.username")
        password = self.producer_config.get("sasl.password")

        conf = {"url": self.schema_registry_url}
        if username and password:
            conf["basic.auth.user.info"] = f"{username}:{password}"
        return conf

    def _serialization_context(self, topic: str):
        from confluent_kafka.serialization import MessageField, SerializationContext

        return SerializationContext(topic, MessageField.VALUE)

    def _populate_message(self, message, schema: EntitySchema, row: dict) -> None:
        for field in schema.fields:
            value = row.get(field.name)
            if value is None:
                continue

            normalized = normalize_value(field, value)
            if field.repeated:
                getattr(message, field.name).extend(normalized)
            else:
                setattr(message, field.name, normalized)


def normalize_value(field: FieldSchema, value):
    if field.repeated:
        if not isinstance(value, list):
            value = [value]
        return [normalize_scalar(field.scalar_type, item) for item in value]
    return normalize_scalar(field.scalar_type, value)


def normalize_scalar(scalar_type: str, value):
    if scalar_type == "string":
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"))
        return str(value)
    if scalar_type == "int64":
        return int(value)
    if scalar_type == "bool":
        return bool(value)
    raise ValueError(f"unsupported protobuf scalar type: {scalar_type}")


def build_message_class(schema: EntitySchema):
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    file_descriptor = descriptor_pb2.FileDescriptorProto()
    file_descriptor.name = f"{schema.entity}.proto"
    file_descriptor.package = "rpcstream.evm"
    file_descriptor.syntax = "proto3"

    message_descriptor = file_descriptor.message_type.add()
    message_descriptor.name = schema.message_name

    for index, field in enumerate(schema.fields, start=1):
        field_descriptor = message_descriptor.field.add()
        field_descriptor.name = field.name
        field_descriptor.number = index
        field_descriptor.label = (
            descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
            if field.repeated
            else descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        )
        field_descriptor.type = {
            "string": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
            "int64": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
            "bool": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
        }[field.scalar_type]

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_descriptor)
    descriptor = pool.FindMessageTypeByName(
        f"{file_descriptor.package}.{schema.message_name}"
    )
    return message_factory.GetMessageClass(descriptor)


def _import_schema_registry_components():
    with warnings.catch_warnings():
        try:
            from authlib.deprecate import AuthlibDeprecationWarning
        except Exception:
            AuthlibDeprecationWarning = DeprecationWarning

        warnings.filterwarnings(
            "ignore",
            category=AuthlibDeprecationWarning,
            module=r"authlib\._joserfc_helpers",
        )

        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

    return SchemaRegistryClient, ProtobufSerializer
