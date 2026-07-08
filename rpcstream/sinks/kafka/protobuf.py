from __future__ import annotations

import inspect
import json
import os
import warnings
from urllib.parse import urlparse

from rpcstream.sinks.kafka.schema import (
    CHECKPOINT_SCHEMA,
    DLQ_SCHEMA,
    WATERMARK_STATE_SCHEMA,
    EntitySchema,
    FieldSchema,
)


TYPE_MAP = {
    "string": "string",
    "int64": "int64",
    "bool": "bool",
}

AVRO_TYPE_MAP = {
    "string": "string",
    "int64": "long",
    "bool": "boolean",
}


class SchemaRegistrySerializerRegistry:
    def __init__(
        self,
        schema_registry_url: str,
        producer_config: dict,
        topic_schemas: dict[str, EntitySchema],
        auto_register_schemas: bool = True,
        logger=None,
        schema_format: str = "protobuf",
    ):
        self.schema_registry_url = schema_registry_url
        self.producer_config = producer_config
        self.topic_schemas = topic_schemas
        self.auto_register_schemas = auto_register_schemas
        self.logger = logger
        self.schema_format = _normalize_schema_format(schema_format)
        self._serializers = {}
        self._started = False
        _ensure_schema_registry_host_bypasses_proxy(schema_registry_url)

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
            entry = self._serializers[topic]
            payload = self._empty_payload(entry["schema"])
            entry["serializer"](
                payload,
                self._serialization_context(topic),
            )
            if self.logger:
                self.logger.debug(
                    "kafka.schema_ready",
                    component="sink",
                    topic=topic,
                    schema_format=self.schema_format,
                    message_name=schema.message_name,
                    schema_registry=self.schema_registry_url,
                )
        self._started = True

    def serialize(self, topic: str, row: dict) -> bytes:
        entry = self._serializers.get(topic)
        if entry is None:
            schema = self.topic_schemas.get(topic)
            if schema is None:
                raise KeyError(f"missing schema serializer for topic {topic}")
            entry = self._build_serializer(topic, schema)
            self._serializers[topic] = entry

        if self.schema_format == "protobuf":
            message = entry["message_class"]()
            self._populate_message(message, entry["schema"], row)
            return entry["serializer"](message, self._serialization_context(topic))

        normalized = self._normalize_record(entry["schema"], row)
        return entry["serializer"](normalized, self._serialization_context(topic))

    def build_deserializer(self, topic: str):
        entry = self._serializers.get(topic)
        if entry is None:
            schema = self.topic_schemas.get(topic)
            if schema is None:
                raise KeyError(f"missing schema serializer for topic {topic}")
            entry = self._build_serializer(topic, schema)
            self._serializers[topic] = entry

        return entry["deserializer"]

    def _build_serializer(self, topic: str, schema: EntitySchema) -> dict:
        SchemaRegistryClient, SerializerCls, DeserializerCls = _import_schema_registry_components(
            self.schema_format
        )

        client = SchemaRegistryClient(self._schema_registry_conf())
        if self.schema_format == "protobuf":
            message_class = build_message_class(schema)
            serializer = _instantiate_schema_registry_serializer(
                SerializerCls,
                client,
                schema,
                schema_format=self.schema_format,
                message_class=message_class,
                auto_register_schemas=self.auto_register_schemas,
            )
            deserializer = _instantiate_schema_registry_deserializer(
                DeserializerCls,
                client,
                schema,
                schema_format=self.schema_format,
                message_class=message_class,
            )
            return {
                "schema": schema,
                "message_class": message_class,
                "serializer": serializer,
                "deserializer": deserializer,
            }

        avro_schema = build_avro_schema(schema)
        serializer = _instantiate_schema_registry_serializer(
            SerializerCls,
            client,
            schema,
            schema_format=self.schema_format,
            avro_schema=avro_schema,
            auto_register_schemas=self.auto_register_schemas,
        )
        deserializer = _instantiate_schema_registry_deserializer(
            DeserializerCls,
            client,
            schema,
            schema_format=self.schema_format,
            avro_schema=avro_schema,
        )
        return {
            "schema": schema,
            "avro_schema": avro_schema,
            "serializer": serializer,
            "deserializer": deserializer,
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

    def _empty_payload(self, schema: EntitySchema):
        if self.schema_format == "protobuf":
            return build_message_class(schema)()
        return {}

    def _normalize_record(self, schema: EntitySchema, row: dict) -> dict:
        normalized: dict[str, object] = {}
        for field in schema.fields:
            value = row.get(field.name)
            if value is None:
                continue
            normalized[field.name] = normalize_value(field, value)
        return normalized

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
    file_descriptor.package = schema.package
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


def build_avro_schema(schema: EntitySchema) -> str:
    avro_fields = []
    for field in schema.fields:
        avro_type = AVRO_TYPE_MAP[field.scalar_type]
        if field.repeated:
            avro_type = {"type": "array", "items": avro_type}
        avro_fields.append(
            {
                "name": field.name,
                "type": ["null", avro_type],
                "default": None,
            }
        )

    return json.dumps(
        {
            "type": "record",
            "name": schema.message_name,
            "namespace": schema.package,
            "fields": avro_fields,
        },
        separators=(",", ":"),
    )


def _import_schema_registry_components(schema_format: str = "protobuf"):
    schema_format = _normalize_schema_format(schema_format)
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

        if schema_format == "protobuf":
            from confluent_kafka.schema_registry.protobuf import (
                ProtobufDeserializer,
                ProtobufSerializer,
            )

            return SchemaRegistryClient, ProtobufSerializer, ProtobufDeserializer

        from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

        return SchemaRegistryClient, AvroSerializer, AvroDeserializer


def _instantiate_schema_registry_serializer(
    serializer_cls,
    client,
    schema: EntitySchema,
    *,
    schema_format: str,
    message_class=None,
    avro_schema: str | None = None,
    auto_register_schemas: bool,
):
    if schema_format == "protobuf":
        candidates = (
            {
                "schema_registry_client": client,
                "message_type": message_class,
                "conf": {"auto.register.schemas": auto_register_schemas},
            },
            {
                "schema_registry_client": client,
                "message_class": message_class,
                "conf": {"auto.register.schemas": auto_register_schemas},
            },
            {
                "schema_registry_client": client,
                "message_class": message_class,
            },
        )
    else:
        candidates = (
            {
                "schema_registry_client": client,
                "schema_str": avro_schema,
                "to_dict": lambda value, _ctx: value,
                "conf": {"auto.register.schemas": auto_register_schemas},
            },
            {
                "schema_registry_client": client,
                "schema_str": avro_schema,
                "to_dict": lambda value, _ctx: value,
            },
            {
                "schema_registry_client": client,
                "schema_str": avro_schema,
            },
        )

    for candidate in candidates:
        try:
            return serializer_cls(**candidate)
        except TypeError:
            continue

    if schema_format == "protobuf":
        return serializer_cls(message_class, client)
    return serializer_cls(client, avro_schema)


def _instantiate_schema_registry_deserializer(
    deserializer_cls,
    client,
    schema: EntitySchema,
    *,
    schema_format: str,
    message_class=None,
    avro_schema: str | None = None,
):
    if schema_format == "protobuf":
        candidates = (
            {
                "schema_registry_client": client,
                "message_type": message_class,
            },
            {
                "schema_registry_client": client,
                "message_class": message_class,
            },
        )
    else:
        candidates = (
            {
                "schema_registry_client": client,
                "from_dict": lambda value, _ctx: value,
            },
            {
                "schema_registry_client": client,
                "schema_str": avro_schema,
                "from_dict": lambda value, _ctx: value,
            },
        )

    for candidate in candidates:
        try:
            return deserializer_cls(**candidate)
        except TypeError:
            continue

    if schema_format == "protobuf":
        return deserializer_cls(message_class, schema_registry_client=client)
    return deserializer_cls(schema_registry_client=client)


def _normalize_schema_format(schema_format: str) -> str:
    normalized = str(schema_format).strip().lower()
    if normalized not in {"avro", "protobuf"}:
        raise ValueError("schema registry format must be avro or protobuf")
    return normalized


def _ensure_schema_registry_host_bypasses_proxy(schema_registry_url: str) -> None:
    host = urlparse(schema_registry_url).hostname
    if not host:
        return

    for env_name in ("NO_PROXY", "no_proxy"):
        existing = os.environ.get(env_name, "")
        entries = [entry.strip() for entry in existing.split(",") if entry.strip()]
        if host in entries:
            continue
        entries.append(host)
        os.environ[env_name] = ",".join(entries)


def _record_to_dict(message, schema: EntitySchema) -> dict:
    record = {}
    for field in schema.fields:
        value = getattr(message, field.name)
        if field.repeated:
            record[field.name] = list(value)
            continue

        if field.scalar_type == "string":
            record[field.name] = value or ""
        elif field.scalar_type == "int64":
            record[field.name] = int(value)
        elif field.scalar_type == "bool":
            record[field.name] = bool(value)
        else:
            record[field.name] = value
    return record


def protobuf_message_to_dlq_record(message) -> dict:
    record = _record_to_dict(message, DLQ_SCHEMA)

    for field_name in ("payload", "context"):
        raw = record.get(field_name)
        if raw:
            try:
                record[field_name] = json.loads(raw)
            except json.JSONDecodeError:
                record[field_name] = {"raw": raw}
        else:
            record[field_name] = {}

    if record.get("next_retry_at") == 0:
        record["next_retry_at"] = None
    return record


def checkpoint_message_to_record(message) -> dict:
    return _record_to_dict(message, CHECKPOINT_SCHEMA)


def watermark_state_message_to_record(message) -> dict:
    return _record_to_dict(message, WATERMARK_STATE_SCHEMA)


ProtobufSerializerRegistry = SchemaRegistrySerializerRegistry
