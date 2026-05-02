import warnings

import pytest

from rpcstream.adapters.evm.schema import EVM_ENTITY_SCHEMAS
from rpcstream.sinks.kafka.protobuf import (
    ProtobufSerializerRegistry,
    _import_schema_registry_components,
    build_message_class,
    normalize_scalar,
)


def test_evm_entity_schemas_cover_all_entities():
    assert set(EVM_ENTITY_SCHEMAS) == {
        "block",
        "transaction",
        "log",
        "token_transfer",
        "trace",
    }


def test_protobuf_message_class_contains_trace_address_field():
    pytest.importorskip("google.protobuf")
    message_class = build_message_class(EVM_ENTITY_SCHEMAS["trace"])
    field_names = {field.name for field in message_class.DESCRIPTOR.fields}

    assert "trace_address" in field_names
    assert "ingest_timestamp" in field_names


def test_normalize_scalar_serializes_complex_values_as_json_strings():
    assert normalize_scalar("string", {"hello": "world"}) == '{"hello":"world"}'


def test_schema_registry_conf_uses_basic_auth_user_info_only():
    registry = ProtobufSerializerRegistry(
        schema_registry_url="https://registry.example.com",
        producer_config={
            "sasl.username": "user",
            "sasl.password": "pass",
        },
        topic_schemas={},
    )

    assert registry._schema_registry_conf() == {
        "url": "https://registry.example.com",
        "basic.auth.user.info": "user:pass",
    }


def test_schema_registry_import_suppresses_authlib_deprecation_warning():
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        _import_schema_registry_components()

    assert not [
        warning for warning in recorded
        if warning.category.__name__ == "AuthlibDeprecationWarning"
    ]
