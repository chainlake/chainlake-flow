from rpcstream.ingestion.dlq import (
    FAILED,
    RETRYING,
    build_resolved_record,
    build_retry_record,
    build_unified_dlq_record,
    compute_next_retry_at,
    matches_replay_filter,
    should_retry_record,
)


def test_build_unified_dlq_record_uses_shared_topic_fields_and_partition_key():
    record = build_unified_dlq_record(
        chain="evm",
        network="bsc-mainnet",
        pipeline="bsc_mainnet_ingestion",
        entity="transaction",
        block_number=90000100,
        stage="processor",
        error_type="DecodeError",
        error_message="bad payload",
        payload={"type": "transaction", "hash": "0x1"},
        context={"request": {"rpc": "block"}},
        retry_count=0,
        max_retry=3,
        status="pending",
        first_seen_at=1000,
        last_attempt_at=1000,
        ingest_timestamp=1000,
    )

    assert record["chain"] == "evm"
    assert record["network"] == "bsc-mainnet"
    assert record["pipeline"] == "bsc_mainnet_ingestion"
    assert record["entity"] == "transaction"
    assert record["block_number"] == 90000100
    assert record["payload"]["hash"] == "0x1"
    assert record["context"]["request"]["rpc"] == "block"
    assert record["kafka_partition_key"] == "evm_transaction_90000100"
    assert record["status"] == "pending"
    assert len(record["id"]) == 32


def test_retry_record_uses_exponential_backoff_and_preserves_identity():
    original = build_unified_dlq_record(
        chain="evm",
        network="bsc-mainnet",
        pipeline="bsc_mainnet_ingestion",
        entity="transaction",
        block_number=90000100,
        stage="rpc",
        error_type="TimeoutError",
        error_message="timeout",
        retry_count=0,
        max_retry=3,
        status="pending",
        first_seen_at=1000,
        last_attempt_at=1000,
        next_retry_at=2000,
        ingest_timestamp=1000,
    )

    retrying = build_retry_record(
        original,
        error_type="TimeoutError",
        error_message="timeout again",
        now_ms=5000,
        base_delay_ms=1000,
    )

    assert retrying["id"] == original["id"]
    assert retrying["retry_count"] == 1
    assert retrying["status"] == RETRYING
    assert retrying["next_retry_at"] == 6000
    assert should_retry_record(retrying, now_ms=6000) is True

    failed = build_retry_record(
        retrying,
        error_type="TimeoutError",
        error_message="still failing",
        now_ms=7000,
        base_delay_ms=1000,
    )
    failed = build_retry_record(
        failed,
        error_type="TimeoutError",
        error_message="final failure",
        now_ms=9000,
        base_delay_ms=1000,
    )

    assert failed["status"] == FAILED
    assert failed["next_retry_at"] is None
    assert should_retry_record(failed, now_ms=10000) is False


def test_resolved_record_and_replay_filter_match_latest_state():
    record = build_unified_dlq_record(
        chain="evm",
        network="bsc-mainnet",
        pipeline="bsc_mainnet_ingestion",
        entity="transaction",
        block_number=90000100,
        stage="processor",
        error_type="DecodeError",
        error_message="bad payload",
        retry_count=1,
        max_retry=3,
        status="retrying",
        first_seen_at=1000,
        last_attempt_at=2000,
        ingest_timestamp=2000,
    )

    resolved = build_resolved_record(record, now_ms=3000)
    assert resolved["id"] == record["id"]
    assert resolved["status"] == "resolved"
    assert matches_replay_filter(
        resolved,
        entity="transaction",
        status="resolved",
        stage="processor",
        pipeline="bsc_mainnet_ingestion",
        chain="evm",
    )
    assert compute_next_retry_at(retry_count=3, base_delay_ms=1000, now_ms=0) == 4000
