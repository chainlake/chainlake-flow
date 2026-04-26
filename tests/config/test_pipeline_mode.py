import pytest
from pydantic import ValidationError

from rpcstream.config.resolver import _resolve_checkpoint_config
from rpcstream.config.schema import ErpcInflight, PipelineConfigModel


def test_backfill_mode_requires_start_not_greater_than_end():
    with pytest.raises(ValidationError):
        PipelineConfigModel(
            name="demo",
            mode="backfill",
            start_block="100",
            end_block="99",
        )


def test_realtime_mode_rejects_end_block():
    with pytest.raises(ValidationError):
        PipelineConfigModel(
            name="demo",
            mode="realtime",
            start_block="latest",
            end_block="100",
        )


def test_realtime_mode_accepts_latest_or_numeric_start():
    latest = PipelineConfigModel(
        name="demo",
        mode="realtime",
        start_block="latest",
    )
    numeric = PipelineConfigModel(
        name="demo",
        mode="realtime",
        start_block="90000000",
    )

    assert latest.start_block == "latest"
    assert numeric.start_block == "90000000"


def test_pipeline_checkpoint_defaults_enabled():
    cfg = PipelineConfigModel(
        name="demo",
        mode="realtime",
        start_block="latest",
    )

    assert cfg.checkpoint.enabled is True


def test_pipeline_checkpoint_can_be_disabled():
    cfg = PipelineConfigModel(
        name="demo",
        mode="realtime",
        start_block="latest",
        checkpoint={"enabled": False},
    )

    assert cfg.checkpoint.enabled is False


def test_resolver_prefers_pipeline_checkpoint_over_legacy_root_checkpoint():
    class Obj:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    cfg = Obj(
        model_fields_set={"checkpoint"},
        checkpoint=Obj(enabled=True),
        pipeline=Obj(
            model_fields_set={"checkpoint"},
            checkpoint=Obj(enabled=False),
        ),
    )

    assert _resolve_checkpoint_config(cfg).enabled is False


def test_resolver_supports_legacy_root_checkpoint_when_pipeline_checkpoint_omitted():
    class Obj:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    cfg = Obj(
        model_fields_set={"checkpoint"},
        checkpoint=Obj(enabled=False),
        pipeline=Obj(
            model_fields_set=set(),
            checkpoint=Obj(enabled=True),
        ),
    )

    assert _resolve_checkpoint_config(cfg).enabled is False


def test_backfill_mode_normalizes_numeric_bounds():
    cfg = PipelineConfigModel(
        name="demo",
        mode="backfill",
        start_block="90000000",
        end_block="90000100",
    )

    assert cfg.start_block == 90000000
    assert cfg.end_block == 90000100


def test_inflight_requires_min_at_least_one():
    with pytest.raises(ValidationError):
        ErpcInflight(
            min_inflight=0,
            max_inflight=5,
            initial_inflight=1,
            latency_target_ms=1000,
        )


def test_inflight_requires_max_not_below_min():
    with pytest.raises(ValidationError):
        ErpcInflight(
            min_inflight=3,
            max_inflight=2,
            initial_inflight=3,
            latency_target_ms=1000,
        )


def test_inflight_requires_initial_within_bounds():
    with pytest.raises(ValidationError):
        ErpcInflight(
            min_inflight=2,
            max_inflight=5,
            initial_inflight=1,
            latency_target_ms=1000,
        )

    with pytest.raises(ValidationError):
        ErpcInflight(
            min_inflight=2,
            max_inflight=5,
            initial_inflight=6,
            latency_target_ms=1000,
        )


def test_inflight_accepts_valid_bounds():
    cfg = ErpcInflight(
        min_inflight=1,
        max_inflight=5,
        initial_inflight=3,
        latency_target_ms=1000,
    )

    assert cfg.min_inflight == 1
    assert cfg.max_inflight == 5
    assert cfg.initial_inflight == 3
