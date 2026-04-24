import pytest
from pydantic import ValidationError

from rpcstream.config.schema import PipelineConfigModel


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


def test_backfill_mode_normalizes_numeric_bounds():
    cfg = PipelineConfigModel(
        name="demo",
        mode="backfill",
        start_block="90000000",
        end_block="90000100",
    )

    assert cfg.start_block == 90000000
    assert cfg.end_block == 90000100
