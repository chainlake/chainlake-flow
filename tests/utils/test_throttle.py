import time

from rpcstream.utils.logger import JsonLogger
from rpcstream.utils.throttle import ThrottledLogger


def test_throttled_summary_does_not_crash(capsys):
    logger = ThrottledLogger(JsonLogger(level="DEBUG"), max_per_sec=5.0, summary_interval_sec=0.01)

    for _ in range(10):
        logger.debug("some.repeated.message", foo="bar")

    time.sleep(0.02)
    # Before the fix, this raised:
    #   TypeError: JsonLogger._log() got multiple values for argument 'message'
    # because the summary line passed message=/level= kwargs that collide
    # with _log's own positional params of the same name.
    logger.debug("some.repeated.message", foo="bar")

    output = capsys.readouterr().err
    assert "log.throttled_summary" in output
