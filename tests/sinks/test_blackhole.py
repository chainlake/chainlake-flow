import asyncio

from rpcstream.sinks.blackhole import BlackholeSink


def test_blackhole_sink_counts_rows_and_resolves_delivery_future():
    async def run():
        sink = BlackholeSink()
        future = await sink.send("topic-a", [{"id": "1"}, {"id": "2"}], wait_delivery=True)
        await sink.send_transaction(
            [
                ("topic-a", [{"id": "3"}]),
                ("topic-b", [{"id": "4"}, {"id": "5"}]),
            ]
        )
        return sink, future

    sink, future = asyncio.run(run())

    assert future.done() is True
    assert future.result() is True
    assert sink.message_count == 5
    assert sink.batch_count == 1
    assert sink.transaction_count == 1
    assert sink.topic_counts["topic-a"] == 3
    assert sink.topic_counts["topic-b"] == 2
