from rpcstream.planner.dlq_replay import DlqReplayBlockSource


class FakeDlqClient:
    def read_latest_records(self, *, offset_reset, timeout_sec):
        assert offset_reset == "earliest"
        assert timeout_sec == 30.0
        return (
            {
                "id-1": {
                    "id": "id-1",
                    "entity": "trace",
                    "status": "pending",
                    "stage": "processor",
                    "pipeline": "pipe",
                    "chain": "evm",
                    "block_number": 100,
                },
                "id-2": {
                    "id": "id-2",
                    "entity": "trace",
                    "status": "pending",
                    "stage": "processor",
                    "pipeline": "pipe",
                    "chain": "evm",
                    "block_number": 100,
                },
            },
            2,
        )


def test_dlq_replay_source_tracks_records_per_block():
    source = DlqReplayBlockSource(
        FakeDlqClient(),
        entity="trace",
        status="pending",
        stage="processor",
        pipeline="pipe",
        chain="evm",
        max_records=1,
        logger=None,
    )

    source._load_blocks()

    assert source._blocks == [100]
    records = source.records_for_block(100)
    assert len(records) == 2
    assert {record["id"] for record in records} == {"id-1", "id-2"}
