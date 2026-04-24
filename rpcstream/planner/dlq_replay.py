from __future__ import annotations

import asyncio

from rpcstream.ingestion.dlq import matches_replay_filter
from rpcstream.planner.block_source import BlockSource


class DlqReplayBlockSource(BlockSource):
    def __init__(
        self,
        dlq_client,
        *,
        entity: str | None = None,
        status: str | None = None,
        stage: str | None = None,
        pipeline: str | None = None,
        chain: str | None = None,
        max_records: int | None = None,
        logger=None,
    ):
        self.dlq_client = dlq_client
        self.entity = entity
        self.status = status
        self.stage = stage
        self.pipeline = pipeline
        self.chain = chain
        self.max_records = max_records
        self.logger = logger
        self._loaded = False
        self._blocks = []
        self._index = 0

    async def next_block(self):
        if not self._loaded:
            await asyncio.to_thread(self._load_blocks)
            self._loaded = True

        if self._index >= len(self._blocks):
            return None

        block_number = self._blocks[self._index]
        self._index += 1
        return block_number

    def _load_blocks(self) -> None:
        latest_records = {}
        idle_polls = 0

        self.dlq_client.subscribe()
        while idle_polls < 3:
            message = self.dlq_client.poll(timeout=1.0)
            if message is None:
                idle_polls += 1
                continue

            idle_polls = 0
            latest_records[message.value["id"]] = message.value

        blocks = []
        seen_blocks = set()
        for record in latest_records.values():
            if not matches_replay_filter(
                record,
                entity=self.entity,
                status=self.status,
                stage=self.stage,
                pipeline=self.pipeline,
                chain=self.chain,
            ):
                continue

            block_number = record.get("block_number")
            if block_number in (None, 0) or block_number in seen_blocks:
                continue
            seen_blocks.add(block_number)
            blocks.append(block_number)

            if self.max_records is not None and len(blocks) >= self.max_records:
                break

        self._blocks = sorted(blocks)
        if self.logger:
            self.logger.info(
                "dlq.replay_blocks_loaded",
                component="dlq",
                block_count=len(self._blocks),
                entity=self.entity,
                status=self.status,
                stage=self.stage,
            )
