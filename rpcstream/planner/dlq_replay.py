from __future__ import annotations

import asyncio

from rpcstream.ingestion.dlq import matches_replay_filter
from rpcstream.planner.cursor_source import CursorSource


class DlqReplayCursorSource(CursorSource):
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
        self._cursors = []
        self._index = 0
        self._records_by_cursor = {}

    async def next_cursor(self):
        if not self._loaded:
            await asyncio.to_thread(self._load_cursors)
            self._loaded = True

        if self._index >= len(self._cursors):
            return None

        cursor = self._cursors[self._index]
        self._index += 1
        return cursor

    def _load_cursors(self) -> None:
        scanned = 0
        matched = 0
        latest_records = {}

        if hasattr(self.dlq_client, "read_latest_records"):
            latest_records, scanned = self.dlq_client.read_latest_records(
                offset_reset="earliest",
                timeout_sec=30.0,
            )
        else:
            idle_polls = 0
            self.dlq_client.subscribe()
            if hasattr(self.dlq_client, "wait_until_ready"):
                self.dlq_client.wait_until_ready(timeout_sec=10.0)
            while idle_polls < 3:
                message = self.dlq_client.poll(timeout=1.0)
                if message is None:
                    idle_polls += 1
                    continue

                idle_polls = 0
                scanned += 1
                latest_records[message.value["id"]] = message.value

        cursors = []
        seen_cursors = set()
        records_by_cursor = {}
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
            matched += 1

            cursor = record.get("cursor")
            if cursor in (None, 0):
                continue

            if cursor in seen_cursors:
                records_by_cursor.setdefault(cursor, []).append(record)
                continue

            if self.max_records is not None and len(cursors) >= self.max_records:
                continue

            records_by_cursor.setdefault(cursor, []).append(record)
            seen_cursors.add(cursor)
            cursors.append(cursor)

        self._cursors = sorted(cursors)
        self._records_by_cursor = records_by_cursor
        if self.logger:
            self.logger.info(
                "dlq.replay_cursors_loaded",
                component="dlq",
                cursor_count=len(self._cursors),
                scanned_records=scanned,
                latest_record_count=len(latest_records),
                matched_records=matched,
                entity=self.entity,
                status=self.status,
                stage=self.stage,
            )

    def records_for_cursor(self, cursor: int) -> list[dict]:
        return list(self._records_by_cursor.get(cursor, []))
