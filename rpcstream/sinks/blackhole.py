from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any


class BlackholeSink:
    def __init__(self, logger=None):
        self.logger = logger
        self.message_count = 0
        self.batch_count = 0
        self.transaction_count = 0
        self.topic_counts: dict[str, int] = defaultdict(int)

    async def start(self):
        return None

    async def close(self):
        return None

    async def send(self, topic: str, rows: list[dict[str, Any]], wait_delivery: bool = False):
        self.batch_count += 1
        self.message_count += len(rows)
        self.topic_counts[topic] += len(rows)

        if wait_delivery:
            future = asyncio.get_running_loop().create_future()
            future.set_result(True)
            return future
        return None

    async def send_transaction(self, topic_rows):
        self.transaction_count += 1
        for topic, rows in topic_rows:
            self.message_count += len(rows)
            self.topic_counts[topic] += len(rows)
        return None
