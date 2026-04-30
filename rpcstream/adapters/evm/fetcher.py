from __future__ import annotations

import asyncio

from rpcstream.adapters.evm.rpc_requests import build_debug_trace_block
from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number
from rpcstream.adapters.evm.rpc_requests import build_get_block_receipts


class EvmRpcFetcher:
    def __init__(self, scheduler, entities, logger=None, tracker=None):
        self.scheduler = scheduler
        self.entities = entities
        self.logger = logger
        self.tracker = tracker

    async def fetch(self, cursor):
        cursor = int(cursor)
        if self.logger:
            self.logger.debug(
                "fetcher.request",
                component="fetcher",
                entities=self.entities,
                block=cursor,
                cursor=cursor,
            )

        requests = []

        if "transaction" in self.entities:
            request_entities = ["transaction"]
            if "block" in self.entities:
                request_entities.append("block")
            requests.append(
                (
                    tuple(request_entities),
                    build_get_block_by_number(cursor, True),
                )
            )
        elif "block" in self.entities:
            requests.append(
                (
                    ("block",),
                    build_get_block_by_number(cursor, False),
                )
            )

        if "receipt" in self.entities or "log" in self.entities:
            request_entities = ["receipt"]
            if "log" in self.entities:
                request_entities.append("log")
            requests.append(
                (
                    tuple(request_entities),
                    build_get_block_receipts(cursor),
                )
            )

        if "trace" in self.entities:
            requests.append(
                (
                    ("trace",),
                    build_debug_trace_block(cursor),
                )
            )

        results = await asyncio.gather(
            *(self.scheduler.submit_request(req) for _, req in requests)
        )

        raw_data = {}
        req_method = {}
        for (entities, req), result in zip(requests, results):
            for entity in entities:
                raw_data[entity] = result
                req_method[entity] = req.method

        if self.logger:
            for entity in raw_data:
                self.logger.debug(
                    "fetcher.response",
                    component="fetcher",
                    method=req_method[entity],
                    block=cursor,
                    cursor=cursor,
                    entity=entity,
                )

        return raw_data
