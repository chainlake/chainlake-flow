from __future__ import annotations

import asyncio

from rpcstream.adapters.evm.rpc_requests import build_debug_trace_block
from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number
from rpcstream.adapters.evm.rpc_requests import build_get_block_receipts
from rpcstream.client.models import RpcErrorResult


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
            if "block" not in self.entities:
                requests.append(
                    (
                        ("block",),
                        build_get_block_by_number(cursor, False),
                    )
                )
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
                    method=req_method[entity],
                    block=cursor,
                    cursor=cursor,
                    entity=entity,
                )

        return raw_data


class RawEnvelopeFetcher:
    """Canonical-fetch mode: always fetches full block + receipts in parallel.

    Returns {"raw_envelope": ((block_json, receipts_json), combined_meta)}.
    Both RPC calls use the shared scheduler so concurrency/circuit-breaker
    semantics apply identically to any other entity shard.
    """

    def __init__(self, scheduler, logger=None, tracker=None):
        self.scheduler = scheduler
        self.logger = logger
        self.tracker = tracker

    async def fetch(self, cursor):
        cursor = int(cursor)
        if self.logger:
            self.logger.debug(
                "fetcher.request",
                entities=["raw_envelope"],
                block=cursor,
                cursor=cursor,
            )

        block_result, receipts_result = await asyncio.gather(
            self.scheduler.submit_request(build_get_block_by_number(cursor, True)),
            self.scheduler.submit_request(build_get_block_receipts(cursor)),
        )

        # Surface the first error so the engine can route it to DLQ / retry.
        if isinstance(block_result, RpcErrorResult):
            return {"raw_envelope": block_result}
        if isinstance(receipts_result, RpcErrorResult):
            return {"raw_envelope": receipts_result}

        block_value, block_meta = block_result
        receipts_value, receipts_meta = receipts_result

        # Parallel calls: wall-clock latency is max of the two. Merge into
        # block_meta so the engine's phase timing is accurate.
        block_meta.extra["latency_ms"] = max(
            block_meta.extra.get("latency_ms", 0),
            receipts_meta.extra.get("latency_ms", 0),
        )
        block_meta.extra["queue_wait_ms"] = max(
            block_meta.extra.get("queue_wait_ms", 0),
            receipts_meta.extra.get("queue_wait_ms", 0),
        )
        block_meta.extra["rpc_calls"] = 2

        if self.logger:
            self.logger.debug(
                "fetcher.response",
                method="eth_getBlockByNumber+eth_getBlockReceipts",
                block=cursor,
                cursor=cursor,
                entity="raw_envelope",
            )

        return {"raw_envelope": ((block_value, receipts_value), block_meta)}
