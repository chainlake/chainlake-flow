import asyncio

from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number
from rpcstream.adapters.evm.rpc_requests import build_get_block_receipts
from rpcstream.adapters.evm.rpc_requests import build_debug_trace_block

class EvmRpcFetcher:
    def __init__(self, scheduler, entities, logger=None, tracker=None):
        self.scheduler = scheduler
        self.entities = entities  # List of entities to fetch, e.g., ["block", "transaction"]
        self.logger = logger
        self.tracker = tracker

    async def fetch(self, block_number):
        # -------------------------
        # LOG BEFORE
        # -------------------------
        if self.logger:
            self.logger.debug(
                "fetcher.request",
                component="fetcher",
                entities=self.entities,
                block=block_number,
            )
        
        requests = []

        if "block" in self.entities and "transaction" not in self.entities:
            requests.append((
                ("block",),
                build_get_block_by_number(block_number, False),
            ))

        if "transaction" in self.entities:
            entities = ["transaction"]
            if "block" in self.entities:
                entities.append("block")
            requests.append((
                tuple(entities),
                build_get_block_by_number(block_number, True),
            ))

        if "receipt" in self.entities or "log" in self.entities:
            entities = ["receipt"]
            if "log" in self.entities:
                entities.append("log")
            requests.append((
                tuple(entities),
                build_get_block_receipts(block_number),
            ))
    
        if "trace" in self.entities:
            requests.append((
                ("trace",),
                build_debug_trace_block(block_number),
            ))

        results = await asyncio.gather(
            *(self.scheduler.submit_request(req) for _, req in requests)
        )

        raw_data = {}
        req_method = {}
        for (entities, req), result in zip(requests, results):
            for entity in entities:
                raw_data[entity] = result
                req_method[entity] = req.method

        # Log after fetch
        if self.logger:
            for entity in raw_data:
                self.logger.debug(
                    "fetcher.response",
                    component="fetcher",
                    method=req_method[entity],
                    block=block_number,
                    entity=entity
                )
        
        return raw_data
