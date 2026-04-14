from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number

class RpcFetcher:
    def __init__(self, scheduler):
        self.scheduler = scheduler

    async def get_block(self, block_number):
        req = build_get_block_by_number(block_number)
        return await self.scheduler.submit_request(req)