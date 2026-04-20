import uuid
import aiohttp
import orjson

from rpcstream.client.base import BaseClient


class JsonRpcClient(BaseClient):
    """
    JSON-RPC transport light client.
    """

    def __init__(
        self,
        base_url: str,
        timeout_sec: int = 10,
        pool_limit: int = 200,
        dns_ttl_sec: int = 300,
        max_retries: int = 0,
        logger=None,
    ):
        super().__init__(base_url, max_retries=max_retries, logger=logger)

        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        connector = aiohttp.TCPConnector(
            limit=pool_limit,
            ttl_dns_cache=dns_ttl_sec,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )

        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            json_serialize=lambda obj: orjson.dumps(obj).decode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
        )

    async def _execute(self, request, span):
        payload = {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": request.method,
            "params": request.params,
        }

        span.set_attribute("rpc.method", request.method)

        if self.logger:
            self.logger.debug(
                "client.http_send",
                component="client",
                method=request.method,
                payload_preview=str(payload)[:200]
            )

        async with self.session.post(self.base_url, json=payload) as resp:
            resp.raise_for_status()
            raw = await resp.read()
            data = orjson.loads(raw)

        if "error" in data:
            span.set_attribute("rpc.status", "error")
            span.set_attribute("rpc.error", str(data["error"]))
            
            if self.logger:
                self.logger.error(
                    "client.rpc_response_error",
                    component="client",
                    method=request.method,
                    error=str(data["error"])
                )
            
            raise RuntimeError(data["error"])

        if self.logger:
            self.logger.debug(
                "client.rpc_response",
                component="client",
                method=request.method,
                response_preview=str(data)[:200]
            )

        return data["result"]

    async def close(self):
        await self.session.close()