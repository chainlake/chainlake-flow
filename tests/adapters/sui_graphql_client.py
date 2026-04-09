import aiohttp
import asyncio
import orjson


class GraphQLClient:
    def __init__(self, base_url: str, token: str = None, timeout: int = 10):
        self.base_url = base_url
        self.headers = {
            "Content-Type": "application/json",
        }
        if token:
            self.headers["x-token"] = token
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session = aiohttp.ClientSession(timeout=self.timeout)

    async def execute(self, query: str, variables: dict = None):
        payload = {
            "query": query,
            "variables": variables or {}
        }
        async with self.session.post(self.base_url, json=payload, headers=self.headers) as resp:
            resp.raise_for_status()
            raw = await resp.read()
            return orjson.loads(raw)

    async def close(self):
        await self.session.close()