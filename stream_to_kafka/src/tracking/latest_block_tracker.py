import asyncio
from src.logging import log
import aiohttp
import json
import time
import asyncio

# 外部链状态
# 弱一致
# 不应和 ingestion 强绑定
class LatestBlockTracker:
    def __init__(
        self,
        router,
        refresh_interval: float = 2.0,
        max_reorg_tolerance: int = 5,
        max_forward_gap: int = 100,
    ):
        self.router = router
        self.refresh_interval = refresh_interval
        self.max_reorg_tolerance = max_reorg_tolerance
        self.max_forward_gap = max_forward_gap

        self._latest: int | None = None
        self._last_raw: int | None = None

        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._stopped = False

    def get_cached(self) -> int | None:
        """
        ✅ 永不 await
        ✅ 单调（在容忍 reorg 语义下）
        """
        return self._latest

    async def _refresh_loop(self):
        while not self._stopped:
            try:
                candidate = await self.router.get_latest_block()
                async with self._lock:
                    self._apply_candidate(candidate)
            except Exception as e:
                log.warning(
                    "⚠️ latest_block_refresh_failed",
                    extra={"error": str(e)},
                )

            await asyncio.sleep(self.refresh_interval)

    def _apply_candidate(self, candidate: int):
        self._last_raw = candidate

        # ---------- first value ----------
        if self._latest is None:
            self._latest = candidate
            log.info(
                "📌 latest_block_initialized",
                extra={"latest": candidate},
            )
            return

        current = self._latest

        # ---------- equal ----------
        if candidate == current:
            return

        # ---------- backward (reorg or dirty RPC) ----------
        if candidate < current:
            delta = current - candidate

            if delta <= self.max_reorg_tolerance:
                # small reorg, accept
                self._latest = candidate
                log.warning(
                    "🔄 chain_reorg_detected",
                    extra={
                        "from": current,
                        "to": candidate,
                        "delta": delta,
                    },
                )
            else:
                # dirty RPC, reject
                log.warning(
                    "🛑 abnormal_chain_head_backward",
                    extra={
                        "current": current,
                        "candidate": candidate,
                        "delta": delta,
                    },
                )
            return

        # ---------- forward ----------
        forward_gap = candidate - current

        if forward_gap > self.max_forward_gap:
            capped = current + self.max_forward_gap
            self._latest = capped
            log.warning(
                "⚠️ abnormal_chain_head_forward_jump",
                extra={
                    "current": current,
                    "candidate": candidate,
                    "gap": forward_gap,
                    "capped_to": capped,
                },
            )
            return

        # ---------- normal forward ----------
        self._latest = candidate

    def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._refresh_loop())

    async def stop(self):
        self._stopped = True
        if self._task:
            await self._task




# webSocket version of latestBlockTracker
class WsLatestBlockTracker:
    def __init__(self, rpc_pool):
        self.rpc_pool = rpc_pool
        self.latest_block = None
        self.last_update_ts = 0
        self._task = None
        self._stop = asyncio.Event()

    def start(self):
        self._task = asyncio.create_task(self._run())

    def get_cached(self):
        return self.latest_block

    async def _run(self):
        while not self._stop.is_set():

            ws_pool = RpcPool(
                providers=[
                    p for p in self.rpc_pool.all_providers()
                    if p.ws_url
                ]
            )
            ws_provider = ws_pool.pick_ws_provider()

            if not ws_provider:
                raise RuntimeError("No WS provider available")

            ws_url, key_env = await ws_provider.acquire_ws_url()

            log.info(
                "🔌 ws_connect",
                extra={"rpc": ws_provider.name,
                      "key_env": key_env,
                      },
            )

            try:
                await self._consume(ws_url, ws_provider)
            except Exception as e:
                log.warning(
                    "⚠️ ws_failed",
                    extra={
                        "rpc": ws_provider.name,
                        "error": str(e),
                    },
                )

            # self.rpc_pool.mark_unhealthy(ws_provider.name)
            await asyncio.sleep(1)  # backoff

    async def _consume(self, ws_url, provider):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url, heartbeat=30) as ws:
                await ws.send_json({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["newHeads"],
                })

                async for msg in ws:
                    data = json.loads(msg.data)
                    if data.get("method") != "eth_subscription":
                        continue

                    bn = data["params"]["result"].get("number")
                    if bn:
                        self.latest_block = int(bn, 16)
                        self.last_update_ts = time.time()

                    # 🚨 stale detection
                    if time.time() - self.last_update_ts > 15:
                        raise RuntimeError("WS stale, force reconnect")