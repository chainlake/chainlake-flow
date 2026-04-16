from abc import ABC, abstractmethod
import asyncio


# =========================
# Base Interface
# =========================
class BlockSource(ABC):
    @abstractmethod
    async def next_block(self):
        """
        Return next block number to process.
        Return None when finished (for bounded sources).
        """
        pass


# =========================
# BACKFILL (bounded)
# =========================
class BackfillBlockSource(BlockSource):
    """
    Deterministic bounded block replay:
    start → end (inclusive)
    """

    def __init__(
        self,
        start: int,
        end: int,
        delay_ms: float = 0
    ):
        self.current = start
        self.end = end
        self.delay = delay_ms / 1000.0

    async def next_block(self):
        if self.current > self.end:
            return None   # <-- IMPORTANT: signals completion

        b = self.current
        self.current += 1

        # optional throttling (useful for testing backpressure)
        if self.delay > 0:
            await asyncio.sleep(self.delay)

        return b
    
# Realtime implementation
class RealtimeBlockSource(BlockSource):
    def __init__(self, tracker):
        self.tracker = tracker
        self.last_emitted = None

    async def next_block(self):
        while True:
            latest = self.tracker.get_latest()

            if latest is None:
                await asyncio.sleep(0.1)
                continue

            if self.last_emitted is None:
                self.last_emitted = latest
                return latest

            if latest > self.last_emitted:
                self.last_emitted += 1
                return self.last_emitted

            await asyncio.sleep(0.05)