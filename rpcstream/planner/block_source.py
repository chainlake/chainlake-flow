from abc import ABC, abstractmethod
import asyncio

from opentelemetry import trace

tracer = trace.get_tracer(__name__)

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
        self._span = None

    async def next_block(self):
        # start span only once
        if self._span is None:
            self._span = tracer.start_span("block_source.backfill_range")
            self._span.set_attribute("start", self.current)
            self._span.set_attribute("end", self.end)
            
        if self.current > self.end:
            if self._span:
                self._span.set_attribute("total_blocks", self.end - self.current + 1)
                self._span.end()
            return None   # signals completion

        b = self.current
        self.current += 1

        if self._span:
            self._span.add_event(
                "block_emitted",
                {"block_number": b}
            )

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

            # -------------------------
            # WAIT FOR FIRST HEAD
            # -------------------------
            if latest is None:
                with tracer.start_as_current_span("block_source.wait_for_head") as span:
                    span.set_attribute("source", "realtime")
                    await asyncio.sleep(0.1)
                continue

            # -------------------------
            # FIRST BLOCK
            # -------------------------
            if self.last_emitted is None:
                self.last_emitted = latest
                return latest

            # -------------------------
            # NORMAL PROGRESSION
            # -------------------------
            if latest > self.last_emitted:
                self.last_emitted += 1
                return self.last_emitted

            await asyncio.sleep(0.05) # Prevents CPU Hogging and Infinite Busy Loop