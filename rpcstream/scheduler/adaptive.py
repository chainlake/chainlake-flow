import asyncio
import time

from rpcstream.scheduler.base import BaseRpcScheduler
from rpcstream.rpc.models import RpcTaskMeta, RpcErrorResult

from opentelemetry import trace

tracer = trace.get_tracer("rpcstream.scheduler")


class AdaptiveRpcScheduler(BaseRpcScheduler):
    def __init__(self, client, **kwargs):
        super().__init__(**kwargs)
        self.client = client

    async def submit(self, method, params, meta_extra):
        enqueue_ts = time.time()

        with tracer.start_as_current_span("scheduler.submit") as span:
            span.set_attribute("rpc.method", method)

            await self._acquire_slot()

            wait_ms = (time.time() - enqueue_ts) * 1000
            self._update_queue_wait(wait_ms)

            submit_ts = time.time()

            meta = RpcTaskMeta(
                task_id=id(asyncio.current_task()),
                submit_ts=submit_ts,
                extra=meta_extra,
            )

            meta.extra["queue_wait_ms"] = wait_ms

            span.set_attribute("scheduler.queue_wait_ms", round(wait_ms, 2))
            span.set_attribute("scheduler.window", self.current_limit)

            try:
                result = await self.client.call(method, params)

                latency = (time.time() - submit_ts) * 1000

                self.success += 1
                self._update_latency(latency)
                self._adjust_window(True)

                meta.extra["latency_ms"] = round(latency, 2)

                span.set_attribute("scheduler.status", "ok")
                span.set_attribute("scheduler.latency_ms", round(latency, 2))

                return result, meta

            except Exception as exc:
                latency = (time.time() - submit_ts) * 1000

                self.errors += 1
                self._update_latency(latency)
                self._adjust_window(False)

                span.set_attribute("scheduler.status", "error")
                span.set_attribute("scheduler.exception", str(exc))
                span.set_attribute("scheduler.latency_ms", round(latency, 2))

                return RpcErrorResult(exc, meta)

            finally:
                self._release_slot()

    def _adjust_window(self, success):
        cur = self.current_limit

        increase_step = 1
        mild_decrease_factor = 0.95
        strong_decrease_factor = 0.85

        if not success:
            self.current_limit = max(
                self.min_inflight,
                int(cur * strong_decrease_factor),
            )
            return

        latency = self.latency_ema or self.latency_target_ms

        if latency > self.latency_target_ms * 3:
            self.current_limit = max(
                self.min_inflight,
                int(cur * strong_decrease_factor),
            )

        elif latency > self.latency_target_ms:
            self.current_limit = max(
                self.min_inflight,
                int(cur * mild_decrease_factor),
            )

        else:
            self.current_limit = min(
                self.max_inflight,
                cur + increase_step,
            )