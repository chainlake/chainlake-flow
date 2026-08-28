import asyncio
import time

from rpcstream.client.base import BaseClient
from rpcstream.client.models import (
    RpcTaskMeta,
    RpcErrorResult,
    exception_log_fields,
    is_expected_rpc_warning,
    summarize_exception,
)
from rpcstream.protocol.request import BaseRpcRequest  # Generic RPC request
from rpcstream.scheduler.base import BaseScheduler
from rpcstream.runtime.observability.context import ObservabilityContext
from rpcstream.metrics.scheduler import SchedulerMetrics


class AdaptiveRpcScheduler(BaseScheduler):
    def __init__(
        self,
        client: BaseClient,
        logger=None,
        observability: ObservabilityContext | None = None,
        circuit_breaker_enabled=True,
        trip_consecutive_failures=5,
        trip_failure_rate=0.5,
        backoff_base_sec=1.0,
        backoff_max_sec=30.0,
        probe_budget=3,
        **kwargs,
    ):
        super().__init__(
            circuit_breaker_enabled=circuit_breaker_enabled,
            trip_consecutive_failures=trip_consecutive_failures,
            trip_failure_rate=trip_failure_rate,
            backoff_base_sec=backoff_base_sec,
            backoff_max_sec=backoff_max_sec,
            probe_budget=probe_budget,
            **kwargs,
        )
        self.client = client
        self.logger = logger
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)
        self.metrics = SchedulerMetrics(self.observability.get_meter("rpcstream.scheduler"))
        self.metrics.bind(self)
        
    # ----------------------------
    # Generic submit method for BaseRpcRequest
    # ----------------------------
    async def submit_request(self, request: BaseRpcRequest):
        """
        Submit a generic RPC request.
        request: BaseRpcRequest instance
        Returns (result, RpcTaskMeta) or RpcErrorResult
        """
        enqueue_ts = time.time()

        with self._tracer.start_as_current_span("scheduler.submit_request") as span:
            span.set_attribute("scheduler.method", request.operation_name())

            if self.logger:  # DEBUG
                self.logger.debug(
                    "scheduler.enqueue",
                    method=request.operation_name(),
                    inflight=self.inflight,
                    window=self.current_limit,
                )

            await self._acquire_slot()

            wait_ms = (time.time() - enqueue_ts) * 1000
            self._update_queue_wait(wait_ms)

            if self.logger:
                self.logger.debug(
                    "scheduler.slot_acquired",
                    method=request.operation_name(),
                    queue_wait_ms=round(wait_ms, 2),
                    inflight=self.inflight,
                    window=self.current_limit,
                )

            submit_ts = time.time()

            meta = RpcTaskMeta(
                task_id=id(asyncio.current_task()),
                submit_ts=submit_ts,
                extra=request.meta.copy(),
            )

            meta.extra["queue_wait_ms"] = round(wait_ms, 2)
            meta.extra["inflight"] = self.inflight

            span.set_attribute("scheduler.queue_wait_ms", round(wait_ms, 2))
            span.set_attribute("scheduler.window", self.current_limit)

            try:
                # The client only needs the request
                result = await self.client.execute(request)

                latency = (time.time() - submit_ts) * 1000

                self.success += 1
                self._update_latency(latency)
                self._adjust_window(True)
                self._record_outcome(True)

                meta.extra["latency_ms"] = round(latency, 2)

                span.set_attribute("scheduler.status", "ok")
                span.set_attribute("scheduler.latency_ms", round(latency, 2))
                
                if self.logger:
                    self.logger.debug(
                        "scheduler.request_success",
                        method=request.operation_name(),
                        latency_ms=round(latency, 2),
                        inflight=self.inflight,
                        window=self.current_limit,
                    )

                return result, meta

            except Exception as exc:
                latency = (time.time() - submit_ts) * 1000

                self.errors += 1
                self._update_latency(latency)
                expected_warning = is_expected_rpc_warning(exc)
                if not expected_warning:
                    self._adjust_window(False)
                    self._record_outcome(False)

                error_msg = summarize_exception(exc)
                error_fields = exception_log_fields(exc)

                span.set_attribute("scheduler.status", "error")
                span.set_attribute("scheduler.exception", error_msg)
                span.set_attribute("scheduler.latency_ms", round(latency, 2))

                if self.logger:
                    log_method = self.logger.warn if expected_warning else self.logger.error
                    log_method(
                        "scheduler.request_failed",
                        method=request.operation_name(),
                        inflight=self.inflight,
                        window=self.current_limit,
                        **error_fields,
                    )

                return RpcErrorResult(
                    error=error_msg,
                    meta=meta,
                    details=error_fields,
                    expected=expected_warning,
                )

            finally:
                self._release_slot()
                
                if self.logger:
                    self.logger.debug(
                        "scheduler.slot_released",
                        inflight=self.inflight,
                        window=self.current_limit,
                    )

    def _adjust_window(self, success):
        prev = self.current_limit
        cur = self.current_limit

        increase_step = 1
        mild_decrease_factor = 0.95
        strong_decrease_factor = 0.85

        # Adaptive target: derived from the learned provider latency floor, so
        # it is correct for any chain/provider without manual tuning. An optional
        # absolute latency_target_ms (if set) acts as an additional hard floor.
        target = self.effective_target_ms()
        # Queue-wait budget: the REAL congestion signal. A single heavy request's
        # rpc_latency being high is NOT congestion (e.g. receipt/log payloads on
        # BSC are intrinsically ~1s even when the upstream is healthy).
        queue_target = self.effective_queue_target_ms()
        queue_wait = self.queue_wait_ema or 0.0

        # ---- Decide the raw signal for THIS window ----
        # Congestion is driven primarily by queue wait, not by a single request's
        # latency. But a sustained breach of the adaptive latency *target* (not a
        # 3x spike) is also real saturation — the target already incorporates a
        # 3x multiplier over the learned floor, so "latency > target" means the
        # upstream is roughly 3x slower than its healthy baseline. We keep this as
        # a secondary signal (queue wait is primary) and debounce both so a single
        # heavy-but-healthy request (BSC receipt/log ~1s) that merely sits near the
        # target does NOT collapse the window.
        # raw_congested: requests are piling up waiting for an admission slot.
        # latency_over: observed latency has sustained above the adaptive target
        #   (i.e. upstream materially slower than its healthy floor).
        # raw_headroom: queue is (near) empty AND latency is comfortably under
        #   target -> we can safely grow.
        raw_congested = queue_wait > queue_target
        latency_over = (self.latency_ema or 0.0) > target
        raw_headroom = (not raw_congested) and (not latency_over) and (self.latency_ema or 0.0) <= target

        # ---- Debounce SHRINK only ----
        # A single heavy-request latency spike (or one transient blip) must not
        # collapse the window, so congestion (queue wait OR 3x latency) must
        # persist for `adjust_cooldown_windows` consecutive windows before we
        # shrink. GROWTH is deliberately NOT debounced: when headroom is present
        # it is safe to scale up every window, which lets us recover far faster
        # than we shrink (recovery must outpace a transient saturation to keep
        # ingestion_lag bounded).
        if raw_congested or latency_over:
            self._congested_windows += 1
        else:
            self._congested_windows = 0

        congested = self._congested_windows >= self.adjust_cooldown_windows

        # ---- Apply ----
        if not success:
            self._set_current_limit(
                max(
                    self.min_inflight,
                    int(cur * strong_decrease_factor),
                )
            )
            reason = "error"
        elif congested and raw_congested:
            # Upstream saturated: requests waiting too long for a slot. Shrink.
            self._set_current_limit(
                max(
                    self.min_inflight,
                    max(cur - 1, int(cur * mild_decrease_factor)),
                )
            )
            reason = "queue_congested"
        elif congested and latency_over:
            # Sustained latency breach of the adaptive target (upstream materially
            # slower than its healthy floor). Strong shrink.
            self._set_current_limit(
                max(
                    self.min_inflight,
                    int(cur * strong_decrease_factor),
                )
            )
            reason = "high_latency_strong"
        elif raw_headroom:
            # Healthy with headroom: grow back quickly (recover faster than we
            # shrink). Step scales with current size so we don't crawl back.
            self._set_current_limit(
                min(
                    self.max_inflight,
                    cur + max(increase_step, int(cur * 0.1)),
                )
            )
            reason = "increase"
        else:
            reason = "stable"
            # no change this window

        # log only when changed
        if self.logger and self.current_limit != prev:
            self.logger.debug(
                "scheduler.window_adjusted",
                prev_window=prev,
                new_window=self.current_limit,
                reason=reason,
                latency_ema_ms=round(self.latency_ema or 0, 2),
                latency_floor_ms=round(self.latency_floor or 0, 2),
                effective_target_ms=round(target, 2),
                queue_wait_ema_ms=round(queue_wait, 2),
                queue_target_ms=round(queue_target, 2),
            )
