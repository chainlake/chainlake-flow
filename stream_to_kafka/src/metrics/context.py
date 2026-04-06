# src/metrics/context.py
import os
from dataclasses import dataclass
from . import definitions as m

@dataclass(frozen=True)
class MetricsContext:
    chain: str
    job: str

    # -------- Lag --------
    checkpoint_lag: any
    checkpoint_lag_raw: any
    chain_latest_block: any
    checkpoint_block: any

    # -------- Throughput --------
    tx_processed: any
    block_processed: any
    tx_per_block: any
    tx_committed: any
    block_committed: any

    # -------- Kafka --------
    kafka_tx_failure: any
    commit_interval: any
    commit_interval_latest: any

    # -------- RPC（无二次 labels）--------
    rpc_submitted: any
    rpc_finished: any
    rpc_started: any
    rpc_queue_wait: any

    # provider / key 动态的保留原 metric
    rpc_completed: any
    
    rpc_failed: any
    rpc_latency: any
    rpc_key_wait: any

    # range registry
    rpc_queue_size: any
    rpc_inflight: any
    range_inflight: any
    max_range_inflight: any
    
    # ===== RPC helpers =====
    def rpc_submitted_inc(self):
        self.rpc_submitted.inc()

    def rpc_finished_inc(self):
        self.rpc_finished.inc()

    def rpc_started_inc(self):
        self.rpc_started.inc()

    def rpc_inflight_inc(self):
        self.rpc_inflight.inc()

    def rpc_inflight_dec(self):
        self.rpc_inflight.dec()

    def rpc_failed_inc(self, provider: str, key: str):
        self.rpc_failed.labels(
            chain=self.chain,
            job=self.job,
            provider=provider,
            key=key,
        ).inc()

    def rpc_completed_inc(self, provider: str, key: str):
        self.rpc_completed.labels(
            chain=self.chain,
            job=self.job,
            provider=provider,
            key=key,
        ).inc()

    def rpc_latency_observe(self, provider: str, value_ms: float):
        self.rpc_latency.labels(
            chain=self.chain,
            job=self.job,
            provider=provider,
        ).observe(value_ms)

    def rpc_queue_wait_observe(self, value_ms: float):
        self.rpc_queue_wait.observe(value_ms)

    def rpc_queue_size_set(self, value_num: float):
        self.rpc_queue_size.set(value_num)

    def rpc_key_wait_inc(self, key: str):
        self.rpc_key_wait.labels(
            chain=self.chain,
            job=self.job,
            key=key,
        ).inc()
        
    # range registry
    def range_inflight_set(self, value_num: float):
        self.range_inflight.set(value_num)

    def max_range_inflight_set(self, value_num: float):
        self.max_range_inflight.set(value_num)


    @classmethod
    def from_env(cls) -> "MetricsContext":
        chain = os.getenv("CHAIN", "unknown")
        job = os.getenv("JOB_NAME", "unknown")

        base = dict(chain=chain, job=job)

        return cls(
            chain=chain,
            job=job,

            # Lag
            checkpoint_lag=m.CHECKPOINT_LAG.labels(**base),
            checkpoint_lag_raw=m.CHECKPOINT_LAG_RAW.labels(**base),
            chain_latest_block=m.CHAIN_LATEST_BLOCK.labels(**base),
            checkpoint_block=m.CHECKPOINT_BLOCK.labels(**base),

            # Throughput
            tx_processed=m.TX_PROCESSED.labels(**base),
            block_processed=m.BLOCK_PROCESSED.labels(**base),
            tx_per_block=m.TX_PER_BLOCK.labels(**base),
            tx_committed=m.TX_COMMITTED.labels(**base),
            block_committed=m.BLOCK_COMMITTED.labels(**base),
            
            # Kafka
            kafka_tx_failure=m.KAFKA_TX_FAILURE.labels(**base),
            commit_interval=m.COMMIT_INTERVAL.labels(**base),
            commit_interval_latest=m.COMMIT_INTERVAL_LATEST.labels(**base),

            # RPC（固定 labels）
            rpc_submitted=m.RPC_SUBMITTED.labels(**base),
            rpc_finished=m.RPC_FINISHED.labels(**base),
            rpc_started=m.RPC_STARTED.labels(**base),
            rpc_queue_wait=m.RPC_QUEUE_WAIT.labels(**base),
            rpc_queue_size=m.RPC_QUEUE_SIZE.labels(**base),
            rpc_inflight=m.RPC_INFLIGHT.labels(**base),

            # RPC（动态 labels）
            rpc_completed=m.RPC_COMPLETED,
            rpc_failed=m.RPC_FAILED,
            rpc_latency=m.RPC_LATENCY,
            rpc_key_wait=m.RPC_KEY_WAIT,
            
            # range registry
            range_inflight=m.RANGE_INFLIGHT.labels(**base),
            max_range_inflight=m.MAX_RANGE_INFLIGHT.labels(**base),
        )

    # ---------- Helper ----------
    def rpc_done(self, provider: str, key: str):
        self.rpc_completed.labels(
            chain=self.chain,
            job=self.job,
            provider=provider,
            key=key,
        ).inc()

    def rpc_fail(self, provider: str, key: str):
        self.rpc_failed.labels(
            chain=self.chain,
            job=self.job,
            provider=provider,
            key=key,
        ).inc()

    def rpc_latency_observe(self, provider: str, value: float):
        self.rpc_latency.labels(
            chain=self.chain,
            job=self.job,
            provider=provider,
        ).observe(value)

    def derive(self, **extra_labels) -> "DerivedMetricsContext":
        """
        Create a derived context (e.g. per range / per window).
        """
        return DerivedMetricsContext(parent=self, extra_labels=extra_labels)



class DerivedMetricsContext:
    """
    Lightweight wrapper for per-task / per-range metrics.
    """

    def __init__(self, parent: MetricsContext, extra_labels: dict):
        self._parent = parent
        self._extra = extra_labels

    # example
    def rpc_done(self, provider: str, key: str):
        self._parent.rpc_completed.labels(
            chain=self._parent.chain,
            job=self._parent.job,
            provider=provider,
            key=key,
            **self._extra,
        ).inc()
