import os
import json
import uuid
import asyncio
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import start_http_server
from confluent_kafka import KafkaException
from src import RangeResult, RangeRegistry, TailingRangePlanner, OrderedResultBuffer
from src.logging import log
from src.state import load_last_state
from src.rpc_provider import Web3AsyncRouter, AsyncRpcClient, AsyncRpcScheduler, RpcPool, RpcErrorResult
from src.kafka_utils import init_producer, get_serializers
from src.web3_utils import current_utctime, decide_resume_plan
from src.metrics import MetricsContext
from src.metrics.runtime import set_current_metrics, get_metrics
from src.tracking import LatestBlockTracker

# -----------------------------
# Environment Variables
# -----------------------------
RUN_ID = os.getenv("RUN_ID", str(uuid.uuid4()))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1")) # refresh interval of latest block number
CHAIN = os.getenv("CHAIN", "bsc").lower() # bsc, eth, base ... from blockchain-rpc-config.yaml
RESUME_FROM_CHECKPOINT = os.getenv("RESUME_FROM_CHECKPOINT", "True").lower() in ("1", "true", "yes")
# -----------------------------
# Kafka / Job Name / Transactional setting
# -----------------------------
JOB_START_TIME = current_utctime()

POD_UID = os.getenv("POD_UID", "unknown-pod")
POD_NAME = os.getenv("POD_NAME", "unknown-pod")
JOB_NAME = "realtime_logs"

STATE_KEY = f"{CHAIN}_{JOB_NAME}"

TRANSACTIONAL_ID = f"{STATE_KEY}.{POD_UID}" # TRANSACTIONAL_ID每次不一样，EOS由Compact State Topic实现

KAFKA_BROKER = "redpanda.kafka.svc:9092"
SCHEMA_REGISTRY_URL = "http://redpanda.kafka.svc:8081"

# Kafka Topics
BLOCKS_TOPIC = f"blockchain.{CHAIN}.ingestion.blocks.raw"
LOGS_TOPIC = f"blockchain.{CHAIN}.ingestion.logs.raw"
TXS_TOPIC = f"blockchain.{CHAIN}.ingestion.transactions.raw"
STATE_TOPIC = "blockchain.ingestion._state"

# -----------------------------
# RPC Config
# -----------------------------
RPC_CONFIG_PATH = "/etc/ingestion/rpc_providers.json"
RPC_MAX_TIMEOUT = int(os.getenv("RPC_MAX_TIMEOUT", "10"))
RPC_MAX_INFLIGHT = int(os.getenv("RPC_MAX_INFLIGHT", "5")) 
# RPC_MAX_INFLIGHT < key_count * 2
# RPC_MAX_INFLIGHT < MAX_INFLIGHT_RANGE < MAX_QUEUE
MAX_INFLIGHT_RANGE = RPC_MAX_INFLIGHT * 2 
MAX_QUEUE=RPC_MAX_INFLIGHT * 3

# -----------------------------
# Log fetching Config
# -----------------------------
RANGE_SIZE = int(os.getenv("RANGE_SIZE", "5")) # how many blocks of range to fetch for logs
# -----------------------------
# RPC Initilization
# -----------------------------
rpc_configs = json.load(open(RPC_CONFIG_PATH))
rpc_pool = RpcPool.grouped_from_config(rpc_configs, CHAIN)

# -----------------------------
# Kafka Producer initialization
# -----------------------------
topics_value_serializer = get_serializers(SCHEMA_REGISTRY_URL, BLOCKS_TOPIC, STATE_TOPIC, LOGS_TOPIC, TXS_TOPIC)
blocks_value_serializer, state_value_serializer, logs_value_serializer, txs_value_serializer = topics_value_serializer
producer = init_producer(TRANSACTIONAL_ID, KAFKA_BROKER)

# -----------------------------
# submit range
# -----------------------------
async def submit_range(scheduler, registry, r):
    task = asyncio.create_task(
        scheduler.submit(
            "eth_getLogs",
            [{
                "fromBlock": hex(r.start_block),
                "toBlock": hex(r.end_block),
                # Optional: "topics": [...], "address" : [...]
            }],
            meta={
                "range_id": r.range_id,
                "start_block": r.start_block,
                "end_block": r.end_block,
                "retry": r.retry,
            },
        )
    )
    registry.mark_inflight(r.range_id, task_id=id(task))
    return task


async def run_stream_ingest(
    *,
    start_block,
    rpc_pool,
    producer,
    max_inflight_ranges: int = MAX_INFLIGHT_RANGE,
    run_mode,
):
    try:
        # -----------------------------
        # Init metrics
        # -----------------------------
        metrics_context = MetricsContext.from_env()
        set_current_metrics(metrics_context)
        metrics = get_metrics() 
        
        metrics.max_range_inflight_set(MAX_INFLIGHT_RANGE)
        
        # -----------------------------
        # RPC infra
        # -----------------------------
        client = AsyncRpcClient(timeout=RPC_MAX_TIMEOUT)
        router = Web3AsyncRouter(rpc_pool, client)

        scheduler = AsyncRpcScheduler(
            router=router,
            max_workers=1,
            max_inflight=RPC_MAX_INFLIGHT,
            max_queue=MAX_QUEUE,
        )

        planner = TailingRangePlanner(start_block, RANGE_SIZE)
        
        # -----------------------------
        # Control plane / Ordering / Inflight Window
        # -----------------------------
        registry = RangeRegistry()
        ordered_buffer = OrderedResultBuffer()
        inflight: set[asyncio.Task] = set()

        # -----------------------------
        # Pre-fill inflight window
        # -----------------------------
        latest_tracker = LatestBlockTracker(router, refresh_interval=POLL_INTERVAL)
        latest_tracker.start()

        while True:
            latest_block = latest_tracker.get_cached()
            if latest_block is not None:
                break
            # 启动阶段 or RPC 全挂
            await asyncio.sleep(0.2)

        while len(inflight) < max_inflight_ranges:
            r = planner.next_range(latest_block)
            if not r:
                break
            
            rr = registry.register(r.range_id, r.start_block, r.end_block)
            inflight.add(await submit_range(scheduler, registry, rr))
            
        # -----------------------------
        # Main streaming loop
        # -----------------------------
        while True:
            if not inflight:
                # realtime idle waiting
                await asyncio.sleep(0.2)
                latest_block = latest_tracker.get_cached()
                
                # refill
                if latest_block is not None:
                    while len(inflight) < max_inflight_ranges:
                        r = planner.next_range(latest_block)
                        if not r:
                            break
                        rr = registry.register(r.range_id, r.start_block, r.end_block)
                        inflight.add(await submit_range(scheduler, registry, rr))
                continue

            
            done, _ = await asyncio.wait(
                inflight,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                inflight.remove(task)
                result = await task
                
                # -----------------------------
                #  RPC error processing
                # -----------------------------
                if isinstance(result, RpcErrorResult):
                    # 失败 → 标记 RangeRecord 并重试
                    meta = result.meta
                    range_id = meta.extra["range_id"]
                    
                    retry_ok = registry.mark_retry(
                        range_id,
                        error=str(result.error),
                    )
                    # log.warning(
                    #     "❌ rpc_range_failed",
                    #     extra={
                    #         "range_id": range_id,
                    #         "retry": registry.get(range_id).retry,
                    #         "rpc": result.rpc or "UNKNOWN",
                    #         "key_env": result.key_env or "UNKNOWN",
                    #         "error": str(result.error),
                    #     },
                    # )
                    # 重新提交该 range（换 RPC）
                    if retry_ok:
                        r = registry.get(range_id)
                        inflight.add(await submit_range(scheduler, registry, r))
                    else:
                        registry.mark_failed(range_id, str(result.error))

                    continue # 跳过下面成功处理逻辑

                # ----------成功 RPC，解包, 顺序消费 ----------  
                logs, rpc, key_env, trace, wid, meta = result

                rr = RangeResult(
                    range_id=meta.extra["range_id"],
                    start_block=meta.extra["start_block"],
                    end_block=meta.extra["end_block"],
                    logs=logs or [],
                    rpc=rpc,
                    key_env=key_env,
                    task_id=meta.task_id,
                )

                ordered_buffer.add(rr)

                # ---------- 顺序消费 ----------
                ready_ranges = ordered_buffer.pop_ready()

                for rr in ready_ranges:
                    # ===== Kafka EOS =====
                    producer.begin_transaction()

                    # -----------------------------
                    # produce logs
                    # -----------------------------
                    logs_by_block = {}
                    for log_item in rr.logs:
                        bn = log_item.get("blockNumber")
                        if bn is None:
                            continue
                        if isinstance(bn, str):
                            bn = int(bn, 16)
                        logs_by_block.setdefault(bn, []).append(log_item)

                    for bn, txs in logs_by_block.items():
                        total_logs = len(txs)
                        
                        metrics.block_processed.inc()
                        metrics.tx_processed.inc(total_logs)
                        
                        for idx, tx in enumerate(txs):
                            
                            log_record = {
                                "block_height": bn,
                                "job_name": JOB_NAME,
                                "run_id": RUN_ID,
                                "raw": json.dumps(tx),
                            }
                            
                            producer.produce(
                                topic=LOGS_TOPIC,
                                key=f"{bn}-{idx}",
                                value=logs_value_serializer(
                                    log_record,
                                    SerializationContext(
                                        LOGS_TOPIC, MessageField.VALUE
                                    ),
                                ),
                            )
                        producer.poll(0)
                            
                    # -----------------------------
                    # commit checkpoint（range-level）
                    # -----------------------------
                    last_committed_block = rr.end_block
                    
                    state_record = {
                        "checkpoint": last_committed_block,
                        "producer": {
                            "pod_uid": POD_UID,
                            "pod_name": POD_NAME
                        },
                        "run": {
                            "run_id": f"{RUN_ID}",
                            "mode": run_mode,
                            "started_at": JOB_START_TIME,
                            "start_block" : start_block
                        },
                    }
                    
                    producer.produce(
                        STATE_TOPIC,
                        key=STATE_KEY,
                        value=state_value_serializer(
                            state_record,
                            SerializationContext(
                                STATE_TOPIC, MessageField.VALUE
                            ),
                        ),
                    )
                    producer.poll(0)
                    producer.commit_transaction()

                    registry.mark_done(rr.range_id)
                    
                    log.info(
                        "✅ range_committed",
                        extra={
                            # "task_id": rr.task_id,
                            "range_id": rr.range_id,
                            "start": rr.start_block,
                            "end": rr.end_block,
                            "rpc": rr.rpc,
                            "key_env": rr.key_env,
                            "log_count": len(rr.logs),
                        },
                    )
                    
                    # 系统 checkpoint 推进速度
                    metrics.tx_per_block.observe(total_logs)
                    metrics.block_committed.inc()
                    metrics.tx_committed.inc(total_logs)

                    latest_block = latest_tracker.get_cached()
                    if latest_block is not None:
                        metrics.chain_latest_block.set(latest_block)
                        metrics.checkpoint_block.set(last_committed_block)
                        metrics.checkpoint_lag.set(max(0, latest_block - last_committed_block))
                
                # -----------------------------
                # Refill inflight window
                # -----------------------------
                latest_block = latest_tracker.get_cached()
                
                if latest_block is not None:
                    while len(inflight) < max_inflight_ranges:
                        r = planner.next_range(latest_block)
                        if not r:                     
                            break
                        
                        rr = registry.register(
                            r.range_id,
                            r.start_block,
                            r.end_block,
                        )
                        inflight.add(await submit_range(scheduler, registry, rr))
                        
    except KafkaException:
        log.exception("kafka txn failed")
        try:
            producer.abort_transaction()
        except Exception:
            pass

        producer = init_producer(TRANSACTIONAL_ID, KAFKA_BROKER)
        raise  # 让 range retry

    except Exception:
        log.exception("non-kafka error, logic bug")
        try:
            producer.abort_transaction()
        except Exception:
            pass
        raise

# Entrypoint
async def main():
    
    client = AsyncRpcClient(timeout=RPC_MAX_TIMEOUT)
    router = Web3AsyncRouter(rpc_pool, client)

    latest_block, *_ = await router.call_once("eth_blockNumber", [])
    if isinstance(latest_block, str):
        latest_block = int(latest_block, 16)

    last_state = None
    if RESUME_FROM_CHECKPOINT:
        last_state = load_last_state(
            state_key=STATE_KEY,
            kafka_broker=KAFKA_BROKER,
            state_topic=STATE_TOPIC,
            schema_registry_url=SCHEMA_REGISTRY_URL,
        )

    start_block, resume_mode = decide_resume_plan(
        resume_from_checkpoint=RESUME_FROM_CHECKPOINT,
        last_state=last_state,
        latest_block=latest_block
    )
    
    run_mode = f"{resume_mode}_resume"

    log.info(
        "▶️ job_start",
        extra={
            "chain": CHAIN,
            "job": JOB_NAME,
            "range_size": RANGE_SIZE,
            "start_block": start_block,
            "run_mode": run_mode
        },
    )
    
    await run_stream_ingest(
        start_block=start_block,
        rpc_pool=rpc_pool,
        producer=producer,
        run_mode=run_mode
        )

if __name__ == "__main__":
    # Prometheus metrics endpoint
    start_http_server(8000)
    asyncio.run(main())