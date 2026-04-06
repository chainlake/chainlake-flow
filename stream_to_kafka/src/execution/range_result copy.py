from dataclasses import dataclass

# Range 数据面/执行结果（RPC → Kafka 的单位）
@dataclass
class RangeResult:
    range_id: int
    start_block: int
    end_block: int
    logs: list
    rpc: str
    key_env: str
    task_id: int
