import os
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from typing import Tuple
from datetime import datetime, timedelta, timezone

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "<YOUR-API-KEY>")
ALCHEMY_BASE_URL = "https://bnb-mainnet.g.alchemy.com/v2"

if not ALCHEMY_API_KEY or "<YOUR-API-KEY>" in ALCHEMY_API_KEY:
    raise RuntimeError("ALCHEMY_API_KEY is not configured")

# -----------------------------
# Connect to BSC RPC
# -----------------------------
BSC_RPC_URL = f"{ALCHEMY_BASE_URL}/{ALCHEMY_API_KEY}"
w3 = Web3(Web3.HTTPProvider(BSC_RPC_URL))
w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
assert w3.is_connected(), "Cannot connect to RPC"

# -----------------------------
# Timestamp → Block Number
# -----------------------------
def get_block_no_by_time(unix_timestamp: int, closest: str = "after") -> int:
    """
    Find the closest block number to a given timestamp.
    
    Args:
        unix_timestamp: int, seconds since epoch
        closest: 'before' or 'after'
    
    Returns:
        block_number: int
    """
    assert closest in ("before", "after"), f"closest must be 'before' or 'after'"

    latest_block = w3.eth.block_number
    earliest_block = 1  # BSC 从 block 1 开始
    
    # Binary search
    low = earliest_block
    high = latest_block
    result_block = None

    while low <= high:
        mid = (low + high) // 2
        block = w3.eth.get_block(mid) # eth_getBlockByNumber
        ts = block["timestamp"]

        if ts < unix_timestamp:
            low = mid + 1
        elif ts > unix_timestamp:
            high = mid - 1
        else:
            # 100% match
            result_block = mid
            break

    if result_block is None:
        # not 100% match, return based on the closest
        if closest == "after":
            result_block = low  # low for the first ts >= unix_timestamp
        else:
            result_block = high  # high for the first ts <= unix_timestamp

    return result_block

# ----------------------------------------
# Date → unix timestamp (UTC) [start, end)
# ----------------------------------------
def date_to_timestamp(date_str: str, end: bool = False) -> int:
    """
    date_str: 'YYYY-MM-DD'
    end=False  -> Day + 0 Date 00:00:00 UTC
    end=True   -> Day + 1 Date 00:00:00 UTC
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)

    if end:
        dt = dt + timedelta(days=1)

    return int(dt.timestamp())

# ----------------------------------------
# Convert：date range → block range
# ----------------------------------------
def get_block_range_by_date(
    start_date: str,
    end_date: str,
) -> Tuple[int, int]:
    """
    Input:
      start_date: 'YYYY-MM-DD'
      end_date:   'YYYY-MM-DD'

    Apply:
      [start_date 00:00:00,
       end_date+1 00:00:00)
    """

    if start_date > end_date:
        raise ValueError(f"start_date: {start_date} > end_date: {end_date}")

    start_ts = date_to_timestamp(start_date, end=False)
    end_ts = date_to_timestamp(end_date, end=True)

    start_block = get_block_no_by_time(
        unix_timestamp=start_ts,
        closest="after",
    )

    end_block = get_block_no_by_time(
        unix_timestamp=end_ts,
        closest="before",
    )

    if start_block > end_block:
        raise RuntimeError(
            f"Invalid block range: {start_block} > {end_block}"
        )

    print(
        f"Date range {start_date} ~ {end_date} "
        f"→ blocks [{start_block}, {end_block}]",
        flush=True,
    )

    return start_block, end_block

# -----------------------------
# Demo
# valide: start_block ('2026-01-11') = end_block ('2026-01-10')
# -----------------------------
if __name__ == "__main__":
    start_date = '2026-01-11'
    end_date = '2026-01-11'
    start_block, end_block = get_block_range_by_date(start_date, end_date)