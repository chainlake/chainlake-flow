def hex_to_dec(hex_string):
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except Exception:
        return None

# parser - normalize / flatten / type cast

# block (eth_getBlockByNumber)
def parse_blocks(block: dict):
    return {
        # --- Block Identity ---
        "number": hex_to_dec(block["number"]),  # Block height (monotonically increasing identifier)
        "hash": block["hash"],  # Current block hash
        "parent_hash": block["parentHash"],  # Parent block hash (used for chain linkage / reorg detection)

        # --- PoW Legacy Fields (mostly irrelevant in PoS chains) ---
        "nonce": hex_to_dec(block.get("nonce")),  # PoW mining nonce (usually 0 or null in PoS)
        "sha3_uncles": block.get("sha3Uncles"),  # Uncle block hash (largely unused in PoS era)

        # --- State & Trie Roots (Merkle Patricia Trie) ---
        "logs_bloom": block.get("logsBloom"),  # Bloom filter for fast log lookup (DO NOT decode)
        "transactions_root": block.get("transactionsRoot"),  # Root of transactions trie
        "state_root": block.get("stateRoot"),  # Global state trie root (core state commitment)
        "receipts_root": block.get("receiptsRoot"),  # Root of receipts trie

        # --- Block Producer Info ---
        "miner": block.get("miner"),
        # PoW: miner address
        # PoS: execution-layer fee recipient (not validator identity)

        # --- Difficulty (no longer meaningful in PoS) ---
        "difficulty": hex_to_dec(block.get("difficulty", "0x0")),
        "total_difficulty": hex_to_dec(block.get("totalDifficulty", "0x0")),
        # After PoS transition, these fields are mostly historical / frozen

        # --- Block Size ---
        "size": hex_to_dec(block.get("size", "0x0")),  # Block size in bytes

        # --- Extra Data ---
        "extra_data": block.get("extraData"),
        # Arbitrary client/validator metadata (often contains validator info in BSC)

        # --- Gas Accounting ---
        "gas_limit": hex_to_dec(block.get("gasLimit", "0x0")),  # Maximum gas allowed in block
        "gas_used": hex_to_dec(block.get("gasUsed", "0x0")),  # Actual gas consumed
        "timestamp": hex_to_dec(block.get("timestamp", "0x0")),  # Block timestamp (seconds since epoch)

        # --- Transaction Statistics ---
        "transaction_count": len(block.get("transactions", [])),
        # Depends on RPC mode:
        # - full mode: list of transaction objects
        # - hash mode: list of tx hashes only

        # --- EIP-1559 Fee Market ---
        "base_fee_per_gas": hex_to_dec(block.get("baseFeePerGas")),
        # Present only in EIP-1559 compatible chains (Ethereum, BSC, Polygon, etc.)
        # Absent in legacy blocks (None)

        # --- Ethereum PoS Specific (EIP-4895 withdrawals) ---
        "withdrawals_root": block.get("withdrawalsRoot"),
        "withdrawals": block.get("withdrawals"),
        # Validator staking withdrawals (Beacon Chain → Execution Layer)
        # Only present in Ethereum PoS
        # Always None for BSC / most other EVM chains

        # --- EIP-4844 (Proto-Danksharding / Blob Transactions) ---
        "blob_gas_used": hex_to_dec(block.get("blobGasUsed")),
        "excess_blob_gas": hex_to_dec(block.get("excessBlobGas")),
        # Blob gas accounting for data availability (EIP-4844)
        # Only supported on Ethereum (post-Dencun upgrade)
        # Typically None on most other EVM chains
    }
    

# transaction flatten
def parse_transactions(block: dict):
    txs = block.get("transactions", [])

    results = []
    for i, tx in enumerate(txs):
        results.append({
            # --- identity ---
            "hash": tx["hash"],
            "block_hash": block["hash"],
            "transaction_index": i,

            # --- addresses ---
            "from_address": tx.get("from"),
            "to_address": tx.get("to"),

            # --- numeric core ---
            "nonce": hex_to_dec(tx.get("nonce")),
            "block_number": hex_to_dec(block.get("number")),
            "block_timestamp": hex_to_dec(block.get("timestamp")),
            "value": hex_to_dec(tx.get("value")),
            "gas": hex_to_dec(tx.get("gas")),
            "gas_price": hex_to_dec(tx.get("gasPrice")),

            # --- EIP-1559 ---
            "max_fee_per_gas": hex_to_dec(tx.get("maxFeePerGas")),
            "max_priority_fee_per_gas": hex_to_dec(tx.get("maxPriorityFeePerGas")),

            # --- blob (optional future) ---
            "max_fee_per_blob_gas": hex_to_dec(tx.get("maxFeePerBlobGas")),

            # --- tx type ---
            "transaction_type": hex_to_dec(tx.get("type")),
            "chain_id": hex_to_dec(tx.get("chainId")),
            "v": hex_to_dec(tx.get("v")),

            # --- signature (keep raw) ---
            "r": tx.get("r"),
            "s": tx.get("s"),

            # --- data ---
            "input": tx.get("input"),
            "blob_versioned_hashes": tx.get("blobVersionedHashes"),
        })

    return results


# receipt + logs（eth_getBlockReceipts）
def parse_receipts(receipts: list):
    receipt_rows = []
    log_rows = []

    for r in receipts:
        block_number = hex_to_dec(r["blockNumber"])
        block_hash = r["blockHash"]

        receipt_rows.append({
            "transaction_hash": r["transactionHash"],
            "transaction_index": hex_to_dec(r["transactionIndex"]),
            "block_hash": block_hash,
            "block_number": block_number,

            "from_address": r.get("from"),
            "to_address": r.get("to"),

            "cumulative_gas_used": hex_to_dec(r.get("cumulativeGasUsed")),
            "gas_used": hex_to_dec(r.get("gasUsed")),

            "contract_address": r.get("contractAddress"),
            "status": hex_to_dec(r.get("status")),

            "effective_gas_price": hex_to_dec(r.get("effectiveGasPrice")),

            "transaction_type": hex_to_dec(r.get("type")),

            # optional (L2 / blob)
            "l1_fee": hex_to_dec(r.get("l1Fee")),
            "l1_gas_used": hex_to_dec(r.get("l1GasUsed")),
            "l1_gas_price": hex_to_dec(r.get("l1GasPrice")),
            "l1_fee_scalar": r.get("l1FeeScalar"),

            "blob_gas_price": hex_to_dec(r.get("blobGasPrice")),
            "blob_gas_used": hex_to_dec(r.get("blobGasUsed")),
        })

        # logs flatten
        for log in r.get("logs", []):
            log_rows.append({
                "log_index": hex_to_dec(log["logIndex"]),
                "transaction_hash": log["transactionHash"],
                "transaction_index": hex_to_dec(log["transactionIndex"]),
                "block_hash": log["blockHash"],
                "block_number": hex_to_dec(log["blockNumber"]),
                "address": log["address"],
                "data": log["data"],
                "topics": log["topics"],
                "removed": log.get("removed", False),
            })

    return receipt_rows, log_rows
  

# trace (trace_block)
def parse_trace_block(traces: list, block_number: int):
    rows = []

    for t in traces:
        action = t.get("action") or {}
        result = t.get("result") or {} # result being None is NORMAL behavior
        
        trace_address = t.get("traceAddress") or []

        # unified trace_id logic (same as debug_trace)
        tx_hash = t.get("transactionHash")
        trace_id = build_trace_id(tx_hash, trace_address)

        # parent_trace_id (for graph consistency)
        parent_trace_id = None
        if trace_address:
            parent_trace_id = build_trace_id(tx_hash, trace_address[:-1])
        
        rows.append({
            # identity
            "block_number": block_number,
            "transaction_hash": t.get("transactionHash"),

            # trace identity
            "trace_id": trace_id,
            "parent_trace_id": parent_trace_id,
            "trace_index": t.get("transactionPosition"),
            
            # addresses
            "from_address": action.get("from"),
            "to_address": action.get("to"),
            
            # value + data
            "value": hex_to_dec(action.get("value")),
            "input": action.get("input"),
            "output": result.get("output"),
            
            # execution
            "call_type": action.get("callType"),
            "trace_type": t.get("type"),
            "status": result.get("status"),
            "error": t.get("error"),
            
            # gas
            "gas": hex_to_dec(action.get("gas")),
            "gas_used": hex_to_dec(result.get("gasUsed")),
            
            # structure
            "depth": len(t.get("traceAddress", [])),
            "subtraces": t.get("subtraces"),
            
            # unified fields
            "reward_type": action.get("rewardType"),
            "trace_address": trace_address,
            
            # origin metadata ⭐
            "trace_source": "parity",
            "trace_method": "trace_block",
            
            
        })
    return rows


# trace (debug_traceBlockByNumber)
def parse_debug_trace_block(traces, block_number):
    rows = []

    for tx in traces:
        tx_hash = tx.get("txHash") or tx.get("transactionHash")

        result = tx.get("result")
        if not result:
            continue

        rows.extend(
            flatten_call(result, block_number, tx_hash)
        )

    return rows

def flatten_call(call, block_number, tx_hash, depth=0, parent_trace_id=None, trace_address=None):
    rows = []

    if trace_address is None:
        trace_address = []
    
    # deterministic trace_id
    trace_id = build_trace_id(tx_hash, trace_address)

    rows.append({
        "block_number": block_number,
        "transaction_hash": tx_hash,

        "trace_id": trace_id,
        "parent_trace_id": parent_trace_id,
        "trace_index": None,

        "from_address": call.get("from"),
        "to_address": call.get("to"),
        
        "value": hex_to_dec(call.get("value")),
        "input": call.get("input"),
        "output": call.get("output"),

        "call_type": call.get("type"),
        "trace_type": "call",
        "status": None,
        "error": call.get("error"),
        
        "gas": hex_to_dec(call.get("gas")),
        "gas_used": hex_to_dec(call.get("gasUsed")),

        "depth": depth,
        "subtraces": len(call.get("calls", [])),

        # unified fields
        "reward_type": None,
        "trace_address": trace_address.copy(),
            
        "trace_source": "geth",
        "trace_method": "debug_trace",
    })

    for i, subcall in enumerate(call.get("calls", []) or []):
        rows.extend(
            flatten_call(
                subcall,
                block_number,
                tx_hash,
                depth + 1,
                trace_id,
                trace_address + [i]
            )
        )

    return rows

# Trace parser dispatcher (clean entrypoint)
def parse_traces_auto(value, block_number, trace_type):
    if trace_type == "trace_block":
        return parse_trace_block(value, block_number)
    elif trace_type == "debug_trace":
        return parse_debug_trace_block(value, block_number)
    else:
        raise ValueError(f"unknown trace_type: {trace_type}")
    
# helper function
def build_trace_id(tx_hash, trace_address):
    if not trace_address:
        return f"{tx_hash}_root"
    return f"{tx_hash}_{'_'.join(map(str, trace_address))}"