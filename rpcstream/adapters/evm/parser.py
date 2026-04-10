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
  

# trace（trace_block）
def parse_traces(traces: list, block_number: int):
    rows = []

    for t in traces:
        rows.append({
            "block_number": block_number,
            "transaction_hash": t.get("transactionHash"),
            "transaction_index": t.get("transactionPosition"),
            "from_address": t.get("action", {}).get("from"),
            "to_address": t.get("action", {}).get("to"),
            "value": hex_to_dec(t.get("action", {}).get("value")),
            "input": t.get("action", {}).get("input"),
            "output": t.get("result", {}).get("output"),
            "trace_type": t.get("type"),
            "call_type": t.get("action", {}).get("callType"),
            "reward_type": t.get("action", {}).get("rewardType"),
            "gas": hex_to_dec(t.get("action", {}).get("gas")),
            "gas_used": hex_to_dec(t.get("result", {}).get("gasUsed")),
            "subtraces": t.get("subtraces"),
            "trace_address": t.get("traceAddress"),
            "error": t.get("error"),
            "status": t.get("result", {}).get("status"),
            "trace_id": t.get("traceId"),
        })

    return rows