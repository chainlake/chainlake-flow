def hex_to_dec(hex_string):
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except ValueError:
        print("Not a hex string %s" % hex_string)
        return hex_string

# parser - normalize / flatten / type cast

# block (eth_getBlockByNumber)
def parse_blocks(block: dict):
    return {
        "number": int(block["number"], 16),
        "hash": block["hash"],
        "parent_hash": block["parentHash"],
        "nonce": block.get("nonce"),
        "sha3_uncles": block.get("sha3Uncles"),
        "logs_bloom": block.get("logsBloom"),
        "transactions_root": block.get("transactionsRoot"),
        "state_root": block.get("stateRoot"),
        "receipts_root": block.get("receiptsRoot"),
        "miner": block.get("miner"),
        "difficulty": int(block.get("difficulty", "0x0"), 16),
        "total_difficulty": int(block.get("totalDifficulty", "0x0"), 16),
        "size": int(block.get("size", "0x0"), 16),
        "extra_data": block.get("extraData"),
        "gas_limit": int(block.get("gasLimit", "0x0"), 16),
        "gas_used": int(block.get("gasUsed", "0x0"), 16),
        "timestamp": int(block.get("timestamp", "0x0"), 16),
        "transaction_count": len(block.get("transactions", [])),
        "base_fee_per_gas": block.get("baseFeePerGas"),
        "withdrawals_root": block.get("withdrawalsRoot"),
        "withdrawals": block.get("withdrawals"),
        "blob_gas_used": block.get("blobGasUsed"),
        "excess_blob_gas": block.get("excessBlobGas"),
    }
    


# transaction flatten
def parse_transactions(block: dict):
    txs = block.get("transactions", [])

    results = []
    for i, tx in enumerate(txs):
        results.append({
            "hash": tx["hash"],
            "nonce": int(tx.get("nonce", "0x0"), 16),
            "block_hash": block["hash"],
            "block_number": int(block["number"], 16),
            "transaction_index": i,
            "from_address": tx.get("from"),
            "to_address": tx.get("to"),
            "value": int(tx.get("value", "0x0"), 16),
            "gas": int(tx.get("gas", "0x0"), 16),
            "gas_price": int(tx.get("gasPrice", "0x0"), 16),
            "input": tx.get("input"),
            "block_timestamp": int(block.get("timestamp", "0x0"), 16),
            "transaction_type": hex_to_dec(tx.get("type")),
            
            "r": tx.get("r"),
            "s": tx.get("s"),
            "v": hex_to_dec(tx.get("v")),
            "chain_id": hex_to_dec(tx.get("chainId")),
            
            # optional
            "max_fee_per_gas": tx.get("maxFeePerGas"),
            "max_priority_fee_per_gas": tx.get("maxPriorityFeePerGas"),
            "max_fee_per_blob_gas": tx.get("maxFeePerBlobGas"),
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
            "value": t.get("action", {}).get("value"),
            "input": t.get("action", {}).get("input"),
            "output": t.get("result", {}).get("output"),
            "trace_type": t.get("type"),
            "call_type": t.get("action", {}).get("callType"),
            "reward_type": t.get("action", {}).get("rewardType"),
            "gas": t.get("action", {}).get("gas"),
            "gas_used": t.get("result", {}).get("gasUsed"),
            "subtraces": t.get("subtraces"),
            "trace_address": t.get("traceAddress"),
            "error": t.get("error"),
            "status": t.get("result", {}).get("status"),
            "trace_id": t.get("traceId"),
        })

    return rows