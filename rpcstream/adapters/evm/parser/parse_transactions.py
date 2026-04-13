def hex_to_dec(hex_string):
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except Exception:
        return None

# transaction flatten
def parse_transactions(block: dict):
    txs = block.get("transactions", [])
    block_timestamp = block.get("timestamp")
    
    results = []
    for i, tx in enumerate(txs):
        results.append({
            "type": "transaction",
            # --- identity ---
            "hash": tx.get("hash"),
            "block_hash": block.get("hash"),
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