from rpcstream.utils import hex_to_dec

# receipt + logs(eth_getBlockReceipts)
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