from dataclasses import asdict

from rpcstream.adapters.evm.entities.log import Log
from rpcstream.adapters.evm.entities.receipt import Receipt
from rpcstream.utils.utils import hex_to_dec

# receipt + logs(eth_getBlockReceipts)
def parse_receipts(receipts: list):
    receipt_rows = []
    log_rows = []

    # eth_getBlockReceipts returns null (not []) for blocks without
    # transactions on some upstreams; never iterate None.
    for r in (receipts or []):
        block_number = hex_to_dec(r["blockNumber"])
        block_hash = r["blockHash"]

        receipt_rows.append(
            asdict(
                Receipt(
                    transaction_hash=r["transactionHash"],
                    transaction_index=hex_to_dec(r["transactionIndex"]),
                    block_hash=block_hash,
                    block_number=block_number,
                    from_address=r.get("from"),
                    to_address=r.get("to"),
                    cumulative_gas_used=hex_to_dec(r.get("cumulativeGasUsed")),
                    gas_used=hex_to_dec(r.get("gasUsed")),
                    contract_address=r.get("contractAddress"),
                    status=hex_to_dec(r.get("status")),
                    effective_gas_price=hex_to_dec(r.get("effectiveGasPrice")),
                    transaction_type=hex_to_dec(r.get("type")),
                    l1_fee=hex_to_dec(r.get("l1Fee")),
                    l1_gas_used=hex_to_dec(r.get("l1GasUsed")),
                    l1_gas_price=hex_to_dec(r.get("l1GasPrice")),
                    l1_fee_scalar=r.get("l1FeeScalar"),
                    blob_gas_price=hex_to_dec(r.get("blobGasPrice")),
                    blob_gas_used=hex_to_dec(r.get("blobGasUsed")),
                )
            )
        )

        # logs flatten -- some receipts (e.g. legacy/early blocks) carry
        # "logs": null instead of an empty array; treat null as no logs
        # rather than failing the whole cursor.
        for log in (r.get("logs") or []):
            log_rows.append(
                asdict(
                    Log(
                        log_index=hex_to_dec(log["logIndex"]),
                        transaction_hash=log["transactionHash"],
                        transaction_index=hex_to_dec(log["transactionIndex"]),
                        block_hash=log["blockHash"],
                        block_number=hex_to_dec(log["blockNumber"]),
                        address=log["address"],
                        data=log["data"],
                        topics=log["topics"],
                        removed=log.get("removed", False),
                    )
                )
            )

    return receipt_rows, log_rows
