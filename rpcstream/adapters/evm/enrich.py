from __future__ import annotations


RECEIPT_PREFIX_FIELDS = (
    "cumulative_gas_used",
    "gas_used",
    "contract_address",
    "status",
    "effective_gas_price",
    "transaction_type",
    "l1_fee",
    "l1_gas_used",
    "l1_gas_price",
    "l1_fee_scalar",
    "blob_gas_price",
    "blob_gas_used",
)


class EvmEnricher:
    def enrich(self, bundle: dict[str, list[dict]]) -> dict[str, list[dict]]:
        enriched = {entity: [row.copy() for row in rows] for entity, rows in bundle.items()}

        block_rows = enriched.get("block", [])
        block_by_number = {
            row["number"]: row
            for row in block_rows
            if row.get("number") is not None
        }

        self._enrich_transactions(enriched)
        self._inject_block_context(enriched.get("receipt", []), block_by_number)
        self._inject_block_context(enriched.get("log", []), block_by_number)
        self._inject_block_context(enriched.get("token_transfer", []), block_by_number)
        self._inject_block_context(enriched.get("trace", []), block_by_number)
        return enriched

    def _enrich_transactions(self, bundle: dict[str, list[dict]]) -> None:
        receipt_by_hash = {
            row.get("transaction_hash"): row
            for row in bundle.get("receipt", [])
            if row.get("transaction_hash") is not None
        }
        enriched_rows = []
        for tx in bundle.get("transaction", []):
            row = tx.copy()
            receipt = receipt_by_hash.get(tx.get("hash"))
            if receipt is not None:
                for field in RECEIPT_PREFIX_FIELDS:
                    row[f"receipt_{field}"] = receipt.get(field)
            enriched_rows.append(row)
        bundle["transaction"] = enriched_rows

    def _inject_block_context(
        self,
        rows: list[dict],
        block_by_number: dict[int, dict],
    ) -> None:
        for row in rows:
            block_number = row.get("block_number")
            if block_number is None:
                continue
            block = block_by_number.get(block_number)
            if block is None:
                continue
            row["block_hash"] = block.get("hash")
            row["block_timestamp"] = block.get("timestamp")
