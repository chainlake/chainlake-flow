def hex_to_dec(hex_string):
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except Exception:
        return None

# block (eth_getBlockByNumber)
def parse_blocks(block: dict):
    return {
        "type": "block",
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
        # "pipeline_stage": "rpc_ingest"
    }
    