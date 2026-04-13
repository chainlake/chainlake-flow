# import logging
class EventIdCalculator:

    def calculate_event_id(self, item):
        if item is None or not isinstance(item, dict):
            return None

        item_type = item.get("type")
        block_hash = item.get("block_hash")

        if not block_hash:
            # logging.warning(f"missing block_hash in item: {item}")
            return None

        # ------------------------
        # BLOCK
        # ------------------------
        if item_type == "block":
            return concat("block", block_hash)

        # ------------------------
        # TRANSACTION
        # ------------------------
        elif item_type == "transaction":
            tx_hash = item.get("hash")
            if tx_hash:
                return concat("tx", tx_hash)

        # ------------------------
        # RECEIPT
        # ------------------------
        elif item_type == "receipt":
            tx_hash = item.get("transaction_hash")
            if tx_hash:
                return concat("receipt", tx_hash)

        # ------------------------
        # LOG
        # ------------------------
        elif item_type == "log":
            tx_hash = item.get("transaction_hash")
            log_index = item.get("log_index")
            if tx_hash is not None and log_index is not None:
                return concat("log", tx_hash, log_index)

        # ------------------------
        # TRACE
        # ------------------------
        elif item_type == "trace":
            trace_id = item.get("trace_id")
            if trace_id:
                return concat("trace", trace_id)

        # logging.warning(f"item_id for item is None: {json.dumps(item)}")
        return None


def concat(*elements):
    return "_".join([str(e) for e in elements])