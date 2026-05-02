from __future__ import annotations

from dataclasses import asdict

from rpcstream.adapters.evm.entities.token_transfer import TokenTransfer
from rpcstream.utils.utils import hex_to_dec


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC1155_TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
ERC1155_TRANSFER_BATCH_TOPIC_PREFIX = "0x4a39dc06"


class EvmDecoder:
    def __init__(self, client=None, logger=None):
        self.client = client
        self.logger = logger

    async def decode(self, bundle: dict[str, list[dict]]) -> dict[str, list[dict]]:
        decoded = {entity: [row.copy() for row in rows] for entity, rows in bundle.items()}
        token_transfers: list[dict] = []

        for log in decoded.get("log", []):
            decoded_transfers = await self._decode_log(log)
            token_transfers.extend(decoded_transfers)

        if token_transfers:
            decoded["token_transfer"] = token_transfers
        return decoded

    async def _decode_log(self, log: dict) -> list[dict]:
        topics = [
            self._normalize_hex(topic)
            for topic in self._get_value(log, "topics", "topics")
            if topic is not None
        ]
        if not topics:
            return []

        topic0 = topics[0]
        if topic0 == TRANSFER_TOPIC:
            return await self._decode_transfer(log, topics)
        if topic0 == ERC1155_TRANSFER_SINGLE_TOPIC:
            return [self._decode_erc1155_single(log, topics)]
        if topic0.startswith(ERC1155_TRANSFER_BATCH_TOPIC_PREFIX):
            return self._decode_erc1155_batch(log, topics)
        return []

    async def _decode_transfer(self, log: dict, topics: list[str]) -> list[dict]:
        transfer_type = "unknown"
        token_id = None
        amount = None

        if len(topics) >= 4:
            transfer_type = "erc721"
            token_id = self._decode_topic_uint256(topics[3])
            amount = "1"
        elif len(topics) == 3:
            transfer_type = "erc20"
            amount = self._decode_uint256(log.get("data"))

        return [
            self._build_token_transfer(
                log=log,
                transfer_type=transfer_type,
                from_address=self._topic_to_address(topics, 1),
                to_address=self._topic_to_address(topics, 2),
                token_id=token_id,
                amount=amount,
            )
        ]

    def _decode_erc1155_single(self, log: dict, topics: list[str]) -> dict:
        data_words = self._split_words(self._get_value(log, "data", "data"))
        token_id = self._decode_word_uint256(data_words[0]) if len(data_words) >= 1 else None
        amount = self._decode_word_uint256(data_words[1]) if len(data_words) >= 2 else None

        return self._build_token_transfer(
            log=log,
            transfer_type="erc1155",
            from_address=self._topic_to_address(topics, 2),
            to_address=self._topic_to_address(topics, 3),
            token_id=token_id,
            amount=amount,
        )

    def _decode_erc1155_batch(self, log: dict, topics: list[str]) -> list[dict]:
        data = self._get_value(log, "data", "data")
        ids = self._decode_uint256_array(data, 0)
        values = self._decode_uint256_array(data, 1)
        if not ids or len(ids) != len(values):
            return []

        transfers: list[dict] = []
        for index, (token_id, amount) in enumerate(zip(ids, values)):
            transfers.append(
                self._build_token_transfer(
                    log=log,
                    transfer_type="erc1155",
                    from_address=self._topic_to_address(topics, 2),
                    to_address=self._topic_to_address(topics, 3),
                    token_id=token_id,
                    amount=amount,
                    transfer_index=index,
                )
            )
        return transfers

    def _build_token_transfer(
        self,
        *,
        log: dict,
        transfer_type: str,
        from_address: str | None,
        to_address: str | None,
        token_id: str | None = None,
        amount: str | None = None,
        transfer_index: int | None = 0,
    ) -> dict:
        source_log_id = self._build_source_log_id(log)
        transfer_id = self._build_transfer_id(source_log_id, transfer_index, log)
        token_transfer = TokenTransfer(
            transfer_type=transfer_type,
            source_log_id=source_log_id,
            token_address=self._get_value(log, "address", "address"),
            from_address=from_address,
            to_address=to_address,
            token_id=token_id,
            amount=amount,
            transaction_hash=self._get_value(log, "transaction_hash", "transactionHash"),
            transaction_index=self._get_value(log, "transaction_index", "transactionIndex"),
            block_hash=self._get_value(log, "block_hash", "blockHash"),
            block_number=self._get_value(log, "block_number", "blockNumber"),
            log_index=self._get_value(log, "log_index", "logIndex"),
            transfer_index=transfer_index,
        )
        row = asdict(token_transfer)
        row["id"] = transfer_id
        return row

    def _build_source_log_id(self, log: dict) -> str | None:
        tx_hash = self._get_value(log, "transaction_hash", "transactionHash")
        log_index = self._get_value(log, "log_index", "logIndex")
        if tx_hash is None or log_index is None:
            return None
        return f"log_{tx_hash}_{log_index}"

    def _build_transfer_id(
        self,
        source_log_id: str | None,
        transfer_index: int | None,
        log: dict,
    ) -> str | None:
        tx_hash = self._get_value(log, "transaction_hash", "transactionHash")
        log_index = self._get_value(log, "log_index", "logIndex")
        if tx_hash is None or log_index is None:
            return None
        if transfer_index is None:
            return f"token_transfer_{tx_hash}_{log_index}"
        return f"token_transfer_{tx_hash}_{log_index}_{transfer_index}"

    def _topic_to_address(self, topics: list[str], index: int) -> str | None:
        if index >= len(topics):
            return None
        topic = topics[index]
        if not topic:
            return None
        topic = self._normalize_hex(topic)
        if len(topic) < 42:
            return None
        return "0x" + topic[-40:]

    def _decode_topic_uint256(self, topic: str | None) -> str | None:
        if not topic:
            return None
        value = hex_to_dec(topic)
        return str(value) if value is not None else None

    def _decode_word_uint256(self, word: str | None) -> str | None:
        if not word:
            return None
        value = hex_to_dec(word)
        return str(value) if value is not None else None

    def _decode_uint256(self, data: str | None) -> str | None:
        words = self._split_words(data)
        if not words:
            return None
        return self._decode_word_uint256(words[0])

    def _decode_uint256_array(self, data: str | None, array_index: int) -> list[str]:
        if not data:
            return []
        payload = self._normalize_hex(data)
        if payload == "0x" or len(payload) <= 2:
            return []
        body = payload[2:]
        if len(body) < 64:
            return []
        offsets = self._decode_dynamic_offsets(body, 2)
        if array_index >= len(offsets):
            return []
        offset = offsets[array_index]
        if offset is None or offset < 0:
            return []
        start = offset * 2
        if start + 64 > len(body):
            return []
        length = hex_to_dec("0x" + body[start : start + 64])
        if length is None or length < 0:
            return []
        values: list[str] = []
        cursor = start + 64
        for _ in range(length):
            word = body[cursor : cursor + 64]
            if len(word) < 64:
                break
            value = hex_to_dec("0x" + word)
            if value is None:
                break
            values.append(str(value))
            cursor += 64
        return values

    def _decode_dynamic_offsets(self, body: str, count: int) -> list[int | None]:
        offsets: list[int | None] = []
        for index in range(count):
            word = body[index * 64 : (index + 1) * 64]
            if len(word) < 64:
                offsets.append(None)
                continue
            offsets.append(hex_to_dec("0x" + word))
        return offsets

    def _split_words(self, data: str | None) -> list[str]:
        if not data:
            return []
        payload = self._normalize_hex(data)
        if payload == "0x" or len(payload) <= 2:
            return []
        body = payload[2:]
        return [
            "0x" + body[i : i + 64]
            for i in range(0, len(body), 64)
            if len(body[i : i + 64]) == 64
        ]

    def _normalize_hex(self, value: str | None) -> str:
        if not value:
            return "0x"
        value = str(value).strip()
        return value.lower() if value.startswith("0x") else f"0x{value.lower()}"

    def _get_value(self, row: dict, *keys: str):
        for key in keys:
            if key in row:
                return row[key]
        return None
