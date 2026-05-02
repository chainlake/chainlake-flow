from __future__ import annotations

import asyncio

from rpcstream.adapters.evm.decoder import EvmDecoder


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TRANSFER_BATCH_TOPIC = "0x4a39dc06ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"


def _topic_address(value: str) -> str:
    return "0x" + ("0" * 24) + value.removeprefix("0x").lower()


def _word(value: int) -> str:
    return "0x" + format(value, "064x")


def _erc1155_batch_data(ids: list[int], values: list[int]) -> str:
    ids_offset = 64
    values_offset = 64 + 32 + (len(ids) * 32)
    words = [_word(ids_offset)[2:], _word(values_offset)[2:]]

    ids_tail = [format(len(ids), "064x")] + [format(value, "064x") for value in ids]
    values_tail = [format(len(values), "064x")] + [format(value, "064x") for value in values]
    return "0x" + "".join(words + ids_tail + values_tail)


def test_evm_decoder_decodes_transfers():
    decoder = EvmDecoder()
    bundle = {
        "log": [
            {
                "type": "log",
                "log_index": 1,
                "transaction_hash": "0xtx20",
                "transaction_index": 7,
                "block_hash": "0xblock",
                "block_number": 11,
                "address": "0xerc20",
                "data": _word(250),
                "topics": [
                    TRANSFER_TOPIC,
                    _topic_address("0x1111111111111111111111111111111111111111"),
                    _topic_address("0x2222222222222222222222222222222222222222"),
                ],
            },
            {
                "type": "log",
                "log_index": 2,
                "transaction_hash": "0xtx721",
                "transaction_index": 8,
                "block_hash": "0xblock",
                "block_number": 11,
                "address": "0xerc721",
                "data": "0x",
                "topics": [
                    TRANSFER_TOPIC,
                    _topic_address("0x3333333333333333333333333333333333333333"),
                    _topic_address("0x4444444444444444444444444444444444444444"),
                    _word(9001),
                ],
            },
            {
                "type": "log",
                "log_index": 3,
                "transaction_hash": "0xtx1155",
                "transaction_index": 9,
                "block_hash": "0xblock",
                "block_number": 11,
                "address": "0xerc1155",
                "data": "0x" + format(77, "064x") + format(5, "064x"),
                "topics": [
                    TRANSFER_SINGLE_TOPIC,
                    _topic_address("0x5555555555555555555555555555555555555555"),
                    _topic_address("0x6666666666666666666666666666666666666666"),
                    _topic_address("0x7777777777777777777777777777777777777777"),
                ],
            },
            {
                "type": "log",
                "log_index": 4,
                "transaction_hash": "0xtx1155batch",
                "transaction_index": 10,
                "block_hash": "0xblock",
                "block_number": 11,
                "address": "0xerc1155batch",
                "data": _erc1155_batch_data([11, 12], [21, 22]),
                "topics": [
                    TRANSFER_BATCH_TOPIC,
                    _topic_address("0x8888888888888888888888888888888888888888"),
                    _topic_address("0x9999999999999999999999999999999999999999"),
                    _topic_address("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                ],
            },
            {
                "type": "log",
                "log_index": 5,
                "transaction_hash": "0xtxunknown",
                "transaction_index": 11,
                "block_hash": "0xblock",
                "block_number": 11,
                "address": "0xunknown",
                "data": _word(333),
                "topics": [
                    TRANSFER_TOPIC,
                    _topic_address("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                    _topic_address("0xcccccccccccccccccccccccccccccccccccccccc"),
                ],
            },
        ]
    }

    decoded = asyncio.run(decoder.decode(bundle))
    transfers = decoded["token_transfer"]

    assert len(transfers) == 6
    assert transfers[0]["transfer_type"] == "erc20"
    assert transfers[0]["source_log_id"] == "log_0xtx20_1"
    assert transfers[0]["id"] == "token_transfer_0xtx20_1_0"
    assert transfers[0]["amount"] == "250"
    assert transfers[0]["token_id"] is None
    assert transfers[1]["transfer_type"] == "erc721"
    assert transfers[1]["source_log_id"] == "log_0xtx721_2"
    assert transfers[1]["id"] == "token_transfer_0xtx721_2_0"
    assert transfers[1]["amount"] == "1"
    assert transfers[1]["token_id"] == "9001"
    assert transfers[2]["transfer_type"] == "erc1155"
    assert transfers[2]["source_log_id"] == "log_0xtx1155_3"
    assert transfers[2]["id"] == "token_transfer_0xtx1155_3_0"
    assert transfers[2]["token_id"] == "77"
    assert transfers[2]["amount"] == "5"
    assert transfers[3]["transfer_type"] == "erc1155"
    assert transfers[3]["source_log_id"] == "log_0xtx1155batch_4"
    assert transfers[3]["id"] == "token_transfer_0xtx1155batch_4_0"
    assert transfers[3]["token_id"] == "11"
    assert transfers[3]["amount"] == "21"
    assert transfers[3]["transfer_index"] == 0
    assert transfers[4]["id"] == "token_transfer_0xtx1155batch_4_1"
    assert transfers[4]["token_id"] == "12"
    assert transfers[4]["amount"] == "22"
    assert transfers[4]["transfer_index"] == 1
    assert transfers[5]["transfer_type"] == "erc20"
    assert transfers[5]["source_log_id"] == "log_0xtxunknown_5"
    assert transfers[5]["id"] == "token_transfer_0xtxunknown_5_0"
    assert transfers[5]["amount"] == "333"


def test_evm_decoder_accepts_camelcase_raw_logs():
    decoder = EvmDecoder()
    bundle = {
        "log": [
            {
                "type": "log",
                "logIndex": "118",
                "transactionHash": "0x392df770fd46ed80400cf1adf2d9aab4b1e0e2817728ffbbf2af5343fbd1c521",
                "transactionIndex": "37",
                "blockHash": "0xcf633128184ebee354eaf34d05e4a12dde54399bf4b6a543fafea36c6747ccbd",
                "blockNumber": "95875358",
                "blockTimestamp": "1777696810",
                "address": "0x4829a1d1fb6ded1f81d26868ab8976648baf9893",
                "data": "0x000000000000000000000000000000000000000000000001b60ab9b0ef2e8733",
                "topics": [
                    TRANSFER_TOPIC,
                    "0x00000000000000000000000035e5604202490c6ada0753bf02ad02f3e53d6ddd",
                    "0x000000000000000000000000238a358808379702088667322f80ac48bad5e6c4",
                ],
                "removed": False,
                "id": "log_0x392df770fd46ed80400cf1adf2d9aab4b1e0e2817728ffbbf2af5343fbd1c521_118",
                "ingestTimestamp": "1777696811193",
            }
        ]
    }

    decoded = asyncio.run(decoder.decode(bundle))
    transfers = decoded["token_transfer"]

    assert len(transfers) == 1
    assert transfers[0]["transfer_type"] == "erc20"
    assert transfers[0]["source_log_id"] == "log_0x392df770fd46ed80400cf1adf2d9aab4b1e0e2817728ffbbf2af5343fbd1c521_118"
    assert transfers[0]["id"] == "token_transfer_0x392df770fd46ed80400cf1adf2d9aab4b1e0e2817728ffbbf2af5343fbd1c521_118_0"
    assert transfers[0]["amount"] == "31564245107957729075"
