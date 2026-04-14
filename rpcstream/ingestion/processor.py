from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions

class EVMProcessor:

    def process(self, block_number, result):
        value, meta = result
      
        block_row = parse_blocks(value)
        tx_rows = parse_transactions(value)

        return {
            "blocks": [block_row],
            "transactions": tx_rows
        }