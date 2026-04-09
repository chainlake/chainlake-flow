from rpcstream.adapters.evm.rpc_requests import batch_get_blocks_by_number,batch_get_block_receipts,batch_trace_blocks, build_get_block_by_number

START_BLOCK = 90000096
END_BLOCK = 90000100


req = build_get_block_by_number(START_BLOCK)
print(req)
print("-----------")
payloads_blocks = list(batch_get_blocks_by_number(START_BLOCK, END_BLOCK))

payloads_receipts = list(batch_get_block_receipts(START_BLOCK, END_BLOCK))

payloads_trace = list(batch_trace_blocks(START_BLOCK, END_BLOCK))


for p in payloads_blocks:
    print(p)
    
for req in payloads_receipts:
    print(req)

for p in payloads_trace:
    print(p)

# print(payload_blocks)

# import aiohttp
# import asyncio
# import json
# # total request = END_BLOCK - START_BLOCK + 1 batch itself
# async def main():
#     # payloads = list(batch_get_blocks(range(START_BLOCK, END_BLOCK),include_transactions=False))
#     payload = build_get_block_by_number(START_BLOCK, False, START_BLOCK)
#     async with aiohttp.ClientSession() as session:
#         async with session.post(
#             "http://localhost:30040/main/evm/56",
#             json=payload,
#         ) as resp:
#             data = await resp.json()
#             print(json.dumps(data, indent=2))

# asyncio.run(main())

