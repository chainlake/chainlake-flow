   
import asyncio
import grpc

from rpcstream.client.sui_pb2 import ledger_service_pb2, ledger_service_pb2_grpc

BLOCKPI_TOKEN = "2f1663f004ba0d418ed53957b5ebf54128c870ab"

async def main():
    # 1. 建立 gRPC 安全通道
    creds = grpc.ssl_channel_credentials()
    async with grpc.aio.secure_channel("sui.blockpi.network:443", creds) as channel:

        # 2. 创建 LedgerService Stub
        stub = ledger_service_pb2_grpc.LedgerServiceStub(channel)

        # 3. 构造请求
        request = ledger_service_pb2.GetCheckpointRequest(
            # 这里可以传 checkpoint_id 或者空
            checkpoint_id=""
        )

        # 4. 调用 gRPC 方法
        # 添加 header 用 token
        call_credentials = grpc.metadata_call_credentials(
            lambda context, callback: callback([("x-token", BLOCKPI_TOKEN)], None)
        )
        composite_creds = grpc.composite_channel_credentials(creds, call_credentials)

        async with grpc.aio.secure_channel("sui.blockpi.network:443", composite_creds) as secure_channel:
            stub = ledger_service_pb2_grpc.LedgerServiceStub(secure_channel)
            response = await stub.GetCheckpoint(request)
            print(response)

if __name__ == "__main__":
    asyncio.run(main())