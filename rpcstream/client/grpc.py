import grpc

from rpcstream.client.base import BaseClient


class GrpcClient(BaseClient):
    """
    Generic gRPC transport client.
    """

    def __init__(
        self,
        base_url: str,
        stub_class,
        timeout_sec: int = 10,
        max_retries: int = 2,
        secure: bool = False,
    ):
        super().__init__(base_url, max_retries=max_retries)

        self.timeout_sec = timeout_sec
        self.stub_class = stub_class

        if secure:
            self.channel = grpc.aio.secure_channel(
                base_url,
                grpc.ssl_channel_credentials(),
            )
        else:
            self.channel = grpc.aio.insecure_channel(base_url)

        self.stub = stub_class(self.channel)

    async def _execute(self, request, span):
        method = getattr(self.stub, request.stub_method)

        span.set_attribute("rpc.method", request.stub_method)

        response = await method(
            request.payload,
            timeout=self.timeout_sec,
        )

        return response

    async def close(self):
        await self.channel.close()