from rpcstream.adapters.base import BaseRpcRequest
from rpcstream.generated.sui import node_service_pb2


class SuiGrpcRequest(BaseRpcRequest):
    pass


def build_checkpoint(sequence_number: int):
    return SuiGrpcRequest(
        stub_method="GetCheckpoint",
        payload=node_service_pb2.GetCheckpointRequest(
            sequence_number=sequence_number
        ),
        request_id=sequence_number,
        meta={
            "rpc": "checkpoint"
        },
    )