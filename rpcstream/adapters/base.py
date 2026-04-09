from typing import Any, Dict, List


class BaseRpcRequest:
    """
    Base class for a generic RPC request.
    Chain-specific implementations (EVM, SUI, SOL, etc.) should inherit from this class.
    """

    def __init__(
        self,
        method: str = None,
        params: List[Any] = None,
        request_id: Any = None,
        meta: Dict = None,
        stub_method: str = None,
        payload: Any = None,
    ):
        self.method = method
        self.params = params or []
        self.request_id = request_id
        self.meta = meta or {}

        self.stub_method = stub_method
        self.payload = payload

    def __repr__(self):
        """
        Pretty print request content.
        """
        return (
            f"{self.__class__.__name__}("
            f"method={self.method}, "
            f"params={self.params}, "
            f"request_id={self.request_id}, "
            f"meta={self.meta})"
        )

    def operation_name(self) -> str:
        """
        Unified request name for tracing / scheduler telemetry.
        """
        return (
            self.method
            or self.stub_method
            or self.__class__.__name__
        )

    def transport_type(self) -> str:
        """
        Default transport type.
        Override in chain-specific requests if needed.
        """
        return "jsonrpc"