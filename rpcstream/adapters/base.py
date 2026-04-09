from typing import Any, Dict, List


class BaseRpcRequest:
    """
    Base class for a generic RPC request.
    Chain-specific implementations (EVM, SUI, SOL, etc.) should inherit from this class.
    """

    def __init__(
        self,
        method: str,
        params: List[Any],
        request_id: Any = None,
        meta: Dict = None,
    ):
        self.method = method
        self.params = params
        self.request_id = request_id
        self.meta = meta or {}

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
        return getattr(self, "method", self.__class__.__name__)

    def transport_type(self) -> str:
        """
        Default transport type.
        Override in chain-specific requests if needed.
        """
        return "jsonrpc"