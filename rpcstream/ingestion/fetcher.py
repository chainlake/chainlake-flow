from dataclasses import dataclass
from rpcstream.adapters.evm.rpc_requests import build_get_block_by_number
from rpcstream.adapters.evm.rpc_requests import build_get_block_receipts
from rpcstream.adapters.evm.rpc_requests import build_debug_trace_block
from opentelemetry import trace

tracer = trace.get_tracer(__name__)


class RpcFetcher:
    def __init__(self, scheduler, pipeline_type, logger=None, tracker=None):
        self.scheduler = scheduler
        self.pipeline_type = pipeline_type
        self.logger = logger
        self.tracker = tracker


    async def fetch(self, block_number):
        with tracer.start_as_current_span("fetcher.fetch") as span:
            span.set_attribute("component", "fetcher")
            span.set_attribute("pipeline", self.pipeline_type)
            span.set_attribute("block_number", block_number)

            # -------------------------
            # BUILD REQUEST
            # -------------------------
            if self.pipeline_type == "block":
                req = build_get_block_by_number(block_number, True)

            elif self.pipeline_type == "receipt":
                req = build_get_block_receipts(block_number)

            elif self.pipeline_type == "trace":
                req = build_debug_trace_block(block_number)

            else:
                raise ValueError(f"Unknown pipeline: {self.pipeline_type}")

            span.set_attribute("rpc.method", req.method)

            # -------------------------
            # LOG BEFORE
            # -------------------------
            if self.logger:
                self.logger.debug(
                    "fetcher.request",
                    component="fetcher",
                    method=req.method,
                    block=block_number,
                    pipeline=self.pipeline_type,
                )

            # -------------------------
            # EXECUTE (delegates to client span)
            # -------------------------
            result = await self.scheduler.submit_request(req)

            # -------------------------
            # LOG AFTER
            # -------------------------
            if self.logger:
                self.logger.debug(
                    "fetcher.response",
                    component="fetcher",
                    method=req.method,
                    block=block_number,
                    pipeline=self.pipeline_type,
                )

            return result