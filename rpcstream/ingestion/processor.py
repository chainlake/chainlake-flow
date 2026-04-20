from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions
from rpcstream.adapters.evm.parser.parse_receipts_logs import parse_receipts
from rpcstream.adapters.evm.parser.parse_traces import parse_traces_auto

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

# Create a tracer object
tracer = trace.get_tracer(__name__)

class EVMProcessor:

    def __init__(self, logger=None):
        self.logger = logger

    def process(self, pipeline_type, block_number, value):
        # Start the parent span for the processing pipeline
        with tracer.start_as_current_span("processor.block_parse") as span:
            span.set_attribute("component", "processor")
            span.set_attribute("pipeline", pipeline_type)
            span.set_attribute("block_number", block_number)
            
            try:
                if pipeline_type == "block":
                    result = self.process_block_pipeline(block_number, value)

                elif pipeline_type == "receipt":
                    result = self.process_receipt_pipeline(block_number, value)

                elif pipeline_type == "trace":
                    result = self.process_trace_pipeline(block_number, value)

                else:
                    raise ValueError(f"Unknown pipeline: {pipeline_type}")

                # -------------------------
                # LOG AFTER PROCESSING
                # -------------------------
                if self.logger:
                    for entity, rows in result.items():
                        self.logger.debug(
                            "processor.success",
                            component="processor",
                            pipeline=pipeline_type,
                            entity=entity,
                            block=block_number,
                            count=len(rows)
                        )

                return result

            except Exception as e:
                # Log error and add it to the span
                if self.logger:
                    self.logger.error(
                        "processor.error",
                        component="processor",
                        pipeline=pipeline_type,
                        block=block_number,
                        error=str(e)
                    )
                span.set_status(Status(StatusCode.ERROR))
                span.set_attribute("error.message", str(e))
                raise

    # -------------------------
    # BLOCK PIPELINE
    # -------------------------
    def process_block_pipeline(self, block_number, value):
        # Perform block processing
        block = parse_blocks(value)
        txs = parse_transactions(value)

        result = {
            "block": [block],
            "transaction": txs
        }

        # Log success after processing
        if self.logger:
            for entity, rows in result.items():
                self.logger.debug(
                    "processor.success",
                    component="processor",
                    pipeline="block",
                    entity=entity,
                    block=block_number,
                    count=len(rows)
                )

        return result

    # -------------------------
    # RECEIPT PIPELINE
    # -------------------------
    def process_receipt_pipeline(self, block_number, value, parent_span):
        # Perform receipt processing
        receipts, logs = parse_receipts(value)

        result = {
            "receipt": receipts,
            "log": logs
        }

        # Log success after processing
        if self.logger:
            for entity, rows in result.items():
                self.logger.debug(
                    "processor.success",
                    component="processor",
                    pipeline="receipt",
                    entity=entity,
                    block=block_number,
                    count=len(rows)
                )

        return result

    # -------------------------
    # TRACE PIPELINE
    # -------------------------
    # -------------------------
    # TRACE PIPELINE
    # -------------------------
    def process_trace_pipeline(self, block_number, value, parent_span):
        # Perform trace processing
        traces = parse_traces_auto(value, block_number, "trace_block")

        result = {
            "trace": traces
        }

        # Log success after processing
        if self.logger:
            for entity, rows in result.items():
                self.logger.debug(
                    "processor.success",
                    component="processor",
                    pipeline="trace",
                    entity=entity,
                    block=block_number,
                    count=len(rows)
                )

        return result