from dataclasses import dataclass
from opentelemetry import trace

@dataclass
class ObservabilityContext:
    tracer: any
    logger: any
    trace_id: str | None = None

    def start_span(self, name: str):
        return self.tracer.start_as_current_span(name)
    
    def trace_id(self, span):
        ctx = span.get_span_context()
        return format(ctx.trace_id, "032x")