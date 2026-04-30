from __future__ import annotations

from rpcstream.adapters.base import ChainAdapter


class SuiChainAdapter(ChainAdapter):
    def build_entity_schemas(self):
        raise NotImplementedError("Sui chain adapter protobuf schemas are not implemented yet")

    def build_event_id_calculator(self):
        raise NotImplementedError("Sui chain adapter event id calculator is not implemented yet")

    def build_event_time_calculator(self):
        raise NotImplementedError("Sui chain adapter event time calculator is not implemented yet")

    def build_tracker(self, *, client, poll_interval: float, logger=None):
        raise NotImplementedError("Sui chain adapter tracker is not implemented yet")

    def build_fetcher(self, *, scheduler, entities, logger=None, tracker=None):
        raise NotImplementedError("Sui chain adapter fetcher is not implemented yet")

    def build_processors(self, *, entities):
        raise NotImplementedError("Sui chain adapter processors are not implemented yet")

    def build_enricher(self):
        raise NotImplementedError("Sui chain adapter enricher is not implemented yet")
