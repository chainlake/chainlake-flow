from __future__ import annotations

from rpcstream.adapters.base import ChainAdapter
from rpcstream.adapters.evm.dag import resolve_internal_entities
from rpcstream.adapters.evm.dag import resolve_sink_entities
from rpcstream.adapters.evm.dag import topic_kind_for_entity
from rpcstream.adapters.evm.enrich import EvmEnricher
from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator
from rpcstream.adapters.evm.fetcher import EvmRpcFetcher
from rpcstream.adapters.evm.processors import PROCESSOR_REGISTRY
from rpcstream.adapters.evm.schema import EVM_ENTITY_SCHEMAS
from rpcstream.adapters.evm.tracker import EvmChainHeadTracker


class EvmChainAdapter(ChainAdapter):
    def resolve_internal_entities(self, requested_entities):
        return resolve_internal_entities(requested_entities)

    def resolve_sink_entities(self, requested_entities):
        return resolve_sink_entities(requested_entities)

    def topic_kind_for_entity(self, entity: str) -> str:
        return topic_kind_for_entity(entity)

    def build_tracker(self, *, client, poll_interval: float, logger=None):
        return EvmChainHeadTracker(
            client=client,
            poll_interval=poll_interval,
            logger=logger,
        )

    def build_fetcher(self, *, scheduler, entities, logger=None, tracker=None):
        return EvmRpcFetcher(scheduler, entities, logger, tracker)

    def build_processors(self, *, entities):
        return {entity: PROCESSOR_REGISTRY[entity] for entity in entities}

    def build_enricher(self):
        return EvmEnricher()

    def build_entity_schemas(self):
        return EVM_ENTITY_SCHEMAS

    def build_event_id_calculator(self):
        return EventIdCalculator()

    def build_event_time_calculator(self):
        return EventTimeCalculator()
