from __future__ import annotations

from abc import ABC, abstractmethod

from rpcstream.sinks.kafka.schema import EntitySchema, build_topic_schemas


class ChainAdapter(ABC):
    def resolve_internal_entities(self, requested_entities):
        return [entity.strip().lower() for entity in requested_entities]

    def resolve_sink_entities(self, requested_entities):
        return [entity.strip().lower() for entity in requested_entities]

    def topic_kind_for_entity(self, entity: str) -> str:
        return "raw"

    def build_entity_schemas(self) -> dict[str, EntitySchema]:
        return {}

    def build_protobuf_topic_schemas(self, *, topic_maps, entities: list[str]) -> dict[str, EntitySchema]:
        return build_topic_schemas(
            topic_maps=topic_maps,
            entity_schemas=self.build_entity_schemas(),
            entities=entities,
        )

    @abstractmethod
    def build_tracker(self, *, client, poll_interval: float, logger=None):
        raise NotImplementedError

    @abstractmethod
    def build_fetcher(self, *, scheduler, entities, logger=None, tracker=None):
        raise NotImplementedError

    @abstractmethod
    def build_processors(self, *, entities):
        raise NotImplementedError

    @abstractmethod
    def build_enricher(self):
        raise NotImplementedError

    @abstractmethod
    def build_event_id_calculator(self):
        raise NotImplementedError

    @abstractmethod
    def build_event_time_calculator(self):
        raise NotImplementedError
