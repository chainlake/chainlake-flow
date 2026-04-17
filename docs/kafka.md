Phase 1: JSON (dev / MVP)
Phase 2: JSON + compression (lz4)
Phase 3: AVRO + registry
Phase 4: Protobuf streaming (advanced)


kafka-configs --alter \
  --entity-type topics \
  --entity-name your_topic \
  --add-config message.timestamp.type=LogAppendTime