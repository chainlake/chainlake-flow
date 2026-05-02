# End-to-end latency

1. Chain latency
   `ingest_timestamp - event_timestamp`

2. Network + Kafka latency
   `kafka_timestamp(LogAppendTime) - ingest_timestamp`

3. End-to-end latency
   `kafka_timestamp(LogAppendTime) - event_timestamp`
