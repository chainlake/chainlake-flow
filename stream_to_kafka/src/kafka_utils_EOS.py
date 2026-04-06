from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from src.logging import log

# -----------------------------
# delivery report for producer callback
# -----------------------------
def delivery_report(err, msg):
    if err:
        log.error(
            "🔥kafka_delivery_failed",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "error": str(err),
            },
        )

def init_producer(TRANSACTIONAL_ID, KAFKA_BROKER):
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        
        # Exactly-once / Transactions
        "enable.idempotence": True,
        "acks": "all",
        "transactional.id": TRANSACTIONAL_ID, # 在整个 Kafka 集群（cluster）范围内唯一
        
        # retry
        "retries": 1000000,
        "max.in.flight.requests.per.connection": 5,
        
        # Throughput tuning
        "linger.ms": 50,
        "batch.size": 524288,          # 512KB
        "compression.type": "lz4",
        
        # Timeouts(avoid Erroneous state)
        "request.timeout.ms": 60000,
        "delivery.timeout.ms": 120000,
        "transaction.timeout.ms": 600000,  # 10 min
        
        # Backpressure protection
        "queue.buffering.max.kbytes": 1048576,  # 1GB
        "queue.buffering.max.messages": 1000000,
        
        # Socket stability
        "socket.keepalive.enable": True,
    })
    
    log.info(
        "Kafka_initializing",
        extra={
            "bootstrap_servers": KAFKA_BROKER,
            "transactional_id": TRANSACTIONAL_ID,
        },
    )
    producer.init_transactions()
    return producer

def get_serializers(SCHEMA_REGISTRY_URL, BLOCKS_TOPIC, STATE_TOPIC, LOGS_TOPIC, TXS_TOPIC):
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    blocks_value_schema = schema_registry.get_latest_version(f"{BLOCKS_TOPIC}-value").schema.schema_str
    state_value_schema = schema_registry.get_latest_version(f"{STATE_TOPIC}-value").schema.schema_str
    logs_value_schema = schema_registry.get_latest_version(f"{LOGS_TOPIC}-value").schema.schema_str
    txs_value_schema = schema_registry.get_latest_version(f"{TXS_TOPIC}-value").schema.schema_str
    return (
        AvroSerializer(schema_registry, blocks_value_schema),
        AvroSerializer(schema_registry, state_value_schema),
        AvroSerializer(schema_registry, logs_value_schema),
        AvroSerializer(schema_registry, txs_value_schema)
    )
