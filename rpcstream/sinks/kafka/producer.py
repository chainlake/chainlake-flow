import asyncio
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


class KafkaWriter:
    def __init__(
        self,
        topic: str,
        protobuf_cls,
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
        queue_size=100_000,
        batch_size=1000,
        linger_ms=5,
    ):
        self.topic = topic
        self.batch_size = batch_size

        # --- Schema Registry ---
        self.schema_registry_client = SchemaRegistryClient({
            "url": schema_registry_url
        })

        # --- Protobuf Serializer ---
        self.value_serializer = ProtobufSerializer(
            protobuf_cls,
            self.schema_registry_client
        )

        # --- Producer ---
        self.producer = SerializingProducer({
            "bootstrap.servers": bootstrap_servers,
            "value.serializer": self.value_serializer,
            "linger.ms": linger_ms,
            "batch.num.messages": batch_size,
            "compression.type": "zstd",
            "enable.idempotence": True,
            "acks": "all",
        })

        # --- Async Queue ---
        self.queue = asyncio.Queue(maxsize=queue_size)

        self._running = False

    # --- enqueue (non-blocking API) ---
    async def write(self, key: str, message):
        await self.queue.put((key, message))

    # --- internal batch sender ---
    async def _sender_loop(self):
        loop = asyncio.get_running_loop()

        while self._running:
            batch = []

            try:
                # 等一个消息
                item = await self.queue.get()
                batch.append(item)

                # 尽量攒 batch
                for _ in range(self.batch_size - 1):
                    try:
                        item = self.queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

                # 发送（非阻塞）
                for key, msg in batch:
                    self.producer.produce(
                        topic=self.topic,
                        key=key,
                        value=msg,
                        on_delivery=self._delivery_callback
                    )

                # poll 触发 callback（必须）
                self.producer.poll(0)

            except Exception as e:
                print("Kafka send error:", e)

            # 控制 event loop
            await asyncio.sleep(0)

    def _delivery_callback(self, err, msg):
        if err:
            print(f"Delivery failed: {err}")

    # --- start background worker ---
    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._sender_loop())

    # --- graceful shutdown ---
    async def stop(self):
        self._running = False
        await self._task
        self.producer.flush()