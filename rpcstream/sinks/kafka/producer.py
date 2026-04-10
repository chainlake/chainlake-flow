import json
import asyncio
from aiokafka import AIOKafkaProducer

class KafkaWriter:
    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str = "chainlake",
        enable_eos: bool = True
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.enable_eos = enable_eos

        self.producer = None
        self.transactional_id = f"{client_id}-txn"
        
        
    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            enable_idempotence=True,
            acks="all",
            linger_ms=5,
            compression_type="lz4",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        await self.producer.start()
 
        
    async def send(self, topic: str, key: str, value: dict):
        await self.producer.send_and_wait(
            topic,
            value=value,
            key=str(key).encode("utf-8")
        )
        
    async def send_batch(self, topic: str, records: list, key_field: str):
        for r in records:
            key = r.get(key_field, "0")
            await self.send(topic, key, r)
            
    
    async def write_checkpoint(self, pipeline_id: str, block_number: int):
        await self.send(
            topic="evm.checkpoints",
            key=pipeline_id,
            value={
                "pipeline_id": pipeline_id,
                "block_number": block_number,
                "timestamp": asyncio.get_event_loop().time()
            }
        )
        
    async def stop(self):
        if self.producer:
            await self.producer.stop()