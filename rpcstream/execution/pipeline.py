class ExecutionEngine:

    def __init__(self, kafka_writer):
        self.kafka = kafka_writer
        
        
    async def execute_and_sink(self, pipeline, block_number, request):
      response = await pipeline.client.execute(request)

      data = pipeline.transformer(response)

      # FANOUT
      for topic, rows in data.items():
          await self.kafka.send_batch(
              topic=f"evm.{topic}",
              records=rows,
              key_field="block_number"
          )

      # checkpoint
      await self.kafka.write_checkpoint(
          pipeline_id=pipeline.name,
          block_number=block_number
      )