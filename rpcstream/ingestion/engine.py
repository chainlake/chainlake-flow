class IngestionEngine:
    def __init__(self, fetcher, processor, sink, topics):
        self.fetcher = fetcher
        self.processor = processor
        self.sink = sink
        self.topics = topics

    async def run_batch(self, start, end):
        for block_number in range(start, end + 1):
            await self._run_one(block_number)

        self.sink.flush()

    async def _run_one(self, block_number):
        value = await self.fetcher.get_block(block_number)

        result = self.processor.process(block_number, value)

        self.sink.send(self.topics["blocks"], result["blocks"])
        self.sink.send(self.topics["transactions"], result["transactions"])