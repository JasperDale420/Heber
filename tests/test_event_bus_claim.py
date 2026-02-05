import asyncio
import json
from unittest.mock import AsyncMock

from heber.bus import ConsumerConfig, RedisEventBus, StreamName


def test_consume_yields_claimed_messages():
    async def run_test():
        bus = RedisEventBus()
        bus._redis = AsyncMock()

        config = ConsumerConfig(
            group_name="test-group",
            consumer_name="test-consumer",
            stream=StreamName.MARKET_BARS,
            batch_size=10,
            block_ms=1,
            claim_idle_ms=1,
        )

        bus._redis.xpending_range.return_value = [
            {"message_id": "1-0", "idle": 1000},
        ]
        bus._redis.xclaim.return_value = [
            ("1-0", {"payload": json.dumps({"a": 1})}),
        ]
        bus._redis.xreadgroup.return_value = []

        gen = bus.consume(config)
        batch = await asyncio.wait_for(gen.__anext__(), timeout=1)
        await gen.aclose()

        return batch

    batch = asyncio.run(run_test())

    assert len(batch) == 1
    assert batch[0].id == "1-0"
    assert batch[0].stream == StreamName.MARKET_BARS.value
    assert batch[0].data["payload"] == {"a": 1}
