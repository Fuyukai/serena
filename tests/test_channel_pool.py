import uuid

import anyio
import pytest
from serena import UnexpectedCloseError, open_connection

pytestmark = pytest.mark.anyio

test_suffix = uuid.uuid4()


async def test_channel_pool():
    """
    Tests opening and using the channel pool.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel_pool(initial_channels=2) as pool:
            assert pool.idle_channels == 2

            await pool.queue_declare(name="", exclusive=True)

        async with conn.open_channel() as channel:
            assert channel.id == 1


async def test_channel_pool_consume():
    """
    Tests channel pool consumption.
    """

    async with (
        open_connection("127.0.0.1") as conn,
        conn.open_channel_pool(initial_channels=2) as pool,
    ):
        queue = await pool.queue_declare(name=f"test-cc-{test_suffix}", auto_delete=True)
        await pool.basic_publish("", routing_key=queue.name, body=b"test")

        async with pool.basic_consume(queue.name) as it:
            async for item in it:
                assert item.body == b"test"
                break

        assert pool.idle_channels == 2


@pytest.mark.slow
async def test_channel_pool_errors():
    """
    Tests handling errors in the channel pool.
    """

    async with (
        open_connection("127.0.0.1") as conn,
        conn.open_channel_pool(initial_channels=2) as pool,
    ):
        with pytest.raises(UnexpectedCloseError):
            await pool.basic_publish("abcdef", routing_key="", body=b"")

        # kinda racey but it's consistent enough in my experience
        assert pool.idle_channels == 1
        # random sleep to make sure the background worker has time to enqueue the next channel
        await anyio.sleep(2)
        assert pool.idle_channels == 2
