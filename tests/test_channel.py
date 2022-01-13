import pytest

from serena.connection import open_connection

pytestmark = pytest.mark.anyio


async def test_channel_opening():
    """
    Tests opening a channel.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            assert channel.open

        assert not channel.open


async def test_basic_publish():
    """
    Tests publishing a message to a queue, and getting the message back.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            queue = await channel.queue_declare(name="", exclusive=True)

            result = await channel.basic_get(queue)
            assert result is None
            await channel.basic_publish("", routing_key=queue, body=b"test")
            result = await channel.basic_get(queue)

            assert result is not None
            await result.ack()  # satisfy rabbitmq
            assert result.body == b"test"
