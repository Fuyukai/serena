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

            result = await channel.basic_get(queue.name)
            assert result is None
            await channel.basic_publish("", routing_key=queue.name, body=b"test")
            result = await channel.basic_get(queue.name)

            assert result is not None
            await result.ack()  # satisfy rabbitmq
            assert result.body == b"test"


async def test_consumption():
    """
    Tests consuming asynchronously.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            queue = await channel.queue_declare(name="", exclusive=True)

            counter = 0
            for i in range(0, 10):
                await channel.basic_publish("", routing_key=queue.name, body=b"test")
                counter += 1

            messages = []
            queue = await channel.queue_declare(name=queue.name, passive=True)
            assert queue.message_count == 10

            async with channel.basic_consume(queue_name=queue.name) as agen:
                async for message in agen:
                    messages.append(message)
                    counter -= 1
                    if counter <= 0:
                        break

            queue = await channel.queue_declare(name=queue.name, passive=True)
            assert queue.message_count == 0
            assert len(messages) == 10


async def test_acks():
    """
    Tests message acknowledgement.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            queue = await channel.queue_declare(name="", exclusive=True)
            await channel.basic_publish("", routing_key=queue.name, body=b"test")

            # very cool amqp feature is that reject() will just get the server to immediately
            # requeue it.
            msg = await channel.basic_get(queue.name)
            assert msg is not None
            await msg.reject(requeue=True)

            assert (await channel.queue_declare(name=queue.name, passive=True)).message_count == 1
            msg = await channel.basic_get(queue.name)
            await msg.ack()
            assert (await channel.queue_declare(name=queue.name, passive=True)).message_count == 0
