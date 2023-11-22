import uuid

import anyio
import pytest
from anyio.abc import TaskStatus
from serena.connection import open_connection
from serena.enums import ExchangeType, ReplyCode
from serena.exc import UnexpectedCloseError

pytestmark = pytest.mark.anyio

test_suffix = uuid.uuid4()


async def test_queue_declaration():
    """
    Tests basic queue declaration.
    """

    async with open_connection("127.0.0.1") as conn, conn.open_channel() as channel:
        declared = await channel.queue_declare(name=f"queue-{test_suffix}")
        assert declared.name == f"queue-{test_suffix}"
        assert declared.message_count == 0


async def test_queue_exclusive_declaration():
    """
    Tests exclusive declaration.
    """

    async with open_connection("127.0.0.1") as conn, conn.open_channel() as channel:
        declared = await channel.queue_declare(name=f"queue-ex-{test_suffix}", exclusive=True)

    async with open_connection("127.0.0.1") as conn:
        with pytest.raises(UnexpectedCloseError) as e:
            async with conn.open_channel() as channel:
                await channel.queue_declare(declared.name, passive=True)

        assert conn.open
        assert e.value.reply_code == ReplyCode.not_found


async def test_queue_delete():
    """
    Tests deleting queues.
    """

    async with open_connection("127.0.0.1") as conn:
        with pytest.raises(UnexpectedCloseError) as e:
            async with conn.open_channel() as channel:
                declared = await channel.queue_declare("", exclusive=True)
                result = await channel.queue_delete(declared.name)
                assert result == 0
                await channel.queue_declare(declared.name, passive=True)

        assert e.value.reply_code == ReplyCode.not_found

        async with conn.open_channel() as channel:
            declared = await channel.queue_declare("", exclusive=True)
            await channel.basic_publish("", routing_key=declared.name, body=b"")
            result = await channel.queue_delete(declared.name)
            assert result == 1


async def test_delete_not_empty():
    """
    Tests deleting a non-empty queue.
    """

    async with open_connection("127.0.0.1") as conn:
        with pytest.raises(UnexpectedCloseError) as e:
            async with conn.open_channel() as channel:
                declared = await channel.queue_declare("", exclusive=True)
                await channel.basic_publish("", routing_key=declared.name, body=b"test")
                await channel.queue_delete(declared.name, if_empty=True)

        assert e.value.reply_code == ReplyCode.precondition_failed


async def test_delete_in_use():
    """
    Tests deleting a queue in use.
    """

    async def consumer(channel, queue_name, *, task_status: TaskStatus):
        async with channel.basic_consume(queue_name):
            task_status.started()
            await anyio.sleep_forever()

    async with open_connection("127.0.0.1") as conn:
        with pytest.raises(ExceptionGroup) as e:
            async with conn.open_channel() as channel, anyio.create_task_group() as group:
                queue = await channel.queue_declare(exclusive=True)
                await group.start(consumer, channel, queue.name)
                await channel.queue_delete(queue.name, if_unused=True)

        assert isinstance(unwrapped := e.value.exceptions[0], UnexpectedCloseError)
        assert unwrapped.reply_code == ReplyCode.precondition_failed


async def test_queue_purge():
    """
    Tests purging a queue.
    """

    async with open_connection("127.0.0.1") as conn, conn.open_channel() as channel:
        queue = await channel.queue_declare(exclusive=True)
        assert (await channel.queue_purge(queue.name)) == 0

        await channel.basic_publish("", routing_key=queue.name, body=b"")
        assert (await channel.queue_purge(queue.name)) == 1


async def test_queue_bind():
    """
    Tests binding a queue.
    """

    async with open_connection("127.0.0.1") as conn, conn.open_channel() as channel:
        exchange_name = await channel.exchange_declare(
            f"test-ex-{test_suffix}", ExchangeType.DIRECT, auto_delete=True
        )
        queue = await channel.queue_declare(name="", exclusive=True)
        await channel.queue_bind(queue.name, exchange_name, routing_key="")

        await channel.basic_publish(exchange_name, routing_key="", body=b"test")
        message = await channel.basic_get(queue.name)
        assert message.body == b"test"
