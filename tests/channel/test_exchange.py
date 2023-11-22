import uuid

import pytest
from serena.connection import open_connection
from serena.enums import ExchangeType, ReplyCode
from serena.exc import UnexpectedCloseError

pytestmark = pytest.mark.anyio

test_suffix = uuid.uuid4()


async def test_ex_basic_declaration():
    """
    Tests basic exchange declaration of all types.
    """

    async with open_connection("127.0.0.1") as conn, conn.open_channel() as channel:
        # no error means that declaration succeeded
        for type in ExchangeType:
            await channel.exchange_declare(name=f"{type}-{test_suffix}", type=type, durable=False)


async def test_ex_declaration_invalid_type():
    """
    Tests declaration with an invalid type.
    """

    with pytest.raises(UnexpectedCloseError) as e:
        async with open_connection("127.0.0.1") as conn:
            async with conn.open_channel() as channel:
                await channel.exchange_declare(name="invalid", type="invalid")

    assert e.value.reply_code == ReplyCode.command_invalid


async def test_ex_delete():
    """
    Tests deleting an exchange.
    """

    async with open_connection("127.0.0.1") as conn, conn.open_channel() as channel:
        name = f"delete-{test_suffix}"

        await channel.exchange_declare(name=name, type=ExchangeType.DIRECT)
        await channel.exchange_delete(name=name)

        # XXX: RabbitMQ doesn't raise a not_found. This is incorrect according to the spec.
        # with pytest.raises(UnexpectedCloseError) as e:
        #     async with conn.open_channel() as channel:
        #         await channel.exchange_delete(name="does not exist")
        #
        # assert e.value.reply_code == ReplyCode.not_found


async def test_ex_binding():
    """
    Tests binding an array to another array.
    """

    exchange_name = f"ex-bind-1-{test_suffix}"
    ex_2 = f"ex-bind-2-{test_suffix}"

    async with open_connection("127.0.0.1") as conn:
        if not conn._is_rabbitmq:
            pytest.skip("rabbitmq-specific extension")

        async with conn.open_channel() as channel:
            await channel.exchange_declare(exchange_name, ExchangeType.DIRECT)
            await channel.exchange_declare(ex_2, ExchangeType.DIRECT)
            await channel.exchange_bind(exchange_name, ex_2, routing_key="")

            queue = await channel.queue_declare("", exclusive=True)
            await channel.queue_bind(queue.name, exchange_name, routing_key="")

            # rabbitmq should forward messages from ex_2 to ex_1 then to the queue
            await channel.basic_publish(ex_2, routing_key="", body=b"test")

            message = await channel.basic_get(queue.name)
            assert message is not None
            assert message.body == b"test"
