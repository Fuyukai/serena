import uuid

import pytest

from serena.connection import open_connection
from serena.enums import ExchangeType, ReplyCode
from serena.exc import UnexpectedCloseError

pytestmark = pytest.mark.anyio

test_prefix = uuid.uuid4()


async def test_ex_basic_declaration():
    """
    Tests basic exchange declaration of all types.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            # no error means that declaration succeeded
            for type in ExchangeType:
                await channel.exchange_declare(
                    name=f"{type}-{test_prefix}", type=type, durable=False
                )


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

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            name = f"delete-{test_prefix}"

            await channel.exchange_declare(name=name, type=ExchangeType.DIRECT)
            await channel.exchange_delete(name=name)

        # XXX: RabbitMQ doesn't raise a not_found. This is incorrect according to the spec.
        # with pytest.raises(UnexpectedCloseError) as e:
        #     async with conn.open_channel() as channel:
        #         await channel.exchange_delete(name="does not exist")
        #
        # assert e.value.reply_code == ReplyCode.not_found
