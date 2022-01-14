import uuid

import pytest

from serena.connection import open_connection
from serena.enums import ReplyCode
from serena.exc import UnexpectedCloseError

pytestmark = pytest.mark.anyio

test_suffix = uuid.uuid4()


async def test_queue_declaration():
    """
    Tests basic queue declaration.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            declared = await channel.queue_declare(name=f"queue-{test_suffix}")
            assert declared.name == f"queue-{test_suffix}"
            assert declared.message_count == 0


async def test_queue_exclusive_declaration():
    """
    Tests exclusive declaration.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            declared = await channel.queue_declare(name=f"queue-ex-{test_suffix}", exclusive=True)

    async with open_connection("127.0.0.1") as conn:
        with pytest.raises(UnexpectedCloseError) as e:
            async with conn.open_channel() as channel:
                await channel.queue_declare(declared.name, passive=True)

        assert conn.open
        assert e.value.reply_code == ReplyCode.not_found
