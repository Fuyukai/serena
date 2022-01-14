import pytest

from serena.connection import open_connection
from serena.enums import ReplyCode
from serena.exc import UnexpectedCloseError


async def test_channel_opening():
    """
    Tests opening a channel.
    """

    async with open_connection("127.0.0.1") as conn:
        async with conn.open_channel() as channel:
            assert channel.open

        assert not channel.open


async def test_channel_server_side_close():
    async with open_connection("127.0.0.1") as conn:
        with pytest.raises(UnexpectedCloseError) as e:
            async with conn.open_channel() as channel:
                await channel.basic_get("non-existing-queue")

        assert e.value.reply_code == ReplyCode.not_found


