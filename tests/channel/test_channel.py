import pytest
from serena.enums import ReplyCode
from serena.exc import UnexpectedCloseError

from tests import _open_connection

pytestmark = pytest.mark.anyio


async def test_channel_opening():
    """
    Tests opening a channel.
    """

    async with _open_connection() as conn:
        async with conn.open_channel() as channel:
            assert channel.open

        assert not channel.open


async def test_channel_server_side_close():
    async with _open_connection() as conn:
        with pytest.raises(UnexpectedCloseError) as e:
            async with conn.open_channel() as channel:
                await channel.basic_get("non-existing-queue")

        assert e.value.reply_code == ReplyCode.not_found


async def test_reusing_channel_id():
    """
    Tests reusing channel IDs.
    """

    async with _open_connection() as conn:
        async with conn.open_channel() as channel:
            assert channel.id == 1

        await channel.wait_until_closed()

        async with conn.open_channel() as channel:
            assert channel.id == 1
