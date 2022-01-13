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
