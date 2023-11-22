import pytest
from anyio import sleep
from serena.connection import open_connection
from serena.enums import ReplyCode
from serena.exc import UnexpectedCloseError

pytestmark = pytest.mark.anyio


async def test_connection_opening():
    """
    Tests the basics of opening and closing a connection.
    """

    async with open_connection("127.0.0.1") as conn:
        assert conn.open

    assert not conn.open


@pytest.mark.slow
async def test_heartbeat_intervals():
    """
    Tests heartbeat intervals.
    """

    async with open_connection("127.0.0.1", heartbeat_interval=2) as conn:
        await sleep(4)

    assert conn.heartbeat_statistics().heartbeat_count > 2


async def test_bad_virtual_host():
    """
    Tests a bad virtual host.
    """

    with pytest.raises(UnexpectedCloseError) as e:
        async with open_connection("127.0.0.1", virtual_host="/does_not_exist"):
            pass

    assert e.value.reply_code == ReplyCode.not_allowed


async def test_bad_authentication():
    """
    Tests bad authentication.
    """

    with pytest.raises(UnexpectedCloseError) as e:
        async with open_connection("127.0.0.1", password="not the right password!"):
            pass

    assert e.value.reply_code == ReplyCode.access_refused


async def test_server_side_close():
    """
    Tests a server-side close.
    """

    with pytest.raises(UnexpectedCloseError):
        async with open_connection("127.0.0.1") as conn:
            async with conn.open_channel() as channel:
                # immediate causes a close with rabbitmq
                await channel.basic_publish("", "", b"", immediate=True)
