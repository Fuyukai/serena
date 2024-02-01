import anyio
import pytest

from tests import _open_connection

pytestmark = pytest.mark.anyio


@pytest.mark.slow
async def test_cancelling_outer_conn_doesnt_cause_hang():
    async def _bad_task():
        async with (
            _open_connection() as conn,
            conn.open_channel_pool() as pool,
            pool.checkout() as _,
        ):
            # keep
            await anyio.sleep_forever()

    async with anyio.create_task_group() as tg:
        tg.start_soon(_bad_task)

        # give it a chance to open
        await anyio.sleep(1)
        tg.cancel_scope.cancel()

    assert True, "what?"
