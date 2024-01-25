import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import serena
from serena.connection import AMQPConnection

# Global variables to store AMQP connection details
AMQP_HOST = os.environ.get("AMQP_HOST", "127.0.0.1")
AMQP_PORT = int(os.environ.get("AMQP_PORT", 5672))
AMQP_USERNAME = os.environ.get("AMQP_USERNAME", "guest")
AMQP_PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")


# pyright complains about this being unused, but it isn't.
@asynccontextmanager
async def _open_connection(**kwargs: Any) -> AsyncGenerator[AMQPConnection, None]:  # type: ignore
    async with serena.open_connection(
        AMQP_HOST,
        port=AMQP_PORT,
        username=AMQP_USERNAME,
        password=AMQP_PASSWORD,
        **kwargs,  # type: ignore
    ) as conn:
        yield conn
