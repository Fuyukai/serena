from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream

from serena.frame import Frame
from serena.payloads.method import (
    ChannelOpenOkPayload,
    MethodFrame,
    method_payload_name,
)

if TYPE_CHECKING:
    from serena.connection import AMQPConnection


class Channel(object):
    """
    A wrapper around an AMQP channel.
    """

    def __init__(self, channel_id: int, connection: AMQPConnection, stream_buffer_size: int):
        """
        :param channel_id: The ID of this channel.
        :param connection: The AMQP connection object to send data on.
        :param stream_buffer_size: The buffer size for the streams.
        """

        self._connection = connection
        self._channel_id = channel_id
        self._open = False

        self._send, self._receive = anyio.create_memory_object_stream(
            max_buffer_size=stream_buffer_size
        )

    @property
    def open(self) -> bool:
        """
        Returns if this channel is open or not.
        """

        return self._open

    @property
    def max_buffer_size(self) -> int:
        """
        Returns the maximum number of frames buffered in this channel. Used internally.
        """

        return int(self._send.statistics().max_buffer_size)

    @property
    def current_buffer_size(self) -> int:
        """
        Returns the current number of frames buffered in this channel. Used internally.
        """

        return self._send.statistics().current_buffer_used

    async def _internal_enqueue(self, frame: Frame):
        await self._send.send(frame)

    async def _wait_until_open(self):
        """
        Waits until the channel is open.
        """

        frame = await self._receive.receive()
        if not isinstance(frame, MethodFrame):
            raise ValueError(f"Expected MethodFrame, got {frame}")

        if not isinstance(frame.payload, ChannelOpenOkPayload):
            raise ValueError(
                f"Expected ChannelOpenOkPayload, got {method_payload_name(frame.payload)}"
            )

        self._open = True
