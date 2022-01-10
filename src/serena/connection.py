from __future__ import annotations

import enum
import importlib.metadata
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from os import PathLike
from ssl import SSLContext
from typing import AsyncContextManager, Optional, Union

import anyio
import attr
from anyio.abc import ByteStream
from anyio.lowlevel import checkpoint

from serena.frame import FrameType
from serena.frameparser import NEED_DATA, FrameParser
from serena.payloads.method import (
    MethodFrame,
    MethodPayload,
    OpenOkPayload,
    OpenPayload,
    StartOkPayload,
    StartPayload,
    TuneOkPayload,
    TunePayload,
    method_payload_name,
)

logger = logging.getLogger(__name__)


class AMQPState(enum.IntEnum):
    """
    State enumeration for the connection object.
    """

    #: The initial state. The connection has opened and we are waiting for the Start payload.
    INITIAL = 0

    #: We have received the Start payload, and we are waiting for the Tune payload.
    RECEIVED_START = 1

    #: We have received the Tune payload, and we are waiting for the Open-Ok payload.
    RECEIVED_TUNE = 2

    #: The default idle state.
    READY = 10


@attr.s(slots=True, frozen=False)
class HeartbeatStatistics:
    #: The previous heartbeat time, in monotonic nanoseconds.
    prev_heartbeat_mn: int = attr.ib(default=None)

    #: The current heartbeat time, in monotonic nanoseconds.
    cur_heartbeat_mn: int = attr.ib(default=None)

    #: The previous heartbeat time, in wall clock time.
    prev_heartbeat_time: datetime = attr.ib(default=None)

    #: The current heartbeat time, in wall clock time.
    cur_heartbeat_time: datetime = attr.ib(default=None)

    @property
    def interval(self) -> Optional[int]:
        """
        Returns the interval between two heartbeats in nanoseconds.
        """

        if self.prev_heartbeat_mn is None:
            return None

        return self.cur_heartbeat_mn - self.prev_heartbeat_mn

    def update(self):
        self.prev_heartbeat_mn = self.cur_heartbeat_mn
        self.prev_heartbeat_time = self.cur_heartbeat_time

        self.cur_heartbeat_mn = time.monotonic_ns()
        self.cur_heartbeat_time = datetime.now(timezone.utc).astimezone()


class AMQPConnection(object):
    """
    A single AMQP connection.
    """

    def __init__(self, stream: ByteStream, *, heartbeat_interval: int = 60):
        """
        :param stream: The :class:`.ByteStream` to use.
        :param heartbeat_interval: The heartbeat interval to negotiate with, in seconds.
        """

        self._sock = stream
        self._parser = FrameParser()

        self._state = AMQPState.INITIAL
        self._closed = False

        self._heartbeat_interval = heartbeat_interval
        # what we negotiate with the server
        self._actual_heartbeat_interval = 0

        # statistics
        self._heartbeat_stats = HeartbeatStatistics()

    @staticmethod
    def get_client_properties():
        version = importlib.metadata.version("serena")

        return {
            "product": "Serena AMQP client",
            "platform": f"Python {sys.version}",
            "version": version,
        }

    async def _read_single_frame(self):
        """
        Reads a single frame from the AMQP connection.
        """
        frame = self._parser.next_frame()
        if frame is not NEED_DATA:
            await checkpoint()
            return frame

        while True:
            data = await self._sock.receive(4096)
            self._parser.receive_data(data)

            frame = self._parser.next_frame()
            if frame is not NEED_DATA:
                return frame

    async def _send_method_frame(self, channel: int, payload: MethodPayload):
        """
        Sends a single method frame.
        """

        data = self._parser.write_method_frame(channel, payload)
        await self._sock.send(data)

    async def _close_ungracefully(self):
        """
        Closes the connection ungracefully.
        """

        if self._closed:
            return await checkpoint()

        logger.debug("The connection is closing...")

        try:
            await self._sock.aclose()
        finally:
            self._closed = True

    async def _send_heartbeat(self):
        """
        Sends a heartbeat frame.
        """

        data = self._parser.write_heartbeat_frame()
        await self._sock.send(data)

    async def _listen_for_messages(self):
        """
        Listens for messages infinitely. This is the primary driving loop of the connection.
        """

        while not self._closed:
            try:
                with anyio.fail_after(self._actual_heartbeat_interval):
                    frame = await self._read_single_frame()
            except TimeoutError:
                logger.error(
                    f"Server failed to send any messages in {self._actual_heartbeat_interval} "
                    "seconds, disconnecting!"
                )

                await self._close_ungracefully()
                raise

            if frame.type == FrameType.HEARTBEAT:
                self._heartbeat_stats.update()
                interval = self._heartbeat_stats.interval
                if interval is None:
                    logger.debug("Received heartbeat frame")
                else:
                    logger.debug(
                        f"Received heartbeat frame (interval: {interval / 1_000_000_000:.2f}s)"
                    )

    async def _do_startup_handshake(self, username: str, password: str, vhost: str):
        """
        Does the startup handshake.
        """

        logger.debug("Sending AMQP handshake...")

        open_message = b"AMQP\x00\x00\x09\x01"
        await self._sock.send(open_message)

        while True:
            incoming_frame = await self._read_single_frame()
            # cpprint(incoming_frame)
            # this can *never* reasonably happen during the handshake
            assert isinstance(incoming_frame, MethodFrame), "incoming frame was not a method???"

            if self._state == AMQPState.INITIAL:
                payload = incoming_frame.payload
                if not isinstance(payload, StartPayload):
                    # todo make specific exception
                    await self.close()
                    raise ValueError(f"Expected StartPayload, got {method_payload_name(payload)}")

                version = (payload.version_major, payload.version_minor)
                if version != (0, 9):
                    await self.close()
                    raise ValueError(f"Expected AMQP 0-9-1, but server speaks {version}")

                mechanisms = set(payload.mechanisms.decode(encoding="utf-8").split(" "))
                locales = set(payload.mechanisms.decode(encoding="utf-8").split(" "))

                platform = payload.properties["platform"].decode(encoding="utf-8")
                product = payload.properties["product"].decode(encoding="utf-8")
                version = payload.properties["version"].decode(encoding="utf-8")
                logger.debug(f"Connected to {product} v{version} ({platform})")

                if "PLAIN" not in mechanisms:
                    # we only speak plain (for now...)
                    await self.close()
                    raise ValueError(
                        f"Expected PLAIN authentication method, but we only have {mechanisms}"
                    )

                sasl_response = b"\x00%s\x00%s" % (
                    username.encode("utf-8"),
                    password.encode("utf-8"),
                )
                ok_frame = StartOkPayload(
                    properties=self.get_client_properties(),
                    mechanism="PLAIN",
                    response=sasl_response,
                    locale="en_US",
                )
                await self._send_method_frame(0, ok_frame)
                self._state = AMQPState.RECEIVED_START

            elif self._state == AMQPState.RECEIVED_START:
                payload = incoming_frame.payload
                if isinstance(payload, TunePayload):
                    wanted_channel_size = min(payload.max_channels, 65536)
                    logger.debug(
                        f"Server asks for {payload.max_channels} channels, "
                        f"we're asking for {wanted_channel_size} channels"
                    )

                    wanted_frame_size = min(payload.max_frame_size, 131072)
                    logger.debug(
                        f"Server asks for {payload.max_frame_size}B frame sizes, "
                        f"we're asking for {wanted_frame_size}B frame sizes"
                    )

                    hb_interval = min(payload.heartbeat_delay, self._heartbeat_interval)
                    logger.debug(
                        f"Server asks for {payload.heartbeat_delay} seconds between "
                        f"heartbeats, we're asking for {hb_interval} seconds"
                    )
                    self._actual_heartbeat_interval = hb_interval

                    tune_ok = TuneOkPayload(
                        max_channels=wanted_channel_size,
                        max_frame_size=wanted_frame_size,
                        heartbeat_delay=hb_interval,
                    )
                    await self._send_method_frame(0, tune_ok)

                    # open the connection now
                    open = OpenPayload(virtual_host=vhost)
                    await self._send_method_frame(0, open)
                    self._state = AMQPState.RECEIVED_TUNE
                else:
                    raise ValueError(f"Expected Tune, got {method_payload_name(payload)}")

            elif self._state == AMQPState.RECEIVED_TUNE:
                payload = incoming_frame.payload
                if isinstance(payload, OpenOkPayload):
                    # we are open
                    logger.info("AMQP connection is ready to go")
                    self._state = AMQPState.READY
                    break
                else:
                    raise ValueError(f"Expected OpenOk, got {method_payload_name(payload)}")

    async def close(self, reply_code: int = 200, reply_text: str = "Normal close"):
        """
        Closes the connection. This method is idempotent.

        :param reply_code: The code to send when closing.
        :param reply_text: The text to send when replying.
        :return: Nothing.
        """

        # todo graceful close...
        await self._close_ungracefully()


async def _open_connection(
    address: Union[str, PathLike],
    *,
    port: int = 6379,
    username: str = "guest",
    password: str = "guest",
    vhost: str = "/",
    ssl_context: SSLContext = None,
    **kwargs,
) -> AMQPConnection:
    """
    Actually implements opening the connection and performing the startup handshake.
    """

    if isinstance(address, os.PathLike) or address.startswith("/"):
        path = os.fspath(address)
        logger.debug(f"Opening connection to {path}")
        sock = await anyio.connect_unix(path)
    else:
        logger.debug(f"Opening TCP connection to {address}:{port}")
        sock = await anyio.connect_tcp(
            address,
            remote_port=port,
            tls=ssl_context is not None,
            ssl_context=ssl_context,
            tls_standard_compatible=True,
        )

    connection = AMQPConnection(sock, **kwargs)
    # noinspection PyProtectedMember
    await connection._do_startup_handshake(username, password, vhost)
    return connection


def open_connection(
    address: Union[str, PathLike],
    *,
    port: int = 5672,
    username: str = "guest",
    password: str = "guest",
    virtual_host: str = "/",
    ssl_context: SSLContext = None,
    **kwargs,
) -> AsyncContextManager[AMQPConnection]:
    """
    Opens a new connection to the AMQP 0-9-1 server. This is an asynchronous context manager.

    Required parameters:

    :param address: The address of the server or the *absolute path* of its Unix socket.

    Optional parameters:

    :param port: The port to connect to. Ignores for Unix sockets. Defaults to 5672.
    :param username: The username to connect using.
    :param password: The password to authenticate with.
    :param virtual_host: The AMQP virtual host to connect to.
    :param ssl_context: The SSL context to connect with.
    """

    @asynccontextmanager
    async def _do_open():
        conn = await _open_connection(
            address=address,
            port=port,
            username=username,
            password=password,
            vhost=virtual_host,
            ssl_context=ssl_context,
            **kwargs,
        )

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(conn._listen_for_messages)
                yield conn
        finally:
            await conn.close()

    return _do_open()
