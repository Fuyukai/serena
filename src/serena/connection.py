from __future__ import annotations

import enum
import importlib.metadata
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import partial
from os import PathLike
from ssl import SSLContext
from typing import AsyncContextManager, Dict, Optional, Union

import anyio
import attr
from anyio import BrokenResourceError, CancelScope, ClosedResourceError, Lock, sleep
from anyio.abc import ByteStream, TaskGroup
from anyio.lowlevel import checkpoint

from serena.channel import Channel
from serena.enums import ReplyCode
from serena.exc import (
    AMQPError,
    AMQPStateError,
    InvalidPayloadTypeError,
    InvalidProtocolError,
    UnexpectedCloseError,
)
from serena.frame import Frame, FrameType
from serena.frameparser import NEED_DATA, FrameParser
from serena.payloads.header import BasicHeader
from serena.payloads.method import (
    BasicDeliverPayload,
    ChannelCloseOkPayload,
    ChannelClosePayload,
    ChannelOpenPayload,
    ClassID,
    CloseOkPayload,
    ClosePayload,
    ConnectionOpenOkPayload,
    ConnectionOpenPayload,
    MethodFrame,
    MethodPayload,
    StartOkPayload,
    StartPayload,
    TuneOkPayload,
    TunePayload,
)
from serena.utils.bitset import BitSet

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
    #: The number of heartbeats so far.
    heartbeat_count: int = attr.ib(default=0)

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
        self.heartbeat_count += 1

        self.prev_heartbeat_mn = self.cur_heartbeat_mn
        self.prev_heartbeat_time = self.cur_heartbeat_time

        self.cur_heartbeat_mn = time.monotonic_ns()
        self.cur_heartbeat_time = datetime.now(timezone.utc).astimezone()


class AMQPConnection(object):
    """
    A single AMQP connection.
    """

    def __init__(
        self,
        stream: ByteStream,
        *,
        heartbeat_interval: int = 60,
        channel_buffer_size: int = 64,  # reasonable default
    ):
        """
        :param stream: The :class:`.ByteStream` to use.
        :param heartbeat_interval: The heartbeat interval to negotiate with, in seconds.
        :param channel_buffer_size: The buffer size for channel messages.
        """

        self._sock = stream
        self._parser = FrameParser()
        self._heartbeat_interval = heartbeat_interval
        self._channel_buffer_size = channel_buffer_size
        self._cancel_scope: CancelScope = None  # type: ignore

        self._closed = False
        self._server_requested_close = False
        self._close_info: Optional[ClosePayload] = None

        # negotiated data
        self._actual_heartbeat_interval = 0
        self._max_frame_size = 0
        self._server_capabilites = {}

        # list of 64-bit ints (as not to overflow long values and cause a bigint)
        # that is used to assign the next channel ID
        self._channels: BitSet = BitSet(0)

        # mapping of channel id -> Channel
        self._channel_channels: Dict[int, Channel] = {}

        self._write_lock = Lock()

        # statistics
        self._heartbeat_stats = HeartbeatStatistics()

    @staticmethod
    def get_client_properties():
        version = importlib.metadata.version("serena")

        return {
            "product": "Serena AMQP client".encode("utf-8"),
            "platform": f"Python {sys.version}".encode("utf-8"),
            "version": version.encode("utf-8"),
            "capabilites": {
                "publisher_confirms": True,
                "basic.nack": True,
            },
        }

    def has_capability(self, name: str) -> bool:
        """
        Checks if the server exposes a capability.
        """

        return self._server_capabilites.get(name, False)

    async def _read_single_frame(self) -> Frame:
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

    # bleh, this is just a hotfix for data contention. i could probably fix this by driving the
    # writer with a background task writer, but that seems like it would achieve the exact same
    # thing...?
    async def _send(self, data: bytes):
        async with self._write_lock:
            await self._sock.send(data)

    async def _send_method_frame(self, channel: int, payload: MethodPayload):
        """
        Sends a single method frame.
        """

        data = self._parser.write_method_frame(channel, payload)
        await self._send(data)

    async def _send_header_frame(
        self,
        channel: int,
        *,
        method_klass: ClassID,
        body_length: int,
        headers: BasicHeader,
    ):
        """
        Sends a single header frame.
        """

        data = self._parser.write_header_frame(channel, method_klass, body_length, headers)
        await self._send(data)

    async def _send_body_frames(self, channel: int, body: bytes):
        """
        Sends multiple body frames.
        """

        data = self._parser.write_body_frames(channel, body, max_frame_size=self._max_frame_size)
        for frame in data:
            await self._send(frame)

    async def _close_ungracefully(self):
        """
        Closes the connection ungracefully.
        """

        if self._closed:
            return await checkpoint()

        try:
            await self._sock.aclose()
        finally:
            self._closed = True

    async def _do_startup_handshake(self, username: str, password: str, vhost: str):
        """
        Does the startup handshake.
        """

        state = AMQPState.INITIAL
        logger.debug("Sending AMQP handshake...")

        open_message = b"AMQP\x00\x00\x09\x01"
        await self._send(open_message)

        while True:
            incoming_frame = await self._read_single_frame()
            # this can *never* reasonably happen during the handshake
            assert isinstance(incoming_frame, MethodFrame), "incoming frame was not a method???"

            if isinstance(incoming_frame.payload, ClosePayload):
                await self._handle_control_frame(frame=incoming_frame)
                continue

            if state == AMQPState.INITIAL:
                payload = incoming_frame.payload
                if not isinstance(payload, StartPayload):
                    # todo make specific exception
                    await self.close()
                    raise InvalidPayloadTypeError(StartPayload, payload)

                version = (payload.version_major, payload.version_minor)
                if version != (0, 9):
                    await self.close()
                    raise InvalidProtocolError(f"Expected AMQP 0-9-1, but server speaks {version}")

                mechanisms = set(payload.mechanisms.decode(encoding="utf-8").split(" "))
                locales = set(payload.mechanisms.decode(encoding="utf-8").split(" "))

                platform = payload.properties["platform"].decode(encoding="utf-8")
                product = payload.properties["product"].decode(encoding="utf-8")
                version = payload.properties["version"].decode(encoding="utf-8")
                logger.debug(f"Connected to {product} v{version} ({platform})")

                caps: Dict[str, bool] = payload.properties["capabilities"]

                # this is the only capability we require
                if not caps.get("publisher_confirms", False):
                    raise AMQPError("Server does not support publisher confirms, which we require")

                self._server_capabilites = caps

                if "PLAIN" not in mechanisms:
                    # we only speak plain (for now...)
                    await self.close()
                    raise AMQPStateError(
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
                state = AMQPState.RECEIVED_START

            elif state == AMQPState.RECEIVED_START:
                payload = incoming_frame.payload
                if isinstance(payload, TunePayload):
                    wanted_channel_size = min(payload.max_channels, 65535)
                    logger.debug(
                        f"Server asks for {payload.max_channels} channels, "
                        f"we're asking for {wanted_channel_size} channels"
                    )

                    self._channels = BitSet(wanted_channel_size)
                    self._channels[0] = True

                    wanted_frame_size = min(payload.max_frame_size, 131072)
                    logger.debug(
                        f"Server asks for {payload.max_frame_size}B frame sizes, "
                        f"we're asking for {wanted_frame_size}B frame sizes"
                    )
                    self._max_frame_size = wanted_frame_size

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
                    open = ConnectionOpenPayload(virtual_host=vhost)
                    await self._send_method_frame(0, open)
                    state = AMQPState.RECEIVED_TUNE
                else:
                    raise InvalidPayloadTypeError(TunePayload, payload)

            elif state == AMQPState.RECEIVED_TUNE:
                payload = incoming_frame.payload
                if isinstance(payload, ConnectionOpenOkPayload):
                    # we are open
                    logger.info("AMQP connection is ready to go")
                    state = AMQPState.READY
                    break
                else:
                    raise InvalidPayloadTypeError(ConnectionOpenOkPayload, payload)

    async def _open_channel(self):
        """
        Opens a new channel.
        """

        for idx, is_used in enumerate(self._channels):
            if not is_used:
                self._channels[idx] = True
                break
        else:
            # todo better error
            raise RuntimeError("All channel IDs have been used")

        channel_object = Channel(idx, self, self._channel_buffer_size)
        self._channel_channels[idx] = channel_object

        open = ChannelOpenPayload()

        async with anyio.create_task_group() as tg:
            tg.start_soon(partial(self._send_method_frame, idx, open))
            await channel_object._wait_until_open()

        return channel_object

    async def _close_channel(self, id: int):
        """
        Closes a channel.
        """

        if self._server_requested_close:
            logger.warning(f"Server requested close without closing channel #{id}")
            # TODO: Should this be a cancel shielded checkpoint?
            return

        frame = ChannelClosePayload(
            reply_code=ReplyCode.success, reply_text="Normal close", class_id=0, method_id=0
        )
        await self._send_method_frame(id, frame)

    async def _handle_control_frame(self, frame: MethodFrame):
        """
        Handles a control frame.
        """

        payload = frame.payload

        if isinstance(payload, ClosePayload):
            # server closing connection
            logger.info("Server requested close...")
            self._server_requested_close = True
            self._close_info = payload
            # cancel our worker tasks
            # noinspection PyAsyncCall
            self._cancel_scope.cancel()

    async def _enqueue_frame(self, channel: Channel, frame: Frame):
        """
        Enqueues a frame onto the channel object. This will automatically disable the channel flow
        if the buffer size is too small.

        :param channel: The channel to handle.
        :param frame: The frame to handle.
        """

        # Backpressure management is very complex, so here's a general gist of how it works.
        # The general problem is that on our side, a Trio-level (when I say trio, I mean anyio)
        # channel has a fixed buffer size and trying to add to a channel when the buffer is full
        # will cause the sender to block until the receiver takes an item. This causes
        # *backpressure*.
        # This is a desired behaviour as it avoids producers filling memory with items when a
        # receiver can't process them fast enough.
        #
        # The problem is that AMQP is multiplexed, which means there's multiple connections
        # running over one connection via channels. This means that if the sender blocks trying
        # to write new frames to channel X, then all items on channel Y (and Y..N) are also
        # blocked until the consumer eats something and the networking code continues a loop.
        # If the consumer never runs, this causes a deadlock.
        #
        # However, AMQP has a feature where we can disable the flow of messages on a particular
        # channel, whilst still allowing messages to flow on other channels. This is allowed on
        # both the server side and client side, but as the server side handling is drastically
        # simpler it'll be explained in the Channel class.
        #
        # For the client side, we automatically disable the flow of messages when the buffer is
        # about to be full (this is subject to change). This follows a several step process:
        #
        # 1) First, we check the buffer size.
        # 2a) If it's less than one below the maximum, we add the frame to the buffer and return.
        #     This is the simplest and easiest case.
        # 2b) If it's one below the maximum, then we once again add the frame to the buffer,
        #     but don't return; we have to then disable the channel.
        # 3) To disable the channel, we send a flow message synchronously to the server. As this is
        #    happening in the same context as the network receiver, no new messages are received
        #    during this loop.
        # 4) Then, we have to listen out for when the channel is ready to receive messages again.
        #    This is achieved by an event that is set on the channel class whenever a frame is
        #    *fully* processed, which is listened to by a task running in a separate nursery.
        # 5) The task in the other nursery checks the buffer size each time the event is set;
        #    when it is *half* of the maximum size, a new Flow message is sent to unblock the
        #    channel. This avoids the case where new messages constantly arrive and overflow the
        #    channel, causing a Flow to be sent, then it's immediately reset after one message and
        #    causes a lot of unnecessary network traffic.

        threshold = channel.max_buffer_size - 1
        count = threshold - channel.current_buffer_size
        # simple case
        if count > 1:
            logger.debug(
                f"Enqueueing frame on channel {channel.id}, {count} frames left in the buffer"
            )
            return await channel._enqueue_delivery(frame)

        # complex case
        raise NotImplementedError("complex case not implemented yet")

    async def _heartbeat_loop(self):
        """
        Sends heartbeats to the AMQP server.
        """

        if self._heartbeat_interval == 0:
            return

        while True:
            # heartbeat frame
            await self._send(b"\x08\x00\x00\x00\x00\x00\x00\xCE")
            await sleep(self._heartbeat_interval / 2)

    async def _listen_for_messages(self):
        """
        Listens for messages infinitely. This is the primary driving loop of the connection.
        """

        while not (self._closed or self._server_requested_close):
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

            elif frame.type == FrameType.METHOD:
                assert isinstance(frame, MethodFrame)

                # intercept control frames, e.g. close payloads
                channel = frame.channel_id
                if channel == 0:
                    await self._handle_control_frame(frame)  # type: ignore
                    continue

                # we intercept certain control frames
                # channelcloseok is handled here because channels can't deal with their own closure
                # (as they don't know when they finish)
                channel_object = self._channel_channels[channel]

                if isinstance(frame.payload, (ChannelClosePayload, ChannelCloseOkPayload)):
                    is_unclean = isinstance(frame, ChannelClosePayload)

                    self._channels[channel] = False
                    self._channel_channels.pop(channel)
                    await channel_object._close(frame.payload if is_unclean else None)

                    # ack the close
                    # todo: should this be here?
                    if is_unclean:
                        await self._send_method_frame(0, ChannelCloseOkPayload())

                    continue

                elif isinstance(frame.payload, BasicDeliverPayload):
                    # requires special logic
                    await self._enqueue_frame(channel_object, frame)

                else:
                    # just delivery normally
                    await channel_object._enqueue_regular(frame)

            elif frame.type == FrameType.HEADER or frame.type == FrameType.BODY:
                channel = frame.channel_id
                assert channel != 0, "header frame cannot happen on control channel"
                channel_object = self._channel_channels[channel]
                await self._enqueue_frame(channel_object, frame)

    def _start_tasks(self, nursery: TaskGroup):
        """
        Starts the background tasks for this connection.
        """
        self._cancel_scope = nursery.cancel_scope
        nursery.start_soon(self._listen_for_messages)
        nursery.start_soon(self._heartbeat_loop)
        # nursery.start_soon(self._flow_handler)

    async def _close(self, reply_code: int = 200, reply_text: str = "Normal close"):
        if self._closed:
            await checkpoint()
            return

        # at this point we're being called in the finally, so we have exclusive control of the
        # socket
        # we may be being cancelled, however, so we need to shield ourselves when sending more
        # things

        with CancelScope(shield=True):
            # send a CloseOk method if the server requested our closure
            if self._server_requested_close:
                logger.debug("Acknowledging server close...")
                payload = CloseOkPayload()
                await self._send_method_frame(0, payload)
                await self._close_ungracefully()

                if self._close_info.reply_code != 200:
                    raise UnexpectedCloseError.of(self._close_info)

            else:
                logger.debug("Sending close payload...")

                payload = ClosePayload(
                    reply_code=ReplyCode(reply_code), reply_text=reply_text, class_id=0, method_id=0
                )

                await self._send_method_frame(0, payload)

                try:
                    while True:
                        # drain previous frames
                        # channels will send their own close frame before we get a chance to drain
                        # it

                        try:
                            reply = await self._read_single_frame()
                        except (BrokenResourceError, ClosedResourceError):
                            raise AMQPStateError(
                                f"Expected CloseOk, but connection failed to send it"
                            )

                        if isinstance(reply, MethodFrame) and isinstance(
                            reply.payload, CloseOkPayload
                        ):
                            logger.debug("Received CloseOk, closing connection")
                            return
                finally:
                    await self._close_ungracefully()

    def open_channel(self) -> AsyncContextManager[Channel]:
        """
        Opens a new channel.

        :return: An asynchronous context manager that will open a new channel.
        """

        @asynccontextmanager
        async def cm():
            channel = await self._open_channel()
            try:
                yield channel
            finally:
                await self._close_channel(channel.id)

        return cm()

    async def close(self, reply_code: int = 200, reply_text: str = "Normal close"):
        """
        Closes the connection. This method is idempotent.

        There's no real reason to call this method.

        :param reply_code: The code to send when closing.
        :param reply_text: The text to send when replying.
        :return: Nothing.
        """

        # close any background tasks
        if not self._cancel_scope.cancel_called:
            # noinspection PyAsyncCall
            self._cancel_scope.cancel()

        await self._close(reply_code, reply_text)


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
            async with anyio.create_task_group() as nursery:
                conn._start_tasks(nursery)
                yield conn
        finally:
            await conn.close()

    return _do_open()
