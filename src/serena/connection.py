from __future__ import annotations

import copy
import enum
import importlib.metadata
import os
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from functools import partial
from os import PathLike
from ssl import SSLContext
from typing import Any

import anyio
import attr
from anyio import CancelScope, EndOfStream, Lock, WouldBlock, sleep
from anyio.abc import ByteStream, TaskGroup
from anyio.lowlevel import checkpoint

from serena.channel import Channel
from serena.enums import ClassID, ReplyCode
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
    BasicGetEmptyPayload,
    BasicGetOkPayload,
    BasicQOSOkPayload,
    BasicQOSPayload,
    ChannelCloseOkPayload,
    ChannelClosePayload,
    ChannelOpenPayload,
    ConfirmSelectOkPayload,
    ConfirmSelectPayload,
    ConnectionCloseOkPayload,
    ConnectionClosePayload,
    ConnectionOpenOkPayload,
    ConnectionOpenPayload,
    ConnectionStartOkPayload,
    ConnectionStartPayload,
    ConnectionTuneOkPayload,
    ConnectionTunePayload,
    MethodFrame,
    MethodPayload,
    method_payload_name,
)
from serena.pool import ChannelPool
from serena.utils import LoggerWithTrace
from serena.utils.bitset import BitSet

logger: LoggerWithTrace = LoggerWithTrace.get(__name__)


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


@attr.s(slots=True, frozen=False)
class HeartbeatStatistics:
    #: The number of heartbeats so far.
    heartbeat_count: int = attr.ib(default=0)

    #: The previous heartbeat time, in monotonic nanoseconds.
    prev_heartbeat_mn: int = attr.ib(default=-1)

    #: The current heartbeat time, in monotonic nanoseconds.
    cur_heartbeat_mn: int = attr.ib(default=-1)

    #: The previous heartbeat time, in wall clock time.
    prev_heartbeat_time: datetime = attr.ib(default=None)

    #: The current heartbeat time, in wall clock time.
    cur_heartbeat_time: datetime = attr.ib(default=None)

    @property
    def interval(self) -> int | None:
        """
        Returns the interval between two heartbeats in nanoseconds.
        """

        if self.prev_heartbeat_mn == -1 or self.cur_heartbeat_mn == -1:
            return None

        return self.cur_heartbeat_mn - self.prev_heartbeat_mn

    def update(self) -> None:
        self.heartbeat_count += 1

        self.prev_heartbeat_mn = self.cur_heartbeat_mn
        self.prev_heartbeat_time = self.cur_heartbeat_time

        self.cur_heartbeat_mn = time.monotonic_ns()
        self.cur_heartbeat_time = datetime.now(UTC).astimezone()


# noinspection PyProtectedMember
class AMQPConnection:
    """
    A single AMQP connection.
    """

    def __init__(
        self,
        stream: ByteStream,
        *,
        heartbeat_interval: int = 60,
        channel_buffer_size: int = 48,  # reasonable default
        desired_frame_size: int = 131072,
    ) -> None:
        """
        :param stream: The :class:`.ByteStream` to use.
        :param heartbeat_interval: The heartbeat interval to negotiate with, in seconds.
        :param channel_buffer_size: The buffer size for channel messages.
        :param desired_frame_size: The maximum body message frame size desired.
        """

        self._sock = stream
        self._parser = FrameParser()
        self._heartbeat_interval = heartbeat_interval
        self._channel_buffer_size = channel_buffer_size
        self._cancel_scope: CancelScope | None = None

        self._closed = False
        self._server_requested_close = False
        self._close_info: ConnectionClosePayload | None = None

        self._is_rabbitmq = False

        # negotiated data
        self._actual_heartbeat_interval = 0
        self._max_frame_size = desired_frame_size
        self._server_capabilities: dict[str, bool] = {}

        # list of 64-bit ints (as not to overflow long values and cause a bigint)
        # that is used to assign the next channel ID
        self._channels: BitSet = BitSet(0)

        # mapping of channel id -> Channel
        self._channel_channels: dict[int, Channel] = {}

        self._write_lock = Lock()

        # statistics
        self._heartbeat_stats = HeartbeatStatistics()

    @staticmethod
    def get_client_properties() -> dict[str, Any]:  # TODO: typeddict?
        version = importlib.metadata.version("serena")

        return {
            "product": b"Serena AMQP client",
            "platform": f"Python {sys.version}".encode(),
            "version": version.encode("utf-8"),
            "capabilities": {
                "publisher_confirms": True,
                "basic.nack": True,
                "authentication_failure_close": True,
                "per_consumer_qos": True,
            },
        }

    @property
    def open(self) -> bool:
        """
        Checks if this connection is actually open.
        """

        return not self._closed and not self._server_requested_close

    def has_capability(self, name: str) -> bool:
        """
        Checks if the server exposes a capability.
        """

        return self._server_capabilities.get(name, False)

    def heartbeat_statistics(self) -> HeartbeatStatistics:
        """
        Returns a copy of the heartbeat statistics for this connection.
        """

        return copy.copy(self._heartbeat_stats)

    async def _read_single_frame(self) -> Frame:
        """
        Reads a single frame from the AMQP connection.
        """

        # for some reason this isn't properly smart casted
        frame = self._parser.next_frame()
        if frame is not NEED_DATA:
            await checkpoint()
            return frame  # type: ignore

        while True:
            data = await self._sock.receive(4096)
            self._parser.receive_data(data)

            frame = self._parser.next_frame()
            if frame is not NEED_DATA:
                return frame  # type: ignore

    # bleh, this is just a hotfix for data contention. i could probably fix this by driving the
    # writer with a background task writer, but that seems like it would achieve the exact same
    # thing...?
    async def _send(self, data: bytes) -> None:
        async with self._write_lock:
            await self._sock.send(data)

    async def _send_method_frame(self, channel: int, payload: MethodPayload) -> None:
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
    ) -> None:
        """
        Sends a single header frame.
        """

        data = self._parser.write_header_frame(channel, method_klass, body_length, headers)
        await self._send(data)

    async def _send_body_frames(self, channel: int, body: bytes) -> None:
        """
        Sends multiple body frames.
        """

        data = self._parser.write_body_frames(channel, body, max_frame_size=self._max_frame_size)

        if not data:
            await checkpoint()
            return

        for frame in data:
            await self._send(frame)

    async def _close_ungracefully(self) -> None:
        """
        Closes the connection ungracefully.
        """

        if self._closed:
            return await checkpoint()

        try:
            await self._sock.aclose()
        finally:
            self._closed = True

    async def _do_startup_handshake(self, username: str, password: str, vhost: str) -> None:
        """
        Does the startup handshake.
        """

        state = AMQPState.INITIAL
        logger.debug("Sending AMQP handshake...")

        open_message = b"AMQP\x00\x00\x09\x01"
        await self._send(open_message)

        while self.open:
            incoming_frame = await self._read_single_frame()
            # this can *never* reasonably happen during the handshake
            assert isinstance(incoming_frame, MethodFrame), "incoming frame was not a method???"
            method_frame: MethodFrame[MethodPayload] = incoming_frame

            if isinstance(method_frame.payload, ConnectionClosePayload):
                close_ok = ConnectionCloseOkPayload()
                try:
                    await self._send_method_frame(0, close_ok)
                finally:
                    raise UnexpectedCloseError.of(method_frame.payload)

            if state == AMQPState.INITIAL:
                payload = method_frame.payload

                if not isinstance(payload, ConnectionStartPayload):  # pragma: no cover
                    await self._close_ungracefully()
                    raise InvalidPayloadTypeError(ConnectionStartPayload, payload)

                version = (payload.version_major, payload.version_minor)

                if version != (0, 9):  # pragma: no cover
                    await self._close_ungracefully()
                    raise InvalidProtocolError(f"Expected AMQP 0-9-1, but server speaks {version}")

                mechanisms = set(payload.mechanisms.decode(encoding="utf-8").split(" "))
                set(payload.mechanisms.decode(encoding="utf-8").split(" "))

                platform = payload.properties["platform"].decode(encoding="utf-8")
                product = payload.properties["product"].decode(encoding="utf-8")
                if product == "RabbitMQ":  # pragma: no cover
                    logger.debug("Enabling RabbitMQ-specific code paths...")
                    self._is_rabbitmq = True

                version = payload.properties["version"].decode(encoding="utf-8")
                logger.debug(f"Connected to {product} v{version} ({platform})")

                caps: dict[str, bool] = payload.properties["capabilities"]

                # this is the only capability we require
                if not caps.get("publisher_confirms", False):  # pragma: no cover
                    raise AMQPError("Server does not support publisher confirms, which we require")

                self._server_capabilities = caps
                for cap, value in self._server_capabilities.items():
                    if value:
                        logger.trace(f"Server supports capability {cap}")

                if "PLAIN" not in mechanisms:  # pragma: no cover
                    # we only speak plain (for now...)
                    await self._close_ungracefully()
                    raise AMQPStateError(
                        f"Expected PLAIN authentication method, but we only have {mechanisms}"
                    )

                sasl_response = b"\x00%s\x00%s" % (
                    username.encode("utf-8"),
                    password.encode("utf-8"),
                )
                ok_frame = ConnectionStartOkPayload(
                    properties=self.get_client_properties(),
                    mechanism="PLAIN",
                    response=sasl_response,
                    locale="en_US",
                )
                await self._send_method_frame(0, ok_frame)
                state = AMQPState.RECEIVED_START

            elif state == AMQPState.RECEIVED_START:
                payload = method_frame.payload
                if isinstance(payload, ConnectionTunePayload):
                    wanted_channel_size = min(payload.max_channels, 65535)
                    logger.debug(
                        f"Server asks for {payload.max_channels} channels, "
                        f"we're asking for {wanted_channel_size} channels"
                    )

                    self._channels = BitSet(wanted_channel_size)
                    self._channels[0] = True

                    wanted_frame_size = min(payload.max_frame_size, self._max_frame_size)
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

                    tune_ok = ConnectionTuneOkPayload(
                        max_channels=wanted_channel_size,
                        max_frame_size=wanted_frame_size,
                        heartbeat_delay=hb_interval,
                    )
                    await self._send_method_frame(0, tune_ok)

                    # open the connection now
                    open = ConnectionOpenPayload(virtual_host=vhost)
                    await self._send_method_frame(0, open)
                    state = AMQPState.RECEIVED_TUNE
                else:  # pragma: no cover
                    raise InvalidPayloadTypeError(ConnectionTunePayload, payload)

            elif state == AMQPState.RECEIVED_TUNE:
                payload = method_frame.payload
                if isinstance(payload, ConnectionOpenOkPayload):
                    # we are open
                    logger.info("AMQP connection is ready to go")
                    break

                raise InvalidPayloadTypeError(ConnectionOpenOkPayload, payload)  # pragma: no cover

    async def _open_channel(self) -> Channel:
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

        logger.debug(f"Opening new AMQP channel with {idx=}")

        channel_object = Channel(idx, self, self._channel_buffer_size)
        self._channel_channels[idx] = channel_object

        try:
            async with anyio.create_task_group() as tg:
                open = ChannelOpenPayload()

                tg.start_soon(partial(self._send_method_frame, idx, open))
                await channel_object._wait_until_open()
        except:
            self._channels[idx] = False
            self._channel_channels.pop(idx)
            raise

        logger.debug(f"Enabling channel QOS with {self._channel_buffer_size // 3} max payloads.")

        # enable qos and select
        qos = BasicQOSPayload(
            prefetch_size=0,
            prefetch_count=self._channel_buffer_size // 3,  # METHOD, HEADER, BODY
            global_=False,
        )
        await channel_object._send_and_receive_frame(qos, BasicQOSOkPayload)

        select = ConfirmSelectPayload(no_wait=False)
        await channel_object._send_and_receive_frame(select, ConfirmSelectOkPayload)

        return channel_object

    async def _close_channel(self, id: int) -> None:
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
        self._channel_channels[id]._close_info = frame
        await self._send_method_frame(id, frame)

    async def _handle_control_frame(self, frame: MethodFrame[MethodPayload]) -> None:
        """
        Handles a control frame.
        """

        payload = frame.payload

        if isinstance(payload, ConnectionClosePayload):
            # server closing connection
            logger.info("Server requested close...")
            self._server_requested_close = True
            self._close_info = payload

            # cancel our worker tasks if needed
            if self._cancel_scope is not None:
                # noinspection PyAsyncCall
                self._cancel_scope.cancel()

    def _remove_channel(self, channel_id: int, payload: ChannelClosePayload | None) -> None:
        """
        Removes a channel.
        """

        self._channels[channel_id] = False
        chan = self._channel_channels.pop(channel_id)
        chan._close(payload)

    async def _enqueue_frame(self, channel: Channel, frame: Frame) -> None:
        """
        Enqueues a frame onto the channel object.

        :param channel: The channel to handle.
        :param frame: The frame to handle.
        """

        # pfft, there used to be a whole doc comment about enabling/disabling flows but turns out
        # i can't read and there's just a qos() method.

        threshold = channel.max_buffer_size - 1
        count = threshold - channel.current_buffer_size
        # simple case
        if count >= 1:
            logger.debug(
                f"Enqueueing frame on channel {channel.id}, {count} frames left in the buffer"
            )
            return await channel._enqueue_delivery(frame)

        # complex case
        logger.critical(
            f"!!! CHANNEL {channel.id} IS ABOUT TO BLOCK: This WILL cause a deadlock !!!"
        )
        logger.critical("The most likely cause is too large messages!")
        logger.critical(
            "You *must* adjust your desired frame size on both the client and the "
            "server, raise the channel buffer, or have faster consumers."
        )
        logger.critical(
            f"Current frame size is {self._max_frame_size}, and the current buffer "
            f"size is {self._channel_buffer_size}."
        )
        logger.debug(f"Enqueuing frame on blocked channel {channel.id}")
        return await channel._enqueue_delivery(frame)

    async def _heartbeat_loop(self) -> None:
        """
        Sends heartbeats to the AMQP server.
        """

        if self._heartbeat_interval == 0:
            return

        while True:
            # heartbeat frame
            await self._send(b"\x08\x00\x00\x00\x00\x00\x00\xce")
            await sleep(self._heartbeat_interval / 2)

    async def _listen_for_messages(self) -> None:
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
                method_frame: MethodFrame[MethodPayload] = frame

                # intercept control frames, e.g. close payloads
                channel = frame.channel_id
                if channel == 0:
                    await self._handle_control_frame(method_frame)
                    continue

                # we intercept certain control frames
                # channelcloseok is handled here because channels can't deal with their own closure
                # (as they don't know when they finish)
                channel_object = self._channel_channels[channel]
                payload = method_frame.payload

                if isinstance(payload, ChannelClosePayload | ChannelCloseOkPayload):
                    is_unclean = isinstance(payload, ChannelClosePayload)
                    logger.debug(f"Channel close received, {is_unclean=}")
                    close_payload = payload if is_unclean else None

                    # ack the close
                    # todo: should his be here?
                    try:
                        self._remove_channel(channel, close_payload)
                    finally:
                        if is_unclean:
                            await self._send_method_frame(channel, ChannelCloseOkPayload())

                    continue

                if isinstance(
                    payload, BasicDeliverPayload | BasicGetOkPayload | BasicGetEmptyPayload
                ):
                    # requires special logic
                    await self._enqueue_frame(channel_object, method_frame)

                else:
                    # just delivery normally
                    try:
                        channel_object._enqueue_regular(method_frame)
                    except WouldBlock:
                        logger.warning(
                            f"Channel #{channel} was not listening for frame "
                            f"{method_payload_name(payload)}, dropping"
                        )

            elif frame.type == FrameType.HEADER or frame.type == FrameType.BODY:
                channel = frame.channel_id
                assert channel != 0, "header frame cannot happen on control channel"
                channel_object = self._channel_channels[channel]
                await self._enqueue_frame(channel_object, frame)

    async def _listen_wrapper(self) -> None:
        """
        Wraps ``listen_for_messages`` so that it'll automatically signal to all channels if
        the task is closed.
        """

        # https://matrix.to/#/%23python-trio_general%3Agitter.im/%24b81b-VPgvL6z6ALlACbL27TYjAk27Bz_jIM6q39QWW0?via=gitter.im&via=matrix.org
        # Basically, if the top-most task that holds the connection open is cancelled, then the
        # "wait for channel closure" logic won't be fired, because the task actually responsible for
        # pumping the messages around is killed.
        #
        # So, this wraps the listen function in a try/finally that sets the event manually.

        try:
            await self._listen_for_messages()
        finally:
            for channel in self._channel_channels.values():
                channel._close(None)

    def _start_tasks(self, nursery: TaskGroup) -> None:
        """
        Starts the background tasks for this connection.
        """
        self._cancel_scope = nursery.cancel_scope
        nursery.start_soon(self._listen_wrapper)
        nursery.start_soon(self._heartbeat_loop)

    async def _close(self, reply_code: int = 200, reply_text: str = "Normal close") -> None:
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
                await self._send_method_frame(0, ConnectionCloseOkPayload())
                await self._close_ungracefully()

                assert self._close_info is not None, "where's the close info???"

                if self._close_info.reply_code != 200:
                    raise UnexpectedCloseError.of(self._close_info)

            else:
                logger.debug("Sending close payload...")

                await self._send_method_frame(
                    0,
                    ConnectionClosePayload(
                        reply_code=ReplyCode(reply_code),
                        reply_text=reply_text,
                        class_id=0,
                        method_id=0,
                    ),
                )

                try:
                    while True:
                        # drain previous frames
                        # channels will send their own close frame before we get a chance to drain
                        # it

                        try:
                            reply = await self._read_single_frame()
                        except EndOfStream:
                            raise AMQPStateError(
                                "Expected CloseOk, but connection failed to send it"
                            ) from None

                        if isinstance(reply, MethodFrame):
                            method_frame: MethodFrame[MethodPayload] = reply

                            if isinstance(method_frame.payload, ConnectionCloseOkPayload):
                                logger.debug("Received CloseOk, closing connection")
                                return
                finally:
                    await self._close_ungracefully()

    @asynccontextmanager
    async def open_channel(self) -> AsyncGenerator[Channel, None]:
        """
        Opens a new channel.

        :return: An asynchronous context manager that will open a new channel.
        """

        if not self.open:
            raise RuntimeError("The connection is not open")

        channel = await self._open_channel()
        try:
            yield channel
        finally:
            # don't try and re-close the channel if the channel was closed server-side

            if not channel._closed:
                channel._closed = True

                with anyio.CancelScope(shield=True):
                    await self._close_channel(channel.id)

    @asynccontextmanager
    async def open_channel_pool(
        self, initial_channels: int = 64
    ) -> AsyncGenerator[ChannelPool, None]:
        """
        Opens a new channel pool.

        :param initial_channels: The number of channels to use initially.
        :return: An asynchronous context manager that will open a new channel pool.
        """

        pool = ChannelPool(self, initial_channels)

        async with anyio.create_task_group() as tg:
            tg.start_soon(pool._open_channels)

            try:
                await pool._open(initial_size=initial_channels)
                yield pool
            finally:
                tg.cancel_scope.cancel()

                with CancelScope(shield=True):
                    await pool._close()

    async def close(self, reply_code: int = 200, reply_text: str = "Normal close") -> None:
        """
        Closes the connection. This method is idempotent.

        There's no real reason to call this method.

        :param reply_code: The code to send when closing.
        :param reply_text: The text to send when replying.
        :return: Nothing.
        """

        # close any background tasks
        if self._cancel_scope is not None and not self._cancel_scope.cancel_called:
            # noinspection PyAsyncCall
            self._cancel_scope.cancel()

        await self._close(reply_code, reply_text)


async def _open_connection(
    address: str | PathLike[str],
    *,
    port: int = 6379,
    username: str = "guest",
    password: str = "guest",
    vhost: str = "/",
    ssl_context: SSLContext | None = None,
    **kwargs: Any,
) -> AMQPConnection:
    """
    Actually implements opening the connection and performing the startup handshake.
    """

    sock: ByteStream

    if isinstance(address, os.PathLike) or address.startswith("/"):
        path = os.fspath(address)
        logger.debug(f"Opening connection to {path}")
        sock = await anyio.connect_unix(path)
    else:
        logger.debug(f"Opening TCP connection to {address}:{port}")
        if ssl_context is None:
            sock = await anyio.connect_tcp(address, remote_port=port)
        else:
            sock = await anyio.connect_tcp(
                address,
                remote_port=port,
                ssl_context=ssl_context,
                tls_standard_compatible=True,
            )

    connection = AMQPConnection(sock, **kwargs)
    await connection._do_startup_handshake(username, password, vhost)
    return connection


@asynccontextmanager
async def open_connection(
    address: str | PathLike[str],
    *,
    port: int = 5672,
    username: str = "guest",
    password: str = "guest",
    virtual_host: str = "/",
    ssl_context: SSLContext | None = None,
    **kwargs: Any,
) -> AsyncGenerator[AMQPConnection, None]:
    """
    Opens a new connection to the AMQP 0-9-1 server. This is an asynchronous context manager.

    Required parameters:

    :param address: The address of the server or the *absolute path* of its Unix socket.

    Optional parameters:

    :param port: The port to connect to. Ignores for Unix sockets. Defaults to 5672.
    :param username: The username to connect using. Defaults to ``guest``.
    :param password: The password to authenticate with. Defaults to ``guest``.
    :param virtual_host: The AMQP virtual host to connect to. Defaults to ``/``.
    :param ssl_context: The SSL context to connect with. Defaults to None.

    In addition, some parameters are passed directly to the class, which allows customising
    some protocol details.

    :param heartbeat_interval: The heartbeat interval to negotiate with, in seconds. This may not
                               be the actual heartbeat interval used.
    :param channel_buffer_size: The buffer size for channel messages.
    :param desired_frame_size: The maximum body message frame size desired.

    .. warning::

        This uses a :class:`~anyio.abc.TaskGroup` underneath; connection-wide closee errors will
        be transformed into a :class:`.ExceptionGroup`.
    """

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
            try:
                yield conn
            except BaseException as e:
                logger.error(f"Caught error {type(e).__qualname__}, closing connection")
                raise
            finally:
                # noinspection PyAsyncCall
                nursery.cancel_scope.cancel()
    finally:
        await conn.close()
