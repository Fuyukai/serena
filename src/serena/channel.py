from __future__ import annotations

import logging
from contextlib import aclosing, asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncIterable,
    Dict,
    Optional,
    Type,
    TypeVar,
    cast,
)

import anyio
from anyio import CancelScope, ClosedResourceError, EndOfStream, Event, Lock
from anyio.lowlevel import checkpoint

from serena.exc import AMQPStateError, UnexpectedCloseError
from serena.frame import BodyFrame, Frame
from serena.message import AMQPMessage
from serena.payloads.header import BasicHeader, ContentHeaderFrame, ContentHeaderPayload
from serena.payloads.method import (
    BasicCancelPayload,
    BasicConsumeOkPayload,
    BasicConsumePayload,
    BasicDeliverPayload,
    BasicPublishPayload,
    ChannelClosePayload,
    ChannelOpenOkPayload,
    MethodFrame,
    MethodPayload,
    QueueDeclareOkPayload,
    QueueDeclarePayload,
    method_payload_name,
)

if TYPE_CHECKING:
    from serena.connection import AMQPConnection

_PAYLOAD = TypeVar("_PAYLOAD", bound=MethodPayload)

logger = logging.getLogger()


# noinspection PyProtectedMember
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

        # no buffer as these are for events that should return immediately
        self._send, self._receive = anyio.create_memory_object_stream(0)

        self._delivery_send, self._delivery_receive = anyio.create_memory_object_stream(
            max_buffer_size=stream_buffer_size
        )

        self._close_info: Optional[ChannelClosePayload] = None

        self._recv_wakeup = Event()
        self._lock = Lock()

        # internal state used by the connection
        # server requested a flow stop
        self._server_flow_stopped = False
        # we requested a flow stop
        self._client_flow_stopped = False

        # used to decompose

    def __str__(self):
        return f"<Channel id={self.id} buffered={self.current_buffer_size}>"

    __repr__ = __str__

    @property
    def id(self) -> int:
        """
        Returns the ID of this channel.
        """

        return self._channel_id

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

        return int(self._delivery_send.statistics().max_buffer_size)

    @property
    def current_buffer_size(self) -> int:
        """
        Returns the current number of frames buffered in this channel. Used internally.
        """

        return self._delivery_send.statistics().current_buffer_used

    def _check_closed(self):
        """
        Checks if the channel is closed.
        """

        if not self._open:
            # todo: switch to our own exception?
            raise ClosedResourceError("This channel is closed")

    async def _close(self, payload: ChannelClosePayload):
        """
        Closes this channel.
        """

        await self._receive.aclose()
        await self._delivery_receive.aclose()
        self._open = False
        self._close_info = payload

        # aclose doesn't seem to checkpoint...
        await checkpoint()

    async def _enqueue_regular(self, frame: MethodFrame):
        """
        Enqueues a regular method frame.
        """

        await self._send.send(frame)

    async def _enqueue_delivery(self, frame: Frame):
        """
        Enqueues a delivery frame.
        """

        await self._delivery_send.send(frame)

    async def _receive_delivery_message(self):
        """
        Receives a single delivery message. This will reassemble a full message into its constituent
        frames.
        """

        method = None
        headers = None
        body = b""

        while True:
            # check for successful reassembly first
            if headers is not None and len(body) >= headers.payload.full_size:
                await checkpoint()
                # hehe payload.payload
                return AMQPMessage(
                    envelope=method.payload,
                    header=headers.payload.payload,
                    body=body,
                )

            next_frame = await self._delivery_receive.receive()

            if method is None:
                if not isinstance(next_frame, MethodFrame):
                    raise AMQPStateError(f"Expected a method frame, got {next_frame} instead")

                if not isinstance(next_frame.payload, BasicDeliverPayload):
                    raise AMQPStateError(
                        f"Expected basic.deliver, got {next_frame.payload} instead"
                    )

                method = next_frame

            elif headers is None:
                if not isinstance(next_frame, ContentHeaderFrame):
                    raise AMQPStateError(f"Expected a header frame, got {next_frame} instead")

                # explicit type hint as pycharm incorrectly infers based on the previous if check
                payload: ContentHeaderPayload = next_frame.payload  # type: ignore
                if payload.class_id != method.payload.klass:
                    raise AMQPStateError(
                        f"Class mismatch ({payload.class_id} != {method.payload.klass})"
                    )

                headers = next_frame

            else:
                if not isinstance(next_frame, BodyFrame):
                    raise AMQPStateError(f"Expected a body frame, got {next_frame} instead")

                body += next_frame.data

    async def _receive_frame(self) -> Frame:
        """
        Receives a single frame from the channel.
        """

        self._check_closed()

        try:
            frame = await self._receive.receive()
        except EndOfStream:
            if self._close_info is None:
                raise AMQPStateError("Channel was closed improperly")

            raise UnexpectedCloseError.of(self._close_info) from None

        # notify connection code that we received an event so it can unblock the channel
        # noinspection PyAsyncCall
        self._recv_wakeup.set()
        self._recv_wakeup = Event()
        return frame

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

    async def _send_and_receive_frame(
        self, payload: MethodPayload, type: Type[_PAYLOAD] = None
    ) -> MethodFrame[_PAYLOAD]:
        """
        Sends and receives a method payload.

        :param payload: The :class:`.MethodPayload` to send.
        :param type: The type of the expected payload to return.
        :return: A :class:`.MethodFrame` that was returned as a result.
        """

        async with self._lock:
            await self._connection._send_method_frame(self._channel_id, payload)
            return cast(MethodFrame[type], await self._receive_frame())

    ## METHODS ##
    async def queue_declare(
        self,
        name: str = "",
        *,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Dict[str, Any] = None,
    ):
        """
        Declares a queue.

        :param name: The name of the queue. If blank, a name will be automatically generated by
                     the server and returned.
        :param passive: If True, the server will return a DeclareOk if the queue exists, and an
                        error if it doesn't. This can be used to inspect server state without
                        modification.
        :param durable: If True, the queue being created will persist past server restarts.
        :param exclusive: If True, this queue will only belong to this connection, and will be
                          automatically deleted when the connection closes. Best combined with an
                          automatically generated queue name.
        :param auto_delete: If True, this queue will be automatically deleted after all consumers
                            have finished. The queue will never be deleted before the first consumer
                            starts.
        :param arguments: An optional dictionary of server implementation-specific arguments.
        :return: The name of the declared queue.
        """

        # TODO: Expose the other two parameters of QueueDeclareOk.

        self._check_closed()

        payload = QueueDeclarePayload(
            reserved_1=0,
            name=name or "",
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            no_wait=False,
            arguments=arguments or {},
        )

        result = await self._send_and_receive_frame(payload, QueueDeclareOkPayload)
        return result.payload.name

    async def basic_publish(
        self,
        exchange_name: str,
        routing_key: str,
        body: bytes,
        *,
        header: BasicHeader = None,
        mandatory: bool = True,
        immediate: bool = False,
    ):
        """
        Publishes a message to a specific exchange.

        :param exchange_name: The name of the exchange to publish to. This can be blank to mean the
                              default exchange.
        :param routing_key: The routing key to publish to.
        :param body: The body for this payload.
        :param header: The headers to use for this message. If unset, will use the default blank
                       headers.
        :param mandatory: Iff True, the server must return a ``Return`` message if the message
                          could not be routed to a queue.
        :param immediate: Iff True, the server must return a ``Return`` message if the message could
                          not be immediately consumed.

        .. warning::

            The immediate flag is *not* supported in RabbitMQ 3.x, and will cause the connection
            to close.
        """

        # this is slightly different as the server doesn't (normally) respond, and we have to send
        # manual frames.

        self._check_closed()

        async with self._lock:
            method_payload = BasicPublishPayload(
                reserved_1=1,
                name=exchange_name,
                routing_key=routing_key,
                mandatory=mandatory,
                immediate=immediate,
            )

            # 1) method frame
            await self._connection._send_method_frame(self._channel_id, method_payload)
            # 2) header frame
            headers = header or BasicHeader()
            await self._connection._send_header_frame(
                self._channel_id,
                method_klass=method_payload.klass,
                body_length=len(body),
                headers=headers,
            )

            # 3) body
            await self._connection._send_body_frames(self._channel_id, body)

    def basic_consume(
        self,
        queue_name: str,
        consumer_tag: str = "",
        *,
        no_local: bool = False,
        no_ack: bool = False,
        exclusive: bool = False,
        arguments: Dict[str, Any] = None,
    ) -> AsyncContextManager[AsyncIterable[AMQPMessage]]:
        """
        Starts a basic consume operation. This returns an async context manager over an asynchronous
        iterator that yields incoming :class:`.AMQPMessage` instances.

        The channel can still be used for other operations during this operation.

        :param queue_name: The name of the queue to consume from.
        :param consumer_tag: The tag for this consume.
        :param no_local: If True, messages will not be sent to this consumer if it is on the same
                         connection that published them.
        :param no_ack: If True, messages will not be expected to be acknowledged. This can cause
                       data loss.
        :param exclusive: If True, then only this consumer can access the queue. Will fail if there
                          is another consumer already active.
        :param arguments: Implementation-specific arguments.
        """

        async def _agen():
            while True:
                yield (await self._receive_delivery_message())

        @asynccontextmanager
        async def _do():
            payload = BasicConsumePayload(
                reserved_1=0,
                queue_name=queue_name,
                consumer_tag=consumer_tag,
                no_local=no_local,
                no_ack=no_ack,
                exclusive=exclusive,
                no_wait=False,
                arguments=arguments or {},
            )

            response = await self._send_and_receive_frame(payload, BasicConsumeOkPayload)

            try:
                async with aclosing(_agen()) as agen:
                    yield agen
            finally:
                cancel_payload = BasicCancelPayload(
                    consumer_tag=response.payload.consumer_tag,
                    no_wait=False,
                )

                with CancelScope(shield=True):
                    await self._send_and_receive_frame(cancel_payload)

        return _do()
