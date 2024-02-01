from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Self,
    TypeVar,
    cast,
)

import anyio
import outcome
from anyio import CancelScope, ClosedResourceError, EndOfStream, Event, Lock
from anyio.abc import TaskStatus
from anyio.lowlevel import checkpoint
from outcome import Error, Value
from typing_extensions import override

from serena.enums import ExchangeType
from serena.exc import (
    AMQPStateError,
    InvalidPayloadTypeError,
    MessageReturnedError,
    UnexpectedCloseError,
)
from serena.frame import BodyFrame, Frame
from serena.message import AMQPEnvelope, AMQPMessage
from serena.mixin import ChannelLike
from serena.payloads.header import BasicHeader, ContentHeaderFrame, ContentHeaderPayload
from serena.payloads.method import (
    BasicAckPayload,
    BasicCancelPayload,
    BasicConsumeOkPayload,
    BasicConsumePayload,
    BasicDeliverPayload,
    BasicGetEmptyPayload,
    BasicGetOkPayload,
    BasicGetPayload,
    BasicNackPayload,
    BasicPublishPayload,
    BasicRejectPayload,
    BasicReturnPayload,
    ChannelClosePayload,
    ChannelOpenOkPayload,
    ExchangeBindOkPayload,
    ExchangeBindPayload,
    ExchangeDeclareOkPayload,
    ExchangeDeclarePayload,
    ExchangeDeleteOkPayload,
    ExchangeDeletePayload,
    ExchangeUnBindOkPayload,
    ExchangeUnBindPayload,
    MethodFrame,
    MethodPayload,
    QueueBindOkPayload,
    QueueBindPayload,
    QueueDeclareOkPayload,
    QueueDeclarePayload,
    QueueDeleteOkPayload,
    QueueDeletePayload,
    QueuePurgeOkPayload,
    QueuePurgePayload,
    QueueUnbindOkPayload,
    QueueUnbindPayload,
)
from serena.utils import LoggerWithTrace

if TYPE_CHECKING:
    from serena.connection import AMQPConnection

logger: LoggerWithTrace = LoggerWithTrace.get(__name__)

PayloadType = TypeVar("PayloadType", bound=MethodPayload)


class Channel(ChannelLike):
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

        # Why is there both an `_open` and a `_closed`?
        # `_open` is True between ChannelOpenOk, and ChannelCloseOk, and is mostly just used for
        #  internal bookkeeping.
        # `_closed` is False when the ``async with`` block exits.

        self._open = False
        self._closed = False
        self._close_event = Event()

        # no buffer as these are for events that should return immediately
        self._send, self._receive = anyio.create_memory_object_stream[MethodFrame[MethodPayload]](0)

        self._delivery_send, self._delivery_receive = anyio.create_memory_object_stream[Frame](
            max_buffer_size=stream_buffer_size
        )

        self._close_info: ChannelClosePayload | None = None
        self._lock = Lock()

        self._is_consuming = False

        # internal state used by the connection
        # server requested a flow stop
        self._server_flow_stopped = False

        # used to count acks
        self._message_counter = 0

    @override
    def __str__(self) -> str:
        return f"<Channel id={self.id} buffered={self.current_buffer_size}>"

    __repr__: Callable[[Self], str] = __str__

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

        return not self._closed

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

    def _check_closed(self) -> None:
        """
        Checks if the channel is closed.
        """

        if self._closed:
            # todo: switch to our own exception?
            raise ClosedResourceError("This channel is closed")

    def _close(self, payload: ChannelClosePayload | None) -> None:
        """
        Closes this channel.
        """

        if self._close_info is None:
            self._close_info = payload

        self._send.close()
        self._delivery_send.close()
        self._closed = True

        self._close_event.set()

    def _enqueue_regular(self, frame: MethodFrame[MethodPayload]) -> None:
        """
        Enqueues a regular method frame.
        """

        self._send.send_nowait(frame)

    async def _enqueue_delivery(self, frame: Frame) -> None:
        """
        Enqueues a delivery frame.
        """

        await self._delivery_send.send(frame)

    async def _receive_delivery_message(self) -> AMQPMessage | None:
        """
        Receives a single delivery message. This will reassemble a full message into its constituent
        frames.
        """

        method: MethodFrame[MethodPayload] | None = None
        headers = None
        body = b""

        while True:
            # check for successful reassembly first
            if headers is not None and len(body) >= headers.payload.full_size:
                assert method is not None, "reached reassembly with invalid method"

                await checkpoint()
                # hehe payload.payload
                return AMQPMessage(
                    channel=self,
                    envelope=AMQPEnvelope.of(method.payload),
                    header=headers.payload.payload,
                    body=body,
                )

            try:
                next_frame = await self._delivery_receive.receive()
            except EndOfStream:
                if self._close_info is None:
                    raise AMQPStateError("Channel was closed improperly") from None

                raise UnexpectedCloseError.of(self._close_info) from None

            if method is None:
                if not isinstance(next_frame, MethodFrame):
                    raise AMQPStateError(f"Expected a method frame, got {next_frame} instead")

                method_frame: MethodFrame[MethodPayload] = next_frame

                if isinstance(method_frame.payload, BasicGetOkPayload | BasicDeliverPayload):
                    method = method_frame

                elif isinstance(method_frame.payload, BasicGetEmptyPayload):
                    return None

                else:
                    raise AMQPStateError(
                        "Expected Basic.Deliver or Basic.Get-Ok, "
                        f"got {method_frame.payload} instead"
                    )

            elif headers is None:
                if not isinstance(next_frame, ContentHeaderFrame):
                    raise AMQPStateError(f"Expected a header frame, got {next_frame} instead")

                # explicit type hint as pycharm incorrectly infers based on the previous if check
                payload: ContentHeaderPayload = next_frame.payload
                if payload.class_id != method.payload.klass:
                    raise AMQPStateError(
                        f"Class mismatch ({payload.class_id} != {method.payload.klass})"
                    )

                headers = next_frame

            else:
                if not isinstance(next_frame, BodyFrame):
                    raise AMQPStateError(f"Expected a body frame, got {next_frame} instead")

                body += next_frame.data

    async def _receive_frame(self) -> MethodFrame[MethodPayload]:
        """
        Receives a single frame from the channel.
        """

        self._check_closed()

        try:
            frame = await self._receive.receive()
        except EndOfStream:
            if self._close_info is None:
                raise AMQPStateError("Channel was closed improperly") from None

            raise UnexpectedCloseError.of(self._close_info) from None

        # notify connection code that we received an event so it can unblock the channel
        # noinspection PyAsyncCall
        return frame

    async def _wait_until_open(self) -> None:
        """
        Waits until the channel is open.
        """

        frame = await self._receive.receive()
        if not isinstance(frame, MethodFrame):  # type: ignore
            raise AMQPStateError(f"Expected MethodFrame, got {frame}")

        if not isinstance(frame.payload, ChannelOpenOkPayload):
            raise InvalidPayloadTypeError(ChannelOpenOkPayload, frame.payload)

        self._open = True

    async def _send_single_frame(self, payload: MethodPayload) -> None:
        """
        Sends a single frame to the connection. This won't wait for a response.
        """

        async with self._lock:
            await self._connection._send_method_frame(self._channel_id, payload)

    async def _send_and_receive(
        self, callable: Callable[[], Awaitable[None]], expected: type[PayloadType] | None
    ) -> MethodFrame[PayloadType]:
        """
        Sends and receives a payload.

        :param callable: A callable to do the actual sending.
        :param type: The type of the expected payload to return.
        :return: A :class:`.MethodFrame` that was returned as a result.
        """

        returned_frame: Value[MethodFrame[MethodPayload]] | Error = None  # type: ignore

        async def _closure(task_status: TaskStatus[None]) -> None:
            task_status.started()

            nonlocal returned_frame
            returned_frame = await outcome.acapture(self._receive_frame)

        async with anyio.create_task_group() as nursery:
            await nursery.start(_closure)

            await callable()

        returned = returned_frame.unwrap()
        if expected is not None and not isinstance(returned.payload, expected):
            raise InvalidPayloadTypeError(expected, returned.payload)

        # safe cast, we checked the type above.
        return cast(MethodFrame[PayloadType], returned)

    async def _send_and_receive_frame(
        self, payload: MethodPayload, expected_type: type[PayloadType] | None
    ) -> MethodFrame[PayloadType]:
        """
        Sends and receives a method payload.

        :param payload: The :class:`.MethodPayload` to send.
        :param type: The type of the expected payload to return.
        :return: A :class:`.MethodFrame` that was returned as a result.
        """

        async with self._lock:
            fn = partial(self._connection._send_method_frame, self._channel_id, payload)
            return await self._send_and_receive(fn, expected_type)

    async def wait_until_closed(self) -> None:
        """
        Waits until the channel is closed.
        """

        await self._close_event.wait()

    ## METHODS ##
    @override
    async def exchange_declare(
        self,
        name: str,
        type: ExchangeType | str,
        *,
        passive: bool = False,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> str:
        """
        Declares a new exchange.

        :param name: The name of the exchange. Must not be empty.
        :param type: The type of the exchange to create.
        :param passive: If True, the server will return a DeclareOk if the exchange exists, and
                        an error if it doesn't. This can be used to inspect server state without
                        modification.
        :param durable: If True, then the declared exchange will survive a server restart.
        :param auto_delete: If True, then the declared exchange will be automatically deleted
                            when all queues have finished using it.
        :param internal: If True, then the exchange may not be used directly by publishers.
        :param arguments: A dictionary of implementation-specific arguments.
        :return: The name of the exchange, as it exists on the server.
        """

        if isinstance(type, ExchangeType):
            type = type.value

        payload = ExchangeDeclarePayload(
            reserved_1=0,
            name=name,
            exchange_type=type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            no_wait=False,
            arguments=arguments or {},
        )

        await self._send_and_receive_frame(payload, ExchangeDeclareOkPayload)
        return payload.name

    @override
    async def exchange_delete(
        self,
        name: str,
        *,
        if_unused: bool = False,
    ) -> None:
        """
        Deletes an exchange.

        :param name: The name of the exchange to delete.
        :param if_unused: If True, then the exchange will only be deleted if it has no queue
                          bindings.
        :return: Nothing.
        """

        payload = ExchangeDeletePayload(
            reserved_1=0,
            name=name,
            if_unused=if_unused,
            no_wait=False,
        )

        await self._send_and_receive_frame(payload, ExchangeDeleteOkPayload)

    @override
    async def exchange_bind(
        self,
        destination: str,
        source: str,
        routing_key: str,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """
        Binds an exchange to another exchange. This is a
        `RabbitMQ extension <https://www.rabbitmq.com/e2e.html>`__ and may not be supported in other
        AMQP implementations.

        :param destination: The name of the destination exchange to bind. A blank name means the
                            default exchange.
        :param source: The name of the source exchange to bind. A blank name means the default
                       exchange.
        :param routing_key: The routing key for the exchange binding.
        :param arguments: A dictionary of implementation-specific arguments.
        :return: Nothing.
        """

        payload = ExchangeBindPayload(
            reserved_1=0,
            destination_name=destination,
            source_name=source,
            routing_key=routing_key,
            no_wait=False,
            arguments=arguments or {},
        )

        await self._send_and_receive_frame(payload, ExchangeBindOkPayload)

    @override
    async def exchange_unbind(
        self,
        destination: str,
        source: str,
        routing_key: str,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """
        Unbinds an exchange from another exchange. This is a
        `RabbitMQ extension <https://www.rabbitmq.com/e2e.html>`__ and may not be supported in other
        AMQP implementations.

        :param destination: The name of the destination exchange to unbind. A blank name means the
                            default exchange.
        :param source: The name of the source exchange to unbind. A blank name means the default
                       exchange.
        :param routing_key: The routing key for the exchange binding that is being unbinded.
        :param arguments: A dictionary of implementation-specific arguments.
        :return: Nothing.
        """

        payload = ExchangeUnBindPayload(
            reserved_1=0,
            destination_name=destination,
            source_name=source,
            routing_key=routing_key,
            no_wait=False,
            arguments=arguments or {},
        )

        await self._send_and_receive_frame(payload, ExchangeUnBindOkPayload)

    @override
    async def queue_declare(
        self,
        name: str = "",
        *,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> QueueDeclareOkPayload:
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
        :param arguments: Optional server implementation-specific arguments.
        :return: The :class:`.QueueDeclareOkPayload` the server returned.
        """

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
        return result.payload

    @override
    async def queue_bind(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """
        Binds a queue to an exchange.

        :param queue_name: The queue to bind.
        :param exchange_name: The exchange to bind to.
        :param routing_key: The routing key to use when binding.
        :param arguments: Any server-specific or exchange-specific extra arguments.
        :return: Nothing.
        """

        payload = QueueBindPayload(
            reserved_1=0,
            queue_name=queue_name,
            exchange_name=exchange_name,
            routing_key=routing_key,
            no_wait=False,
            arguments=arguments or {},
        )

        await self._send_and_receive_frame(payload, QueueBindOkPayload)

    @override
    async def queue_delete(
        self,
        queue_name: str,
        *,
        if_empty: bool = False,
        if_unused: bool = False,
    ) -> int:
        """
        Deletes a queue.

        :param queue_name: The name of the queue to delete.
        :param if_empty: If True, the queue will only be deleted if it is empty.
        :param if_unused: If True, the queue will only be deleted if it is unused.
        :return: The number of messages deleted.
        """

        payload = QueueDeletePayload(
            reserved_1=0,
            queue_name=queue_name,
            if_empty=if_empty,
            if_unused=if_unused,
            no_wait=False,
        )

        result = await self._send_and_receive_frame(payload, QueueDeleteOkPayload)
        return result.payload.message_count

    @override
    async def queue_purge(
        self,
        queue_name: str,
    ) -> int:
        """
        Purges all messages from a queue.

        :param queue_name: The name of the queue to be purged.
        :return: The number of messages deleted.
        """

        payload = QueuePurgePayload(
            reserved_1=0,
            name=queue_name,
            no_wait=False,
        )

        result = await self._send_and_receive_frame(payload, QueuePurgeOkPayload)
        return result.payload.message_count

    @override
    async def queue_unbind(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """
        Unbinds a queue from an exchange.

        :param queue_name: The name of the queue to unbind.
        :param exchange_name: The name of the exchange to unbind from.
        :param routing_key: The routing key to unbind using.
        :param arguments: Implementation-specific arguments to use.
        """

        payload = QueueUnbindPayload(
            reserved_1=0,
            queue_name=queue_name,
            exchange_name=exchange_name,
            routing_key=routing_key,
            no_wait=False,
            arguments=arguments or {},
        )

        await self._send_and_receive_frame(payload, QueueUnbindOkPayload)

    @asynccontextmanager
    @override
    async def basic_consume(
        self,
        queue_name: str,
        consumer_tag: str = "",
        *,
        no_local: bool = False,
        no_ack: bool = False,
        exclusive: bool = False,
        auto_ack: bool = True,
        arguments: dict[str, Any] | None = None,
    ) -> AsyncGenerator[AsyncIterable[AMQPMessage], None]:
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
        :param auto_ack: If True, then messages will be automatically positively acknowledged
                         in the generator loop. Has no effect if ``no_ack`` is True. This is a
                         Serena-exclusive feature, not a protocol feature.
        """

        if self._is_consuming:
            raise AMQPStateError("Cannot start two consumers on the same channel")

        async def _agen() -> AsyncGenerator[AMQPMessage, None]:
            while True:
                message = await self._receive_delivery_message()
                if message is None:
                    # what?
                    continue

                yield message

                if auto_ack:
                    await self.basic_ack(message.envelope.delivery_tag)

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
            self._is_consuming = True
            async with aclosing(_agen()) as agen:
                yield agen
        finally:
            cancel_payload = BasicCancelPayload(
                consumer_tag=response.payload.consumer_tag,
                no_wait=False,
            )

            with CancelScope(shield=True):
                if not self._closed:
                    await self._send_and_receive_frame(cancel_payload, None)

                self._is_consuming = False

    @override
    async def basic_publish(
        self,
        exchange_name: str,
        routing_key: str,
        body: bytes,
        *,
        header: BasicHeader | None = None,
        mandatory: bool = True,
        immediate: bool = False,
    ) -> None:
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
        :raise MessageReturnedError: If the message was returned to the publisher.

        .. warning::

            The immediate flag is *not* supported in RabbitMQ 3.x, and will cause the connection
            to close.
        """

        # we have to send three manual frames before we get an ACK

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
            response: MethodFrame[MethodPayload]
            if body:
                # Body is not empty, we send_and_receive with the body and not the headers.
                await self._connection._send_header_frame(
                    self._channel_id,
                    method_klass=method_payload.klass,
                    body_length=len(body),
                    headers=headers,
                )

                # 3) Body
                fn = partial(self._connection._send_body_frames, self._channel_id, body)
                response = await self._send_and_receive(fn, None)

            else:
                # Body is empty, we send_and_receive with the headers.
                fn = partial(
                    self._connection._send_header_frame,
                    self._channel_id,
                    method_klass=method_payload.klass,
                    body_length=len(body),
                    headers=headers,
                )

                response = await self._send_and_receive(fn, None)

            self._message_counter += 1

            # 4) check for Ack or Return
            payload = response.payload

            if isinstance(payload, BasicAckPayload):
                if payload.delivery_tag != self._message_counter:
                    raise AMQPStateError(
                        f"Expected Ack for delivery tag {self._message_counter}, "
                        f"but got Ack for delivery tag {payload.delivery_tag}"
                    )
                logger.trace(f"C#{self.id}: Server ACKed published message")

            elif isinstance(payload, BasicReturnPayload):
                raise MessageReturnedError(
                    exchange=payload.exchange,
                    routing_key=payload.routing_key,
                    reply_code=payload.reply_code,
                    reply_text=payload.reply_text,
                )

            elif isinstance(payload, BasicNackPayload):
                raise AMQPStateError("Server NACKed message to be published")

    # note to self: these are only defined on Channel cos they make no sense to be defined on
    # pools. oops!

    async def basic_ack(self, delivery_tag: int, *, multiple: bool = False) -> None:
        """
        Acknowledges AMQP messages.

        :param delivery_tag: The delivery tag of the message to acknowledge.
        :param multiple: If True, then all messages up to and including the message specified will
                         be acknowledged, not just the message specified.
        """

        payload = BasicAckPayload(delivery_tag, multiple)
        await self._send_single_frame(payload)

    async def basic_reject(self, delivery_tag: int, *, requeue: bool = True) -> None:
        """
        Rejects an AMQP message.

        .. note::

            If you are using RabbitMQ, you might want to use :meth:`~.Channel.nack` instead.

        :param delivery_tag: The delivery tag of the message to acknowledge.
        :param requeue: If True, then the rejected message will be requeued if possible.
        """

        payload = BasicRejectPayload(delivery_tag, requeue)
        await self._send_single_frame(payload)

    async def basic_nack(
        self,
        delivery_tag: int,
        *,
        multiple: bool = False,
        requeue: bool = False,
    ) -> None:
        """
        Rejects an AMQP message. This is a RabbitMQ-specific extension.

        :param delivery_tag: The delivery tag of the message to acknowledge.
        :param multiple: If True, then all messages up to and including the message specified will
                         be acknowledged, not just the message specified.
        :param requeue: If True, then the rejected message will be requeued if possible.
        """

        payload = BasicNackPayload(delivery_tag, multiple, requeue)
        await self._send_single_frame(payload)

    @override
    async def basic_get(self, queue: str, *, no_ack: bool = False) -> AMQPMessage | None:
        """
        Gets a single message from a queue.

        :param queue: The queue to get the message from.
        :param no_ack: Iff not True, then messages will need to be explicitly acknowledged on
                       consumption.
        :return: A :class:`.AMQPMessage` if one existed on the queue, otherwise None.
        """

        payload = BasicGetPayload(reserved_1=0, queue_name=queue, no_ack=no_ack)
        await self._send_single_frame(payload)
        return await self._receive_delivery_message()
