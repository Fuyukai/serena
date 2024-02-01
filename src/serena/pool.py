from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
)

import anyio
from anyio import Event, WouldBlock
from typing_extensions import override

from serena.channel import Channel
from serena.enums import ExchangeType
from serena.exc import UnexpectedCloseError
from serena.message import AMQPMessage
from serena.mixin import ChannelLike
from serena.payloads.header import BasicHeader
from serena.payloads.method import QueueDeclareOkPayload

if TYPE_CHECKING:
    from serena.connection import AMQPConnection


class ChannelPool(ChannelLike):
    """
    A pooled container of multiple channels that simplifies both usage and error handling.
    """

    def __init__(self, connection: AMQPConnection, initial_size: int = 64):
        """
        :param connection: The :class:`.AMQPConnection` to use.
        :param initial_size: The initial size of the pool top use. It may grow.
        """

        self._conn = connection
        self._qwrite, self._qread = anyio.create_memory_object_stream[Channel](
            max_buffer_size=initial_size
        )

        self._needs_new_connection = Event()

    @property
    def idle_channels(self) -> int:
        """
        Returns the number of idle channels.
        """

        return self._qread.statistics().current_buffer_used

    @property
    def open_channels(self) -> int:
        """
        Returns the number of open channels.
        """

        return int(self._qread.statistics().max_buffer_size)

    def _return_channel(self, channel: Channel) -> None:
        self._qwrite.send_nowait(channel)

    async def _open(self, initial_size: int) -> None:
        """
        Opens the pool and adds channels.
        """

        for _ in range(0, initial_size):
            self._qwrite.send_nowait(await self._conn._open_channel())

    async def _close(self) -> None:
        """
        Closes all channels in the pool.
        """

        async with anyio.create_task_group() as nursery:
            while True:
                try:
                    next_channel = self._qread.receive_nowait()
                except WouldBlock:
                    self._qread.close()
                    break

                nursery.start_soon(next_channel.wait_until_closed)
                await self._conn._close_channel(next_channel.id)

    async def _open_channels(self) -> NoReturn:
        """
        Opens channels in an infinite loop.
        """

        while True:
            await self._needs_new_connection.wait()
            channel = await self._conn._open_channel()
            self._qwrite.send_nowait(channel)

    @asynccontextmanager
    async def checkout(self) -> AsyncGenerator[Channel, None]:
        """
        Checks out a new channel from the pool, and uses it persistently.

        The channel lifetime will be automatically managed for you.
        """

        channel = await self._qread.receive()

        try:
            yield channel
        except UnexpectedCloseError:
            # noinspection PyAsyncCall
            self._needs_new_connection.set()
            self._needs_new_connection = Event()
            raise

        # gross!
        except BaseException:
            self._return_channel(channel)
            raise
        else:
            self._return_channel(channel)

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

        async with (
            self.checkout() as channel,
            channel.basic_consume(
                queue_name=queue_name,
                consumer_tag=consumer_tag,
                no_local=no_local,
                no_ack=no_ack,
                exclusive=exclusive,
                auto_ack=auto_ack,
                arguments=arguments,
            ) as it,
        ):
            yield it

    @override
    async def basic_get(self, queue: str, *, no_ack: bool = False) -> AMQPMessage | None:
        """
        Gets a single message from a queue.

        :param queue: The queue to get the message from.
        :param no_ack: Iff not True, then messages will need to be explicitly acknowledged on
                       consumption.
        :return: A :class:`.AMQPMessage` if one existed on the queue, otherwise None.
        """

        async with self.checkout() as channel:
            return await channel.basic_get(
                queue=queue,
                no_ack=no_ack,
            )

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

        async with self.checkout() as channel:
            await channel.basic_publish(
                exchange_name=exchange_name,
                routing_key=routing_key,
                body=body,
                header=header,
                mandatory=mandatory,
                immediate=immediate,
            )

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

        async with self.checkout() as channel:
            await channel.exchange_bind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments,
            )

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

        async with self.checkout() as channel:
            return await channel.exchange_declare(
                name=name,
                type=type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                arguments=arguments,
            )

    @override
    async def exchange_delete(self, name: str, *, if_unused: bool = False) -> None:
        """
        Deletes an exchange.
        :param name: The name of the exchange to delete.
        :param if_unused: If True, then the exchange will only be deleted if it has no queue
                          bindings.
        :return: Nothing.

        """

        async with self.checkout() as channel:
            await channel.exchange_delete(
                name=name,
                if_unused=if_unused,
            )

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

        async with self.checkout() as channel:
            await channel.exchange_unbind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments,
            )

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

        async with self.checkout() as channel:
            await channel.queue_bind(
                queue_name=queue_name,
                exchange_name=exchange_name,
                routing_key=routing_key,
                arguments=arguments,
            )

    @override
    async def queue_declare(
        self,
        name: str,
        *,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> QueueDeclareOkPayload:
        """
        Declares a queue.

        :param name: The name of the queue.
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

        async with self.checkout() as channel:
            return await channel.queue_declare(
                name=name,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments,
            )

    @override
    async def queue_delete(
        self, queue_name: str, *, if_empty: bool = False, if_unused: bool = False
    ) -> int:
        """
        Deletes a queue.

        :param queue_name: The name of the queue to delete.
        :param if_empty: If True, the queue will only be deleted if it is empty.
        :param if_unused: If True, the queue will only be deleted if it is unused.
        :return: The number of messages deleted.
        """

        async with self.checkout() as channel:
            return await channel.queue_delete(
                queue_name=queue_name,
                if_empty=if_empty,
                if_unused=if_unused,
            )

    @override
    async def queue_purge(self, queue_name: str) -> int:
        """
        Purges all messages from a queue.

        :param queue_name: The name of the queue to be purged.
        :return: The number of messages deleted.
        """

        async with self.checkout() as channel:
            return await channel.queue_purge(
                queue_name=queue_name,
            )

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

        async with self.checkout() as channel:
            return await channel.queue_unbind(
                queue_name=queue_name,
                exchange_name=exchange_name,
                routing_key=routing_key,
                arguments=arguments,
            )
