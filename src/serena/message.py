from __future__ import annotations

from typing import TYPE_CHECKING

import attr

from serena.payloads.header import BasicHeader
from serena.payloads.method import (
    BasicDeliverPayload,
    BasicGetOkPayload,
    MethodPayload,
    method_payload_name,
)

if TYPE_CHECKING:
    from serena.channel import Channel


@attr.s(frozen=True, slots=True)
class AMQPEnvelope:
    """
    Wraps metadata related to the *delivery* of the message.
    """

    #: The identifier for the consumer. May be None if this message was gotten synchronously.
    consumer_tag: str | None = attr.ib()

    #: The server-assigned delivery tag.
    delivery_tag: int = attr.ib()

    #: Indicates that the message has been previously delivered.
    redelivered: bool = attr.ib()

    #: The name of the exchange the message was originally published to.
    exchange_name: str = attr.ib()

    #: The routing key for the message.
    routing_key: str = attr.ib()

    #: The messages remaining in the queue. May be None if this message was consumed asynchronously.
    message_count: int | None = attr.ib()

    @classmethod
    def from_deliver(cls, payload: BasicDeliverPayload) -> AMQPEnvelope:
        """
        Creates a new envelope from a :class:`.BasicDeliverPayload.`
        """

        return AMQPEnvelope(
            consumer_tag=payload.consumer_tag,
            delivery_tag=payload.delivery_tag,
            redelivered=payload.redelivered,
            exchange_name=payload.exchange_name,
            routing_key=payload.routing_key,
            message_count=None,
        )

    @classmethod
    def from_get(cls, payload: BasicGetOkPayload) -> AMQPEnvelope:
        """
        Creates a new envelope from a :class:`.BasicGetOkPayload`.
        """

        return AMQPEnvelope(
            consumer_tag=None,
            delivery_tag=payload.delivery_tag,
            redelivered=payload.redelivered,
            exchange_name=payload.exchange_name,
            routing_key=payload.routing_key,
            message_count=payload.message_count,
        )

    @classmethod
    def of(cls, payload: MethodPayload) -> AMQPEnvelope:
        """
        Creates a new envelope from a method payload.
        """

        if isinstance(payload, BasicDeliverPayload):
            return cls.from_deliver(payload)

        if isinstance(payload, BasicGetOkPayload):
            return cls.from_get(payload)

        raise TypeError(
            f"Expected Basic.Deliver or Basic.Get-Ok, got {method_payload_name(payload)}"
        )


@attr.s(frozen=True, slots=True)
class AMQPMessage:
    """
    The wrapper around a single, delivered AMQP message.
    """

    _channel: Channel = attr.ib(alias="channel")  # explicit alias makes mypy/pyright/etc happy

    #: The "envelope" for the message. Wraps data about the delivery of the message.
    envelope: AMQPEnvelope = attr.ib()

    #: The header for the message, containing application-specific details.
    header: BasicHeader = attr.ib()

    #: The actual body of this message.
    body: bytes = attr.ib()

    async def ack(self, *, multiple: bool = False) -> None:
        """
        Acknowledges this message. See :class:`~.Channel.basic_ack`.
        """

        return await self._channel.basic_ack(self.envelope.delivery_tag, multiple=multiple)

    async def nack(self, *, multiple: bool = False, requeue: bool = True) -> None:
        """
        Negatively acknowledges this message. See :class:`~.Channel.basic_nack`.
        """

        return await self._channel.basic_nack(
            self.envelope.delivery_tag, multiple=multiple, requeue=requeue
        )

    async def reject(self, *, requeue: bool = True) -> None:
        """
        Rejects this message. See :class:`~.Channel.basic_reject`.
        """

        return await self._channel.basic_reject(self.envelope.delivery_tag, requeue=requeue)
