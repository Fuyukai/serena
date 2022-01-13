from __future__ import annotations

from typing import TYPE_CHECKING

import attr

from serena.payloads.header import BasicHeader
from serena.payloads.method import BasicDeliverPayload

if TYPE_CHECKING:
    from serena.channel import Channel


@attr.s(frozen=True, slots=True)
class AMQPMessage:
    """
    The wrapper around a single, delivered AMQP message.
    """

    _channel: Channel = attr.ib()

    #: The "envelope" for the message. Wraps data about the delivery of the message.
    envelope: BasicDeliverPayload = attr.ib()

    #: The header for the message, containing application-specific details.
    header: BasicHeader = attr.ib()

    #: The actual body of this message.
    body: bytes = attr.ib()

    async def ack(self, *, multiple: bool = False):
        """
        Acknowledges this message. See :class:`~.Channel.basic_ack`.
        """

        return await self._channel.basic_ack(self.envelope.delivery_tag, multiple=multiple)

    async def nack(self, *, multiple: bool = False, requeue: bool = True):
        """
        Negatively acknowledges this message. See :class:`~.Channel.basic_nack`.
        """

        return await self._channel.basic_nack(
            self.envelope.delivery_tag, multiple=multiple, requeue=requeue
        )

    async def reject(self, *, requeue: bool = True):
        """
        Rejects this message. See :class:`~.Channel.basic_reject`.
        """

        return await self._channel.basic_nack(self.envelope.delivery_tag, requeue=requeue)
