import attr

from serena.payloads.header import BasicHeader, ContentHeaderFrame
from serena.payloads.method import BasicDeliverPayload


@attr.s(frozen=True, slots=True)
class AMQPMessage:
    """
    The wrapper around a single, delivered AMQP message.
    """

    #: The "envelope" for the message. Wraps data about the delivery of the message.
    envelope: BasicDeliverPayload = attr.ib()

    #: The header for the message, containing application-specific details.
    header: BasicHeader = attr.ib()

    #: The actual body of this message.
    body: bytes = attr.ib()
