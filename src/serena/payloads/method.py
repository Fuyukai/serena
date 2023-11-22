from __future__ import annotations

import abc
from typing import Any, ClassVar, Generic, TypeVar

import attr

from serena.enums import ClassID, ReplyCode
from serena.frame import Frame, FrameType
from serena.payloads.encoding import (
    aq_type,
    decode_attrs_attribute,
    encode_attrs_attribute,
)
from serena.utils.buffer import DecodingBuffer, EncodingBuffer

# mypy - doesn't understand converter=ReplyCode
# pyright - does understand it, but thinks its bullshit
# if we use a lambda, mypy gets pissy with an unannotated function error.
# so NAMED FUNCTION IT IS!


def _fuck_fuck_fuck(what: int) -> ReplyCode:
    return ReplyCode(what)


@attr.s(frozen=True, slots=True, init=True)
class MethodPayload(abc.ABC):
    """
    Base class for all method payloads.
    """

    #: The class ID for this payload.
    klass: ClassVar[ClassID]

    #: The method ID for this payload. These are shared between classes and are not unique.
    method: ClassVar[int]

    #: If this is a payload sent FROM the client. This may be True on payloads received by the
    #: client, too.
    is_client_side: ClassVar[bool]


_PAYLOAD_TYPE = TypeVar("_PAYLOAD_TYPE", bound=MethodPayload)


@attr.s(frozen=True, slots=True)
class MethodFrame(Frame, Generic[_PAYLOAD_TYPE]):
    """
    A frame that carries a method body.
    """

    type = FrameType.METHOD

    #: The payload for this frame.
    payload: _PAYLOAD_TYPE = attr.ib()


## == CONNECTION == ##
@attr.s(frozen=True, slots=True)
class ConnectionStartPayload(MethodPayload):
    """
    Payload for the ``start`` method.
    """

    klass = ClassID.CONNECTION
    method = 10
    is_client_side = False

    #: The major version of the protocol.
    version_major: int = attr.ib(metadata=aq_type("octet"))

    #: The minor version of the protocol.
    version_minor: int = attr.ib(metadata=aq_type("octet"))

    #: The server properties.
    properties: dict[str, Any] = attr.ib()

    #: The available security mechanisms.
    mechanisms: bytes = attr.ib()

    #: The available message locales.
    locales: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class ConnectionStartOkPayload(MethodPayload):
    """
    Payload for the ``start-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 11
    is_client_side = True

    #: The client properties.
    properties: dict[str, Any] = attr.ib()

    #: The selected security mechanism.
    mechanism: str = attr.ib()

    #: The security response data.
    response: bytes = attr.ib()

    #: The selected locale.
    locale: str = attr.ib()


@attr.s(frozen=True, slots=True)
class ConnectionSecurePayload(MethodPayload):
    """
    Payload for the ``secure`` method.
    """

    klass = ClassID.CONNECTION
    method = 20
    is_client_side = False

    #: The security challenge data.
    challenge: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class ConnectionSecureOkPayload(MethodPayload):
    """
    Payload for the ``secure-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 21
    is_client_side = True

    #: The security response data.
    response: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class ConnectionTunePayload(MethodPayload):
    """
    Payload for the ``tune`` method.
    """

    klass = ClassID.CONNECTION
    method = 30
    is_client_side = False

    #: The server's proposed maximum channels.
    max_channels: int = attr.ib(metadata=aq_type("short"))

    #: The server's proposed maximum frame size.
    max_frame_size: int = attr.ib(metadata=aq_type("long"))

    #: The server's desired heartbeat delay.
    heartbeat_delay: int = attr.ib(metadata=aq_type("short"))


@attr.s(frozen=True, slots=True)
class ConnectionTuneOkPayload(MethodPayload):
    """
    Payload for the ``tune-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 31
    is_client_side = True

    #: The client's negotiated maximum channels.
    max_channels: int = attr.ib(metadata=aq_type("short"))

    #: The client's negotiated maximum frame size.
    max_frame_size: int = attr.ib(metadata=aq_type("long"))

    #: The client's desired heartbeat delay.
    heartbeat_delay: int = attr.ib(metadata=aq_type("short"))


@attr.s(frozen=True, slots=True)
class ConnectionOpenPayload(MethodPayload):
    """
    Payload for the ``open`` method.
    """

    klass = ClassID.CONNECTION
    method = 40
    is_client_side = False

    #: The virtual host to open a connection to.
    virtual_host: str = attr.ib()

    reserved_1: str = attr.ib(default="")
    reserved_2: bool = attr.ib(default=True)


@attr.s(frozen=True, slots=True)
class ConnectionOpenOkPayload(MethodPayload):
    """
    Payload for the ``open-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 41
    is_client_side = True

    reserved_1: str = attr.ib()


@attr.s(frozen=True, slots=True)
class ConnectionClosePayload(MethodPayload):
    """
    Payload for the ``close`` method.
    """

    klass = ClassID.CONNECTION
    method = 50
    is_client_side = True

    #: The code for the error that caused this close.
    reply_code: ReplyCode = attr.ib(converter=_fuck_fuck_fuck, metadata=aq_type("short"))

    #: The text for the error that caused this close.
    reply_text: str = attr.ib()

    #: The class of the method that caused this close.
    class_id: int = attr.ib(metadata=aq_type("short"))

    #: The class of the method that caused this close.
    method_id: int = attr.ib(metadata=aq_type("short"))


@attr.s(frozen=True, slots=True)
class ConnectionCloseOkPayload(MethodPayload):
    """
    Payload (empty) for the ``close-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 51
    is_client_side = True


## == CHANNEL == ##
@attr.s(frozen=True, slots=True)
class ChannelOpenPayload(MethodPayload):
    """
    Payload for the ``open`` method.
    """

    klass = ClassID.CHANNEL
    method = 10
    is_client_side = False

    reserved_1: str = attr.ib(default="")


@attr.s(frozen=True, slots=True)
class ChannelOpenOkPayload(MethodPayload):
    """
    Payload for the ``open-ok`` method.
    """

    klass = ClassID.CHANNEL
    method = 11
    is_client_side = True

    reserved_1: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class ChannelFlowPayload(MethodPayload):
    """
    Payload for the ``flow`` method.
    """

    klass = ClassID.CHANNEL
    method = 20
    is_client_side = True

    #: If the channel should start processing messages again or not.
    active: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class ChannelFlowOkPayload(MethodPayload):
    """
    Payload for the ``flow-ok`` method.
    """

    klass = ClassID.CHANNEL
    method = 21
    is_client_side = True

    #: See :attr:`.FlowPayload.active`.
    active: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class ChannelClosePayload(MethodPayload):
    """
    Payload for the ``close`` method.
    """

    klass = ClassID.CHANNEL
    method = 40
    is_client_side = True

    #: The code for the error that caused this close.
    reply_code: ReplyCode = attr.ib(converter=_fuck_fuck_fuck, metadata=aq_type("short"))

    #: The text for the error that caused this close.
    reply_text: str = attr.ib()

    #: The class of the method that caused this close.
    class_id: int = attr.ib(metadata=aq_type("short"))

    #: The class of the method that caused this close.
    method_id: int = attr.ib(metadata=aq_type("short"))


@attr.s(frozen=True, slots=True)
class ChannelCloseOkPayload(MethodPayload):
    """
    Payload for the ``close-ok`` method.
    """

    klass = ClassID.CHANNEL
    method = 41
    is_client_side = True

    # empty body


## EXCHANGE ##
@attr.s(frozen=True, slots=True)
class ExchangeDeclarePayload(MethodPayload):
    """
    Payload for the ``declare`` method.
    """

    klass = ClassID.EXCHANGE
    method = 10
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the exchange to be created.
    name: str = attr.ib()

    #: The type of the exchange to be created.
    exchange_type: str = attr.ib()

    #: If True, no Declare-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: If True, the exchange may not bee used ddirectly by publishers, but only when bound to other
    #: exchanges.
    internal: bool = attr.ib()

    #: If True, the exchange will be deleted when all queues have finished using it.
    auto_delete: bool = attr.ib()

    #: If True, the the exchange will survive server restarts.
    durable: bool = attr.ib()

    #: If True, the server will return a DeclareOk if the exchange exists, and an error if it
    #: doesn't.
    passive: bool = attr.ib()

    #: Implementation-specific arguments for the declaration.
    arguments: dict[str, Any] = attr.ib()


@attr.s(frozen=True, slots=True)
class ExchangeDeclareOkPayload(MethodPayload):
    """
    Payload for the ``declare-ok`` method.
    """

    klass = ClassID.EXCHANGE
    method = 11
    is_client_side = True

    # empty body


@attr.s(frozen=True, slots=True)
class ExchangeDeletePayload(MethodPayload):
    """
    Payload for the ``delete`` method.
    """

    klass = ClassID.EXCHANGE
    method = 20
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the exchange to delete.
    name: str = attr.ib()

    #: If True, no Declare-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: If True, the server will only delete the exchange if there are no queue bindings.
    if_unused: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class ExchangeDeleteOkPayload(MethodPayload):
    """
    Payload for the ``delete-ok`` method.
    """

    klass = ClassID.EXCHANGE
    method = 21
    is_client_side = True

    # empty body


@attr.s(frozen=True, slots=True)
class ExchangeBindPayload(MethodPayload):
    """
    Payload for the ``bind`` method.
    """

    klass = ClassID.EXCHANGE
    method = 30
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the destination exchange to bind.
    destination_name: str = attr.ib()

    #: The name of the source exchange to bind.
    source_name: str = attr.ib()

    #: The routing key to bind using.
    routing_key: str = attr.ib()

    #: If True, no Bind-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: Implementation-specific arguments for the declaration.
    arguments: dict[str, Any] = attr.ib()


@attr.s(frozen=True, slots=True)
class ExchangeBindOkPayload(MethodPayload):
    """
    Payload for the ``bind-ok`` method.
    """

    klass = ClassID.EXCHANGE
    method = 31
    is_client_side = True

    # empty body


@attr.s(frozen=True, slots=True)
class ExchangeUnBindPayload(MethodPayload):
    """
    Payload for the ``unbind`` method.
    """

    klass = ClassID.EXCHANGE
    method = 40
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the destination exchange to bind.
    destination_name: str = attr.ib()

    #: The name of the source exchange to bind.
    source_name: str = attr.ib()

    #: The routing key to bind using.
    routing_key: str = attr.ib()

    #: If True, no Bind-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: Implementation-specific arguments for the declaration.
    arguments: dict[str, Any] = attr.ib()


@attr.s(frozen=True, slots=True)
class ExchangeUnBindOkPayload(MethodPayload):
    """
    Payload for the ``bind-ok`` method.
    """

    klass = ClassID.EXCHANGE
    method = 41
    is_client_side = True

    # empty body


## QUEUE ##
@attr.s(frozen=True, slots=True)
class QueueDeclarePayload(MethodPayload):
    """
    Payload for the ``declare`` method.
    """

    klass = ClassID.QUEUE
    method = 10
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the queue being declared. May be empty.
    name: str = attr.ib()

    #: If True, no Declare-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: If True, then the queue is automatically deleted when all consumers are finished using it.
    auto_delete: bool = attr.ib()

    #: If True, then the queue is exclusive to this connection.
    exclusive: bool = attr.ib()

    #: If True, then the queue will persist through restarts.
    durable: bool = attr.ib()

    #: If True, the server will return a DeclareOk if the queue exists, and an error if it doesn't.
    passive: bool = attr.ib()

    #: Implementation-specific arguments for the declaration.
    arguments: dict[str, Any] = attr.ib()


@attr.s(frozen=True, slots=True)
class QueueDeclareOkPayload(MethodPayload):
    """
    Payload for the ``declare-ok`` method.
    """

    klass = ClassID.QUEUE
    method = 11
    is_client_side = True

    #: The name of the queue.
    name: str = attr.ib()

    #: The number of the messages present in the queue.
    message_count: int = attr.ib(metadata=aq_type("long"))

    #: The number of consumers consuming from the queue.
    consumer_count: int = attr.ib(metadata=aq_type("long"))


@attr.s(frozen=True, slots=True)
class QueueBindPayload(MethodPayload):
    """
    Payload for the ``bind`` method.
    """

    klass = ClassID.QUEUE
    method = 20
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the queue to bind.
    queue_name: str = attr.ib()

    #: The name of the exchange to bind.
    exchange_name: str = attr.ib()

    #: The message routing key.
    routing_key: str = attr.ib()

    #: If True, no Bind-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: A set of implementation-specific (or exchange-specific) arguments.
    arguments: dict[str, Any] = attr.ib()


@attr.s(frozen=True, slots=True)
class QueueBindOkPayload(MethodPayload):
    """
    Payload for the ``bind-ok`` method.
    """

    klass = ClassID.QUEUE
    method = 21
    is_client_side = True

    # empty body


@attr.s(frozen=True, slots=True)
class QueuePurgePayload(MethodPayload):
    """
    Payload for the ``purge`` method.
    """

    klass = ClassID.QUEUE
    method = 30
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the queue to purge.
    name: str = attr.ib()

    #: If True, no Purge-Ok method will be sent by the server.
    no_wait: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class QueuePurgeOkPayload(MethodPayload):
    """
    Payload for the ``purge-ok`` method.
    """

    klass = ClassID.QUEUE
    method = 31
    is_client_side = True

    #: The number of messages purged from the queue.
    message_count: int = attr.ib(metadata=aq_type("long"))


@attr.s(frozen=True, slots=True)
class QueueDeletePayload(MethodPayload):
    """
    Payload for the ``delete`` method.
    """

    klass = ClassID.QUEUE
    method = 40
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the queue to delete.
    queue_name: str = attr.ib()

    #: If True, no Delete-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: If True, then the queue will be deleted only if it is empty.
    if_empty: bool = attr.ib()

    #: If True, then the queue will be deleted only if it is unused.
    if_unused: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class QueueDeleteOkPayload(MethodPayload):
    """
    Payload for the ``delete-ok`` method.
    """

    klass = ClassID.QUEUE
    method = 41
    is_client_side = True

    #: The message count remaining for the queue.
    message_count: int = attr.ib(metadata=aq_type("long"))


@attr.s(frozen=True, slots=True)
class QueueUnbindPayload(MethodPayload):
    """
    Payload for the ``unbind`` method.
    """

    klass = ClassID.QUEUE
    method = 50
    is_client_side = False

    # identical to QueueBind...
    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the queue to bind.
    queue_name: str = attr.ib()

    #: The name of the exchange to bind.
    exchange_name: str = attr.ib()

    #: The message routing key.
    routing_key: str = attr.ib()

    #: If True, no Bind-Ok method will be sent by the server.
    no_wait: bool = attr.ib()

    #: A set of implementation-specific (or exchange-specific) arguments.
    arguments: dict[str, Any] = attr.ib()


@attr.s(frozen=True, slots=True)
class QueueUnbindOkPayload(MethodPayload):
    """
    Payload for the ``unbind-ok`` method.
    """

    klass = ClassID.QUEUE
    method = 51
    is_client_side = True

    # empty body


## BASIC ##
@attr.s(frozen=True, slots=True)
class BasicQOSPayload(MethodPayload):
    """
    Payload for the ``qos`` method.
    """

    klass = ClassID.BASIC
    method = 10
    is_client_side = False

    #: The prefetch size, in octets.
    prefetch_size: int = attr.ib(metadata=aq_type("long"))

    #: The prefetch count, in messages.
    prefetch_count: int = attr.ib(metadata=aq_type("short"))

    #: If this is a global QOS, or a per-channel one.
    global_: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicQOSOkPayload(MethodPayload):
    """
    Payload for the ``qos-ok`` method.
    """

    klass = ClassID.BASIC
    method = 11
    is_client_side = True

    # empty body


@attr.s(frozen=True, slots=True)
class BasicConsumePayload(MethodPayload):
    """
    Payload for the ``consume`` method.
    """

    klass = ClassID.BASIC
    method = 20
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the queue to consume from.
    queue_name: str = attr.ib()

    #: The consumer tag to use. Will be automatically generated by the server if left blank.
    consumer_tag: str = attr.ib()

    # todo doc
    no_wait: bool = attr.ib()
    exclusive: bool = attr.ib()
    no_ack: bool = attr.ib()
    no_local: bool = attr.ib()

    #: Extra, implementation specific arguments.
    arguments: dict[str, Any] = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicConsumeOkPayload(MethodPayload):
    """
    Payload for the ``consume-ok`` method.
    """

    klass = ClassID.BASIC
    method = 21
    is_client_side = True

    #: The consumer tag for the consumer.
    consumer_tag: str = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicCancelPayload(MethodPayload):
    """
    Payload for the ``cancel`` method.
    """

    klass = ClassID.BASIC
    method = 30
    is_client_side = False

    #: The consumer tag to be cancelled.
    consumer_tag: str = attr.ib()

    #: If set, the server should not respond.
    no_wait: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicCancelOkPayload(MethodPayload):
    """
    Payload for the ``cancel-ok`` method.
    """

    klass = ClassID.BASIC
    method = 31
    is_client_side = True

    #: The consumer tag being cancelled.
    consumer_tag: str = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicPublishPayload(MethodPayload):
    """
    Payload for the ``publish`` method.
    """

    klass = ClassID.BASIC
    method = 40
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the exchange to publish the data to.
    name: str = attr.ib()

    #: The routing key to use.
    routing_key: str = attr.ib()

    #: If True, the server will return a Return message if the message cannot be sent to a consumer
    #: immediately.
    immediate: bool = attr.ib()

    #: If True, the server will return a Return message if the message is unrouteable.
    mandatory: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicReturnPayload(MethodPayload):
    """
    Payload for the ``return`` method.
    """

    klass = ClassID.BASIC
    method = 50
    is_client_side = True

    #: The code for the error that caused this message to be returned.
    reply_code: ReplyCode = attr.ib(converter=_fuck_fuck_fuck, metadata=aq_type("short"))

    #: The text for the error that caused this message to be returned.
    reply_text: str = attr.ib()

    #: The name of the exchange this was returned from.
    exchange: str = attr.ib()

    #: The message routing key.
    routing_key: str = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicDeliverPayload(MethodPayload):
    """
    Payload for the ``deliver`` method.
    """

    klass = ClassID.BASIC
    method = 60
    is_client_side = True

    #: The identifier for the consumer.
    consumer_tag: str = attr.ib()

    #: The server-assigned delivery tag.
    delivery_tag: int = attr.ib(metadata=aq_type("longlong"))

    #: Indicates that the message has been previously delivered.
    redelivered: bool = attr.ib()

    #: The name of the exchange the message was originally published to.
    exchange_name: str = attr.ib()

    #: The routing key for the message.
    routing_key: str = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicGetPayload(MethodPayload):
    """
    Payload for the ``get`` method.
    """

    klass = ClassID.BASIC
    method = 70
    is_client_side = False

    reserved_1: int = attr.ib(metadata=aq_type("short"))

    #: The name of the queue to get from.
    queue_name: str = attr.ib()

    #: If True, then messages will not be expected to be acknowledged.
    no_ack: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicGetOkPayload(MethodPayload):
    """
    Payload for the ``get-ok`` method.
    """

    klass = ClassID.BASIC
    method = 71
    is_client_side = True

    #: The server-assigned delivery tag.
    delivery_tag: int = attr.ib(metadata=aq_type("longlong"))

    #: Indicates that the message has been previously delivered.
    redelivered: bool = attr.ib()

    #: The name of the exchange the message was originally published to.
    exchange_name: str = attr.ib()

    #: The routing key for the message.
    routing_key: str = attr.ib()

    #: The message count remaining for the queue.
    message_count: int = attr.ib(metadata=aq_type("long"))


@attr.s(frozen=True, slots=True)
class BasicGetEmptyPayload(MethodPayload):
    """
    Payload for the ``get-empty`` method.
    """

    klass = ClassID.BASIC
    method = 72
    is_client_side = True

    reserved_1: str = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicAckPayload(MethodPayload):
    """
    Payload for the ``ack`` method.
    """

    klass = ClassID.BASIC
    method = 80
    is_client_side = True

    #: The server-assigned delivery tag.
    delivery_tag: int = attr.ib(metadata=aq_type("longlong"))

    #: If True, the delivery tag is set to "up to and including".
    multiple: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicRejectPayload(MethodPayload):
    """
    Payload for the ``reject`` method.
    """

    klass = ClassID.BASIC
    method = 90
    is_client_side = False

    #: The server-assigned delivery tag.
    delivery_tag: int = attr.ib(metadata=aq_type("longlong"))

    #: If True, the message should be re-queued.
    requeue: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicNackPayload(MethodPayload):
    """
    Payload for the ``nack`` method.
    """

    klass = ClassID.BASIC
    method = 120
    is_client_side = True

    #: The server-assigned delivery tag.
    delivery_tag: int = attr.ib(metadata=aq_type("longlong"))

    #: If True, the message should be re-queued.
    requeue: bool = attr.ib()

    #: If True, the delivery tag is set to "up to and including".
    multiple: bool = attr.ib()


## CONFIRM (RabbitMQ extension) ##
@attr.s(frozen=True, slots=True)
class ConfirmSelectPayload(MethodPayload):
    """
    Payload for the ``select`` method.
    """

    klass = ClassID.CONFIRM
    method = 10
    is_client_side = False

    #: If set, the server should not respond.
    no_wait: bool = attr.ib()


@attr.s(frozen=True, slots=True)
class ConfirmSelectOkPayload(MethodPayload):
    """
    Payload for the ``select-ok`` method.
    """

    klass = ClassID.CONFIRM
    method = 11
    is_client_side = True

    # empty body


PAYLOAD_TYPES: dict[ClassID, dict[int, type[MethodPayload]]] = {
    ClassID.CONNECTION: {
        ConnectionStartPayload.method: ConnectionStartPayload,
        ConnectionStartOkPayload.method: ConnectionStartOkPayload,
        ConnectionSecurePayload.method: ConnectionSecurePayload,
        ConnectionSecureOkPayload.method: ConnectionSecureOkPayload,
        ConnectionTunePayload.method: ConnectionTunePayload,
        ConnectionTuneOkPayload.method: ConnectionTuneOkPayload,
        ConnectionOpenPayload.method: ConnectionOpenPayload,
        ConnectionOpenOkPayload.method: ConnectionOpenOkPayload,
        ConnectionClosePayload.method: ConnectionClosePayload,
        ConnectionCloseOkPayload.method: ConnectionCloseOkPayload,
    },
    ClassID.CHANNEL: {
        ChannelOpenPayload.method: ChannelOpenPayload,
        ChannelOpenOkPayload.method: ChannelOpenOkPayload,
        ChannelFlowPayload.method: ChannelFlowPayload,
        ChannelFlowOkPayload.method: ChannelFlowOkPayload,
        ChannelClosePayload.method: ChannelClosePayload,
        ChannelCloseOkPayload.method: ChannelCloseOkPayload,
    },
    ClassID.EXCHANGE: {
        ExchangeDeclarePayload.method: ExchangeDeclarePayload,
        ExchangeDeclareOkPayload.method: ExchangeDeclareOkPayload,
        ExchangeDeletePayload.method: ExchangeDeletePayload,
        ExchangeDeleteOkPayload.method: ExchangeDeleteOkPayload,
        ExchangeBindPayload.method: ExchangeBindPayload,
        ExchangeBindOkPayload.method: ExchangeBindOkPayload,
        ExchangeUnBindPayload.method: ExchangeUnBindPayload,
        ExchangeUnBindOkPayload.method: ExchangeUnBindPayload,
    },
    ClassID.QUEUE: {
        QueueDeclarePayload.method: QueueDeclarePayload,
        QueueDeclareOkPayload.method: QueueDeclareOkPayload,
        QueueBindPayload.method: QueueBindPayload,
        QueueBindOkPayload.method: QueueBindOkPayload,
        QueueDeletePayload.method: QueueDeletePayload,
        QueueDeleteOkPayload.method: QueueDeleteOkPayload,
        QueuePurgePayload.method: QueuePurgePayload,
        QueuePurgeOkPayload.method: QueuePurgeOkPayload,
        QueueUnbindPayload.method: QueueUnbindPayload,
        QueueUnbindOkPayload.method: QueueUnbindOkPayload,
    },
    ClassID.BASIC: {
        BasicPublishPayload.method: BasicPublishPayload,
        BasicDeliverPayload.method: BasicDeliverPayload,
        BasicConsumePayload.method: BasicConsumePayload,
        BasicConsumeOkPayload.method: BasicConsumeOkPayload,
        BasicCancelPayload.method: BasicCancelPayload,
        BasicCancelOkPayload.method: BasicCancelOkPayload,
        BasicQOSPayload.method: BasicQOSPayload,
        BasicQOSOkPayload.method: BasicQOSOkPayload,
        BasicAckPayload.method: BasicAckPayload,
        BasicGetPayload.method: BasicGetPayload,
        BasicGetOkPayload.method: BasicGetOkPayload,
        BasicGetEmptyPayload.method: BasicGetEmptyPayload,
        BasicReturnPayload.method: BasicReturnPayload,
    },
    ClassID.CONFIRM: {
        ConfirmSelectPayload.method: ConfirmSelectPayload,
        ConfirmSelectOkPayload.method: ConfirmSelectOkPayload,
    },
}


def deserialise_payload(body: bytes) -> MethodPayload:
    """
    Deserialises a method payload.

    :param body: The body of the payload to decode.
    :return: A :class:`.MethodPayload` matching the returned payload.
    """

    klass, method, rest = (
        ClassID(int.from_bytes(body[0:2], byteorder="big")),
        int.from_bytes(body[2:4], byteorder="big"),
        body[4:],
    )

    try:
        payload_klass = PAYLOAD_TYPES[klass][method]
    except KeyError:
        raise KeyError(f"Unknown method: {klass.name}/{method}") from None

    attr.resolve_types(payload_klass)  # type: ignore
    fields = attr.fields(payload_klass)
    init_params = {}
    buffer = DecodingBuffer(rest)

    for field in fields:
        init_params[field.name] = decode_attrs_attribute(buffer, field)

    return payload_klass(**init_params)


def serialise_payload(payload: MethodPayload) -> bytes:
    """
    Serialises a payload into a bytearray.
    """

    typ = type(payload)
    attr.resolve_types(typ)  # type: ignore

    header = typ.klass.to_bytes(2, byteorder="big") + typ.method.to_bytes(2, byteorder="big")

    buf = EncodingBuffer()
    fields = attr.fields(typ)
    for field in fields:
        encode_attrs_attribute(buf, field, getattr(payload, field.name))

    buf.force_write_bits()
    return header + buf.get_data()


def method_payload_name(payload: MethodPayload) -> str:
    """
    Gets the name of a method payload.
    """

    return f"{payload.klass.name}/{payload.method}/{type(payload).__name__}"
