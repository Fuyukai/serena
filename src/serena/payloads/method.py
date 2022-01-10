from __future__ import annotations

import abc
import enum
from typing import Any, ClassVar, Dict, Generic, TypeVar, get_origin

import attr

from serena.enums import ReplyCode
from serena.frame import Frame, FrameType
from serena.utils.buffer import DecodingBuffer, EncodingBuffer


class ClassID(enum.IntEnum):
    """
    Enumeration of method class IDs.
    """

    CONNECTION = 10
    CHANNEL = 20
    EXCHANGE = 40
    QUEUE = 50
    BASIC = 60
    TX = 90


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


def _type(name):
    return {"amqp_type": name}


## == CONNECTION == ##
@attr.s(frozen=True, slots=True)
class StartPayload(MethodPayload):
    """
    Payload for the ``start`` method.
    """

    klass = ClassID.CONNECTION
    method = 10
    is_client_side = False

    #: The major version of the protocol.
    version_major: int = attr.ib(metadata=_type("octet"))

    #: The minor version of the protocol.
    version_minor: int = attr.ib(metadata=_type("octet"))

    #: The server properties.
    properties: Dict[str, Any] = attr.ib()

    #: The available security mechanisms.
    mechanisms: bytes = attr.ib()

    #: The available message locales.
    locales: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class StartOkPayload(MethodPayload):
    """
    Payload for the ``start-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 11
    is_client_side = True

    #: The client properties.
    properties: Dict[str, Any] = attr.ib()

    #: The selected security mechanism.
    mechanism: str = attr.ib()

    #: The security response data.
    response: bytes = attr.ib()

    #: The selected locale.
    locale: str = attr.ib()


@attr.s(frozen=True, slots=True)
class SecurePayload(MethodPayload):
    """
    Payload for the ``secure`` method.
    """

    klass = ClassID.CONNECTION
    method = 20
    is_client_side = False

    #: The security challenge data.
    challenge: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class SecureOkPayload(MethodPayload):
    """
    Payload for the ``secure-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 21
    is_client_side = True

    #: The security response data.
    response: bytes = attr.ib()


@attr.s(frozen=True, slots=True)
class TunePayload(MethodPayload):
    """
    Payload for the ``tune`` method.
    """

    klass = ClassID.CONNECTION
    method = 30
    is_client_side = False

    #: The server's proposed maximum channels.
    max_channels: int = attr.ib(metadata=_type("short"))

    #: The server's proposed maximum frame size.
    max_frame_size: int = attr.ib(metadata=_type("long"))

    #: The server's desired heartbeat delay.
    heartbeat_delay: int = attr.ib(metadata=_type("short"))


@attr.s(frozen=True, slots=True)
class TuneOkPayload(MethodPayload):
    """
    Payload for the ``tune-ok`` method.
    """

    klass = ClassID.CONNECTION
    method = 31
    is_client_side = True

    #: The client's negotiated maximum channels.
    max_channels: int = attr.ib(metadata=_type("short"))

    #: The client's negotiated maximum frame size.
    max_frame_size: int = attr.ib(metadata=_type("long"))

    #: The client's desired heartbeat delay.
    heartbeat_delay: int = attr.ib(metadata=_type("short"))


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
class ClosePayload(MethodPayload):
    """
    Payload for the ``close`` method.
    """

    klass = ClassID.CONNECTION
    method = 50
    is_client_side = True

    #: The code for the error that caused this close.
    reply_code: ReplyCode = attr.ib(converter=ReplyCode, metadata=_type("short"))

    #: The text for the error that caused this close.
    reply_text: str = attr.ib()

    #: The class of the method that caused this close.
    class_id: int = attr.ib(metadata=_type("short"))

    #: The class of the method that caused this close.
    method_id: int = attr.ib(metadata=_type("short"))


@attr.s(frozen=True, slots=True)
class CloseOkPayload(MethodPayload):
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
class FlowPayload(MethodPayload):
    """
    Payload for the ``flow`` method.
    """

    klass = ClassID.CHANNEL
    method = 20
    is_client_side = True

    #: If the channel should start processing messages again or not.
    active: bool = attr.ib()


class FlowOkPayload(MethodPayload):
    """
    Payload for the ``flow-ok`` method.
    """

    klass = ClassID.CHANNEL
    method = 21
    is_client_side = True

    #: See :attr:`.FlowPayload.active`.
    active: bool = attr.ib()


PAYLOAD_TYPES = {
    ClassID.CONNECTION: {
        StartPayload.method: StartPayload,
        StartOkPayload.method: StartOkPayload,
        SecurePayload.method: SecurePayload,
        SecureOkPayload.method: SecureOkPayload,
        TunePayload.method: TunePayload,
        TuneOkPayload.method: TuneOkPayload,
        ConnectionOpenPayload.method: ConnectionOpenPayload,
        ConnectionOpenOkPayload.method: ConnectionOpenOkPayload,
        ClosePayload.method: ClosePayload,
        CloseOkPayload.method: CloseOkPayload,
    },
    ClassID.CHANNEL: {
        ChannelOpenPayload.method: ChannelOpenPayload,
        ChannelOpenOkPayload.method: ChannelOpenOkPayload,
        FlowPayload.method: FlowPayload,
        FlowOkPayload.method: FlowOkPayload,
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

    payload_klass = PAYLOAD_TYPES[klass][method]
    attr.resolve_types(payload_klass)
    fields = attr.fields(payload_klass)
    init_params = {}
    buffer = DecodingBuffer(rest)

    for field in fields:
        # check type and decode match
        field_type = get_origin(field.type) or field.type

        if field_type is str:
            fn = buffer.read_short_string

        elif field_type is bytes:
            fn = buffer.read_long_string

        elif field_type is bool:
            fn = buffer.read_bit

        elif field_type is dict:
            fn = buffer.read_table

        elif field_type is list:
            fn = buffer.read_array

        else:
            type_ = field.metadata.get("amqp_type")
            if not type_:  # not an amqp field, probably default field?
                continue

            fn = getattr(buffer, f"read_{type_}")

        init_params[field.name] = fn()

    return payload_klass(**init_params)  # type: ignore


def serialise_payload(payload: MethodPayload) -> bytes:
    """
    Serialises a payload into a bytearray.
    """

    typ = type(payload)
    attr.resolve_types(typ)

    header = typ.klass.to_bytes(2, byteorder="big") + typ.method.to_bytes(2, byteorder="big")

    buf = EncodingBuffer()
    fields = attr.fields(typ)
    for field in fields:
        field_type = get_origin(field.type) or field.type

        if field_type is str:
            fn = buf.write_short_string

        elif field_type is bytes:
            fn = buf.write_long_string

        elif field_type is bool:
            fn = buf.write_bit

        elif field_type is dict:
            fn = buf.write_table

        elif field_type is list:
            # todo
            raise NotImplementedError("list types")

        else:
            type_ = field.metadata.get("amqp_type")
            if not type_:  # not an amqp field, probably default field?
                continue

            fn = getattr(buf, f"write_{type_}")

        value = getattr(payload, field.name)
        fn(value)

    buf.force_write_bits()
    return header + buf.get_data()


def method_payload_name(payload: MethodPayload):
    """
    Gets the name of a method payload.
    """

    return f"{payload.klass.name}/{payload.method}/{type(payload).__name__}"
