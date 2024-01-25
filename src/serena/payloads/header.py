from __future__ import annotations

import struct
from collections.abc import Sequence
from typing import Any

import attr

from serena.frame import Frame, FrameType
from serena.payloads.encoding import (
    aq_type,
    decode_attrs_attribute,
    encode_attrs_attribute,
)
from serena.payloads.method import ClassID
from serena.utils.buffer import DecodingBuffer, EncodingBuffer

# whilst any class can define properties, Basic is the only one that does.
# if, in the future, other classes get properties then I will make this more generic.
# additionally, bit properties are packed differently but as there's no actual bit properties
# i skip the implementation.


@attr.s(frozen=True, slots=True)
class ContentHeaderFrame(Frame):
    """
    Frame for content headers.
    """

    type = FrameType.HEADER

    payload: ContentHeaderPayload = attr.ib()


@attr.s(frozen=True, slots=True)
class ContentHeaderPayload:
    #: The class ID of the content this header is for.
    class_id: ClassID = attr.ib()

    #: The full, complete size of the body payload.
    full_size: int = attr.ib()

    #: The payload flags.
    flags: int = attr.ib()

    #: The actual header data for this payload object.
    payload: BasicHeader = attr.ib()


@attr.s(frozen=True, slots=True)
class BasicHeader:
    """
    Defines the data for the Basic header.
    """

    #: The MIME content-type of this message.
    content_type: str | None = attr.ib(default=None, kw_only=True)

    #: The MIME content-encoding of this message.
    content_encoding: str | None = attr.ib(default=None, kw_only=True)

    #: The custom user headers for this message.
    headers: dict[str, Any] = attr.ib(factory=dict, kw_only=True)

    #: The delivery mode for this message.
    delivery_mode: int | None = attr.ib(default=None, kw_only=True, metadata=aq_type("octet"))

    #: The message priority, 0 through 9.
    priority: int | None = attr.ib(default=None, kw_only=True, metadata=aq_type("octet"))

    #: The application correlation ID.
    correlation_id: str | None = attr.ib(default=None, kw_only=True)

    #: The reply-to field.
    reply_to: str | None = attr.ib(default=None, kw_only=True)

    #: The expiration of this message.
    expiration: str | None = attr.ib(default=None, kw_only=True)

    #: The application-specific message ID of this message.
    message_id: str | None = attr.ib(default=None, kw_only=True)

    #: The message timestamp.
    timestamp: int | None = attr.ib(default=None, kw_only=True, metadata=aq_type("longlong"))

    #: The message type name.
    type_name: str | None = attr.ib(default=None, kw_only=True)

    #: The creating user ID.
    user_id: str | None = attr.ib(default=None, kw_only=True)

    #: The application ID.
    application_id: str | None = attr.ib(default=None, kw_only=True)

    reserved_1: str | None = attr.ib(default=None, kw_only=True)


def deserialise_basic_header(payload: bytes) -> ContentHeaderPayload:
    """
    Deserialises the Basic content header into a payload object.
    """

    klass, body_size, flags, rest = (
        ClassID(int.from_bytes(payload[0:2], byteorder="big")),
        int.from_bytes(payload[4:12], byteorder="big"),
        int.from_bytes(payload[12:14], byteorder="big"),
        payload[14:],
    )

    attr.resolve_types(BasicHeader)  # type: ignore
    fields: Sequence[attr.Attribute[Any | None]] = attr.fields(BasicHeader)  # type: ignore
    buffer = DecodingBuffer(rest)

    params = {}

    for idx, field in enumerate(fields):
        bit_idx = len(fields) - (idx - 1)
        is_provided = (flags & (1 << bit_idx)) != 0
        if is_provided:
            data = decode_attrs_attribute(buffer, field)
            params[field.name] = data

    header = BasicHeader(**params)  # type: ignore
    return ContentHeaderPayload(class_id=klass, full_size=body_size, flags=flags, payload=header)


def serialise_basic_header(klass_id: ClassID, body_size: int, body: BasicHeader) -> bytes:
    """
    Serialises a Basic content header into a byte array.

    :param klass_id: The class ID of the method the header is for.
    :param body_size: The full size of the body.
    :param body: The header body.
    """

    # yikes, attrs type hints kinda suck here?
    attr.resolve_types(BasicHeader)  # type: ignore
    fields: Sequence[attr.Attribute[Any | None]] = attr.fields(BasicHeader)  # type: ignore
    buffer = EncodingBuffer()

    flags = 0

    for idx, field in enumerate(fields):
        field_value = getattr(body, field.name)
        if field_value:  # truthy check for the empty dict
            bit_idx = len(fields) - (idx - 1)
            flags |= 1 << bit_idx
            encode_attrs_attribute(buffer, field, field_value)

    header = struct.pack(">HHQH", klass_id.value, 0, body_size, flags)
    return header + buffer.get_data()
