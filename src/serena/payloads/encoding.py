from __future__ import annotations

from types import UnionType
from typing import Any, Union, get_args, get_origin

from attr import Attribute

from serena.utils.buffer import DecodingBuffer, EncodingBuffer


def aq_type(name: str) -> dict[str, str]:
    return {"amqp_type": name}


def encode_attrs_attribute(buf: EncodingBuffer, att: Attribute[Any], value: Any) -> None:
    """
    Encodes an attrs attribute on a class.

    :param buf: The buffer to encode into.
    :param att: The attribute to encode.
    :param value: The value of the field.
    :return:
    """

    field_type = get_origin(att.type) or att.type

    if field_type is Union or field_type is UnionType:
        unwrapped_field_type = get_args(att.type)[0]
        field_type = get_origin(unwrapped_field_type) or unwrapped_field_type

    if field_type is str:
        buf.write_short_string(value)

    elif field_type is bytes:
        buf.write_long_string(value)

    elif field_type is bool:
        buf.write_bit(value)

    elif field_type is dict:
        buf.write_table(value)

    elif field_type is list:
        # todo
        raise NotImplementedError("list types")

    else:
        type_ = att.metadata.get("amqp_type")
        if not type_:  # not an amqp field, probably default field?
            return

        getattr(buf, f"write_{type_}")(value)


def decode_attrs_attribute(buf: DecodingBuffer, att: Attribute[Any]) -> Any:
    """
    Decodes an attrs field on a class.

    :param buf: The buffer to read from.
    :param att: The attribute to decode.
    :return: The decoded value, or None if there was nothing to decode.
    """

    field_type = get_origin(att.type) or att.type

    # we only allow Optional unions so this is always safe
    if field_type is Union or field_type is UnionType:
        unwrapped_field_type = get_args(att.type)[0]
        field_type = get_origin(unwrapped_field_type) or unwrapped_field_type

    if field_type is str:
        return buf.read_short_string()

    if field_type is bytes:
        return buf.read_long_string()

    if field_type is bool:
        return buf.read_bit()

    if field_type is dict:
        return buf.read_table()

    if field_type is list:
        return buf.read_array()

    type_ = att.metadata.get("amqp_type")
    if not type_:  # not an amqp field, probably default field?
        return None

    return getattr(buf, f"read_{type_}")()
