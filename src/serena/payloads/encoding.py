from typing import Any, Union, get_args, get_origin

from attr import Attribute

from serena.utils.buffer import DecodingBuffer, EncodingBuffer


def aq_type(name):
    return {"amqp_type": name}


def encode_attrs_attribute(buf: EncodingBuffer, att: Attribute, value: Any):
    """
    Encodes an attrs attribute on a class.

    :param buf: The buffer to encode into.
    :param att: The attribute to encode.
    :param value: The value of the field.
    :return:
    """

    field_type = get_origin(att.type) or att.type

    if field_type is Union:
        unwrapped_field_type = get_args(att.type)[0]
        field_type = get_origin(unwrapped_field_type) or unwrapped_field_type

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
        type_ = att.metadata.get("amqp_type")
        if not type_:  # not an amqp field, probably default field?
            return

        fn = getattr(buf, f"write_{type_}")

    fn(value)


def decode_attrs_attribute(buf: DecodingBuffer, att: Attribute) -> Any:
    """
    Decodes an attrs field on a class.

    :param buf: The buffer to read from.
    :param att: The attribute to decode.
    :return: The decoded value, or None if there was nothing to decode.
    """

    field_type = get_origin(att.type) or att.type

    # we only allow Optional unions so this is always safe
    if field_type is Union:
        unwrapped_field_type = get_args(att.type)[0]
        field_type = get_origin(unwrapped_field_type) or unwrapped_field_type

    if field_type is str:
        fn = buf.read_short_string

    elif field_type is bytes:
        fn = buf.read_long_string

    elif field_type is bool:
        fn = buf.read_bit

    elif field_type is dict:
        fn = buf.read_table

    elif field_type is list:
        fn = buf.read_array

    else:
        type_ = att.metadata.get("amqp_type")
        if not type_:  # not an amqp field, probably default field?
            return None

        fn = getattr(buf, f"read_{type_}")

    return fn()
