from __future__ import annotations

from typing import Type, Union

from serena.enums import ReplyCode
from serena.payloads.method import (
    ChannelClosePayload,
    ConnectionClosePayload,
    MethodPayload,
    method_payload_name,
)


class AMQPError(Exception):
    """
    Base class exception for all AMQP-related exceptions.
    """

    __slots__ = ()


class InvalidProtocolError(AMQPError):
    """
    Thrown when the server speaks a protocol that we don't.
    """

    __slots__ = ()


class AMQPStateError(AMQPError):
    """
    Base exception for all AMQP state errors.
    """

    __slots__ = ()


class MessageReturnedError(AMQPError):
    """
    Thrown when a message is returned unexpectedly.
    """

    def __init__(self, reply_code: ReplyCode, reply_text: str, exchange: str, routing_key: str):
        self.reply_code = reply_code
        self.reply_text = reply_text
        self.exchange = exchange
        self.routing_key = routing_key

    def __str__(self):
        msg = (
            f"Message with routing key '{self.routing_key}' was returned from '{self.exchange}': "
            f"{self.reply_code.name}: {self.reply_text}"
        )

        return msg

    __repr__ = __str__


class InvalidPayloadTypeError(AMQPStateError):
    """
    Thrown when a payload's type is invalid.
    """

    __slots__ = ("expected", "actual")

    def __init__(self, expected: Type[MethodPayload], actual: MethodPayload):
        self.expected = expected
        self.actual = actual

        message = f"Expected {expected.__name__}, got {method_payload_name(actual)}"
        super().__init__(message)


class UnexpectedCloseError(AMQPError):
    """
    Thrown when the connection or a channeel closes unexpectedly.
    """

    __slots__ = ("reply_code", "reply_message", "class_id", "method_id")

    @classmethod
    def of(
        cls, payload: Union[ConnectionClosePayload, ChannelClosePayload]
    ) -> UnexpectedCloseError:
        """
        Creates a new :class:`.UnexpectedCloseError` from a close payload.
        """

        return UnexpectedCloseError(
            reply_code=payload.reply_code,
            reply_message=payload.reply_text,
            class_id=payload.class_id,
            method_id=payload.method_id,
        )

    def __init__(
        self,
        reply_code: ReplyCode,
        reply_message: str,
        class_id: int,
        method_id: int,
    ):
        self.reply_code = reply_code
        self.reply_message = reply_message
        self.class_id = class_id
        self.method_id = method_id

        message = f"{reply_code.name}: {reply_message}"
        if class_id > 0:
            message += f" (cause: {self.class_id}/{self.method_id})"

        super().__init__(message)
