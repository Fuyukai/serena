from __future__ import annotations

from collections.abc import Callable
from typing import Self

from typing_extensions import override

from serena.enums import ReplyCode
from serena.payloads.method import (
    ChannelClosePayload,
    ConnectionClosePayload,
    MethodPayload,
    method_payload_name,
)

__all__ = (
    "AMQPError",
    "InvalidProtocolError",
    "AMQPStateError",
    "MessageReturnedError",
    "InvalidPayloadTypeError",
    "UnexpectedCloseError",
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
        #: The server-provided error code.
        self.reply_code = reply_code
        #: The server-provided error text.
        self.reply_text = reply_text

        #: The exchange the message was returned from.
        self.exchange = exchange

        #: The routing key the message was using.
        self.routing_key = routing_key

    @override
    def __str__(self) -> str:
        return (
            f"Message with routing key '{self.routing_key}' was returned from '{self.exchange}': "
            f"{self.reply_code.name}: {self.reply_text}"
        )

    __repr__: Callable[[Self], str] = __str__


class InvalidPayloadTypeError(AMQPStateError):
    """
    Thrown when a payload's type is invalid.
    """

    __slots__ = ("expected", "actual")

    def __init__(self, expected: type[MethodPayload], actual: MethodPayload):
        self.expected: type[MethodPayload] = expected
        self.actual: MethodPayload = actual

        message = f"Expected {expected.__name__}, got {method_payload_name(actual)}"
        super().__init__(message)


class UnexpectedCloseError(AMQPError):
    """
    Thrown when the connection or a channeel closes unexpectedly.
    """

    __slots__ = ("reply_code", "reply_message", "class_id", "method_id")

    @classmethod
    def of(cls, payload: ConnectionClosePayload | ChannelClosePayload) -> UnexpectedCloseError:
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
        #: The server-provided error code.
        self.reply_code: ReplyCode = reply_code
        #: The server-provided error text.
        self.reply_message: str = reply_message

        #: The class ID of the method that caused this error.
        self.class_id: int = class_id
        #: The method ID of the method that caused this error.
        self.method_id: int = method_id

        message = f"{reply_code.name}: {reply_message}"
        if class_id > 0:
            message += f" (cause: {self.class_id}/{self.method_id})"

        super().__init__(message)
