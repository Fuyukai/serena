from typing import Type

from serena.enums import ReplyCode
from serena.payloads.method import MethodPayload, method_payload_name


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
    Thrown when the connection closes unexpectedly.
    """

    __slots__ = ("reply_code", "reply_message", "class_id", "method_id")

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