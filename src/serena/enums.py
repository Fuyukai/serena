from enum import IntEnum

try:
    from enum import StrEnum
except ImportError:
    from enum import StrEnum


__all__ = (
    "ReplyCode",
    "ExchangeType",
    "ClassID",
)


class ClassID(IntEnum):
    """
    Enumeration of method class IDs.
    """

    CONNECTION = 10
    CHANNEL = 20
    EXCHANGE = 40
    QUEUE = 50
    BASIC = 60
    CONFIRM = 85
    TX = 90


class ReplyCode(IntEnum):
    """
    Enumeration of possible AMQP reply codes.
    """

    #: Indicates that the method completed successfully. This reply code is reserved for future use
    #: - the current protocol design does not use positive confirmation and reply codes are sent
    #: only in case of an error.
    success = 200

    #: The client attempted to transfer content larger than the server could accept at the present
    #: time. The client may retry at a later time.
    content_too_large = 311

    #: Message cannot be delivered to any queue.
    no_route = 312

    #: When the exchange cannot deliver to a consumer when the immediate flag is set. As a result
    #: of pending data on the queue or the absence of any consumers of the queue.
    no_consumers = 313

    #: An operator intervened to close the connection for some reason. The client may retry at
    #: some later date.
    connection_forced = 320

    #: The client tried to work with an unknown virtual host.
    invalid_path = 402

    #: The client attempted to work with a server entity to which it has no access due to security
    #: settings.
    access_refused = 403

    #: The client attempted to work with a server entity that does not exist.
    not_found = 404

    #: The client attempted to work with a server entity to which it has no access because
    #: another client is working with it.
    resource_locked = 405

    #: The client requested a method that was not allowed because some precondition failed.
    precondition_failed = 406

    #: The sender sent a malformed frame that the recipient could not decode. This strongly implies
    #: a programming error in the sending peer.
    frame_error = 501

    #: The sender sent a frame that contained illegal values for one or more fields. This strongly
    #: implies a programming error in the sending peer.
    syntax_error = 502

    #: The client sent an invalid sequence of frames, attempting to perform an operation that was
    #: considered invalid by the server. This usually implies a programming error in the client.
    command_invalid = 503

    #: The client attempted to work with a channel that had not been correctly opened. This most
    #: likely indicates a fault in the client layer.
    channel_error = 504

    #: The peer sent a frame that was not expected, usually in the context of a content header
    #: and body. This strongly indicates a fault in the peer's content processing.
    unexpected_frame = 505

    #: The server could not complete the method because it lacked sufficient resources. This may
    #: be due to the client creating too many of some type of entity.
    resource_error = 506

    #: The client tried to work with some entity in a manner that is prohibited by the server, due
    #: to security settings or by some other criteria.
    not_allowed = 530

    #: The client tried to use functionality that is not implemented in the server.
    not_implemented = 540

    #: The server could not complete the method because of an internal error. The server may
    #: require intervention by an operator in order to resume normal operations.
    internal_error = 541


class ExchangeType(StrEnum):
    """
    Enumeration of possible exchange types.
    """

    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"
