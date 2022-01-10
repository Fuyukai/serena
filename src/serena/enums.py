import enum


class ReplyCode(enum.IntEnum):
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

    #: When the exchange cannot deliver to a consumer when the immediate flag is set. As a result
    #: of pending data on the queue or the absence of any consumers of the queue.
    no_consumers = 313

    #: An operator intervened to close the connection for some reason. The client may retry at
    #: some later date.
    connection_forced = 320

    #: The client tried to work with an unknown virtual host.
    invalid_path = 402
