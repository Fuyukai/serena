from __future__ import annotations

import logging
import struct
from typing import Union

from serena.frame import Frame, HeartbeatFrame
from serena.payloads.method import (
    MethodFrame,
    MethodPayload,
    deserialise_payload,
    method_payload_name,
    serialise_payload,
)


class _NEED_DATA:
    pass


#: A special singleton object used if the parser needs more data.
NEED_DATA = _NEED_DATA()

logger = logging.getLogger(__name__)

# Dear RabbitMQ authors:
# Fuck you. I fucking HATE you. Implement the FUCKING SPEC PROPERLY.
# I love spending a day of my life implementing the AMQP 0-9-1 spec only to find that RabbitMQ
# fucking does something else. Short strings in tables? Thrown away. Heartbeat frames? Type 8
# instead of type 4. What the fuck??? Why?????????

METHOD_FRAME = 1
HEADER_FRAME = 2
BODY_FRAME = 3
HEARTBEAT_FRAME = 8


class FrameParser(object):
    """
    A buffer that parses AMQP frames. This is not an AMQP state machine, and is only suitable for
    the low-level wire processing.
    """

    def __init__(self):
        self._buffer: bytearray = bytearray()
        self._processing_partial_packet = False

        # saved state
        self._saved_type = -1
        self._saved_channel = -1
        self._saved_size = -1
        self._bytes_remaining = -1
        self._last_packet_buffer = bytearray()

    def _make_frame(self, type: int, channel: int, payload: bytes) -> Frame:
        """
        Creates a new frame object.
        """

        if type == METHOD_FRAME:
            payload = deserialise_payload(payload)
            frame = MethodFrame(channel_id=channel, payload=payload)
            logger.trace(f"FRAME (METHOD): {method_payload_name(payload)}")
            return frame

        elif type == HEADER_FRAME:
            raise NotImplementedError("header frames")

        elif type == BODY_FRAME:
            raise NotImplementedError("body frames")

        elif type == HEARTBEAT_FRAME:
            assert channel == 0, "heartbeats cannot be on any channel other than zero"
            return HeartbeatFrame(channel_id=0)

        else:
            raise ValueError(f"Invalid frame type: {type}")

    @staticmethod
    def write_method_frame(channel: int, payload: MethodPayload) -> bytes:
        """
        Writes a method payload into a bytearray.

        :param channel: The channel ID this payload is being sent on.
        :param payload: The payload object to send.
        :return: The :class:`bytes` to send over the wire.
        """

        frame_body = serialise_payload(payload)
        size = len(frame_body)

        header = struct.pack(">BHI", METHOD_FRAME, channel, size)
        result = header + frame_body + b"\xCE"
        return result

    @staticmethod
    def write_heartbeat_frame() -> bytes:
        """
        Writes a single heartbeat frame into a bytearray.
        """

    def receive_data(self, data: bytes):
        """
        Receives incoming data from the AMQP server.
        """

        self._buffer += data

    def next_frame(self) -> Union[Frame, _NEED_DATA]:
        """
        Retrieves the next frame from the buffer.
        """

        if len(self._buffer) <= 0:
            return NEED_DATA

        if self._processing_partial_packet:
            body, self._buffer = (
                self._buffer[: self._bytes_remaining],
                self._buffer[self._bytes_remaining :],
            )
            self._last_packet_buffer += body
            self._bytes_remaining -= len(body)

            if self._bytes_remaining > 0:
                return NEED_DATA

            # truncate frame-end octet
            assert body[-1] == 0xCE, "invalid frame-end octet"
            body = body[:-1]

            # packet finished, construct frame from saved values
            self._processing_partial_packet = False
            frame = self._make_frame(
                self._saved_type, self._saved_channel, self._last_packet_buffer
            )
            self._last_packet_buffer = bytearray

            return frame
        else:
            # header is 7 octets, without it we don't care.
            if len(self._buffer) < 7:
                logger.trace("Missing packet header, skipping")
                return NEED_DATA

            # pop off bits
            type_ = self._buffer[0]
            channel = (self._buffer[1] << 8) | self._buffer[2]
            size = (
                self._buffer[3] << 24
                | self._buffer[4] << 16
                | self._buffer[5] << 8
                | self._buffer[6]
            ) + 1  # + 1 is for the frame end byte (0xCE)

            logger.trace(f"Received packet ({type_=} | {channel=} | {size=})")

            body, self._buffer = self._buffer[7 : size + 7], self._buffer[size + 7 :]
            if len(body) < size:
                # missing part of the packet, save and return later
                self._processing_partial_packet = True
                self._saved_size = size
                self._saved_channel = channel
                self._saved_type = type_
                self._last_packet_buffer = body
                self._bytes_remaining = size - len(body)
                logger.trace(f"Missing {self._bytes_remaining} bytes from packet, skipping cycle")
                return NEED_DATA
            else:
                return self._make_frame(type_, channel, body)
