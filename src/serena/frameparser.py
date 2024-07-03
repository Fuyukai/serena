from __future__ import annotations

import struct
from math import ceil

from serena.frame import BodyFrame, Frame, HeartbeatFrame
from serena.payloads.header import (
    BasicHeader,
    ContentHeaderFrame,
    deserialise_basic_header,
    serialise_basic_header,
)
from serena.payloads.method import (
    ClassID,
    MethodFrame,
    MethodPayload,
    deserialise_payload,
    method_payload_name,
    serialise_payload,
)
from serena.utils import LoggerWithTrace


class _NEED_DATA:
    pass


#: A special singleton object used if the parser needs more data.
NEED_DATA = _NEED_DATA()

logger: LoggerWithTrace = LoggerWithTrace.get(__name__)

# Dear RabbitMQ authors:
# Fuck you. I fucking HATE you. Implement the FUCKING SPEC PROPERLY.
# I love spending a day of my life implementing the AMQP 0-9-1 spec only to find that RabbitMQ
# fucking does something else. Short strings in tables? Thrown away. Heartbeat frames? Type 8
# instead of type 4. What the fuck??? Why?????????

METHOD_FRAME = 1
HEADER_FRAME = 2
BODY_FRAME = 3
HEARTBEAT_FRAME = 8


class FrameParser:
    """
    A buffer that parses AMQP frames. This is not an AMQP state machine, and is only suitable for
    the low-level wire processing.
    """

    def __init__(self) -> None:
        self._buffer: bytearray = bytearray()
        self._processing_partial_packet = False

        # saved state
        self._saved_type = -1
        self._saved_channel = -1
        self._saved_size = -1
        self._bytes_remaining = -1
        self._last_packet_buffer = bytearray()

    @staticmethod
    def _make_frame(type: int, channel: int, payload: bytes) -> Frame:
        """
        Creates a new frame object.
        """

        if type == METHOD_FRAME:
            method_payload = deserialise_payload(payload)
            frame = MethodFrame(channel_id=channel, payload=method_payload)
            logger.trace(
                f"S#{channel}->C FRAME (METHOD): {method_payload_name(method_payload)} "
                f"({len(payload)}B)"
            )
            return frame

        if type == HEADER_FRAME:
            content_header = deserialise_basic_header(payload)
            logger.trace(
                f"S#{channel}->C FRAME (HEADER): {content_header.class_id.name} ("
                f"{len(payload)} bytes)"
            )
            return ContentHeaderFrame(channel_id=channel, payload=content_header)

        if type == BODY_FRAME:
            logger.trace(f"S#{channel}->C FRAME (BODY): {len(payload)} bytes")
            return BodyFrame(channel_id=channel, data=payload)

        if type == HEARTBEAT_FRAME:
            assert channel == 0, "heartbeats cannot be on any channel other than zero"
            logger.trace("S->C FRAME (HEARTBEAT)")
            return HeartbeatFrame(channel_id=0)

        raise ValueError(f"Invalid frame type: {type}")

    @staticmethod
    def _pack_frame(type_: int, channel: int, payload: bytes) -> bytes:
        """
        Packs a single frame with a header.

        :param type_: The type of the frame.
        :param channel: The channel the frame is on.
        :param payload: The opaque payload body.
        :return: A fully encoded frame.
        """

        size = len(payload)
        header = struct.pack(">BHI", type_, channel, size)
        return header + payload + b"\xce"

    @staticmethod
    def write_method_frame(channel: int, payload: MethodPayload) -> bytes:
        """
        Writes a method payload into a bytestring.

        :param channel: The channel ID this payload is being sent on.
        :param payload: The payload object to send.
        :return: The :class:`bytes` to send over the wire.
        """

        body = serialise_payload(payload)
        result = FrameParser._pack_frame(METHOD_FRAME, channel, body)
        logger.trace(
            f"C#{channel}->S FRAME (METHOD): {method_payload_name(payload)} ({len(body)} bytes)"
        )
        return result

    @staticmethod
    def write_header_frame(
        channel: int, method_klass: ClassID, body_length: int, headers: BasicHeader
    ) -> bytes:
        """
        Writes a header payload into a bytearray.
        """

        frame_body = serialise_basic_header(method_klass, body_length, headers)
        result = FrameParser._pack_frame(HEADER_FRAME, channel, frame_body)
        logger.trace(
            f"C#{channel}->S FRAME (HEADER): {method_klass.name} ({len(frame_body)} bytes)"
        )
        return result

    @staticmethod
    def write_body_frames(channel: int, body: bytes, max_frame_size: int) -> list[bytes]:
        """
        Writes a set of body frames into a set of byte strings.
        """

        if len(body) <= 0:
            return []

        frames_needed = ceil(len(body) / max_frame_size)
        frames: list[bytes] = []

        for i in range(0, frames_needed):
            frame_body = body[max_frame_size * i : max_frame_size * (i + 1)]
            frame = FrameParser._pack_frame(BODY_FRAME, channel, frame_body)
            logger.trace(
                f"C#{channel}->S FRAME (BODY): {i + 1} / {frames_needed} ({len(frame_body)} bytes)"
            )
            frames.append(frame)

        return frames

    def receive_data(self, data: bytes) -> None:
        """
        Receives incoming data from the AMQP server.
        """

        self._buffer += data

    def next_frame(self) -> Frame | _NEED_DATA:
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
                logger.trace(
                    f"Still missing {self._bytes_remaining} bytes from packet, skipping cycle"
                )
                return NEED_DATA

            # truncate frame-end octet
            assert self._last_packet_buffer[-1] == 0xCE, "invalid frame-end octet"
            body = self._last_packet_buffer[:-1]

            # packet finished, construct frame from saved values
            self._processing_partial_packet = False
            frame = self._make_frame(self._saved_type, self._saved_channel, bytes(body))
            self._last_packet_buffer = bytearray()

            return frame
        else:  # noqa
            # header is 7 octets, without it we don't care.
            if len(self._buffer) < 7:  # pragma: no cover
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
            else:  # noqa
                assert body[-1] == 0xCE, "invalid frame-end octet"
                body = body[:-1]

                return self._make_frame(type_, channel, bytes(body))
