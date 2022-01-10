from __future__ import annotations

import abc
import enum
from typing import ClassVar

import attr


class FrameType(enum.IntEnum):
    """
    Enumeration of possible frame types.
    """

    METHOD = 1
    HEADER = 2
    BODY = 3
    HEARTBEAT = 4


@attr.s(frozen=True, slots=True)
class Frame(abc.ABC):
    """
    Base class for frame objects.
    """

    #: The type of this frame.
    type: ClassVar[FrameType]

    #: The channel ID of this frame.
    channel_id: int = attr.ib()


@attr.s(frozen=True, slots=True)
class HeartbeatFrame(Frame):
    """
    A single heartbeat frame. This should have an empty body.
    """

    type = FrameType.HEARTBEAT
