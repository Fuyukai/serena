import logging

# our public exports, relatively minimal
from serena.channel import Channel
from serena.connection import AMQPConnection, open_connection
from serena.enums import *
from serena.exc import *
from serena.message import AMQPMessage
from serena.mixin import ChannelDelegate, ChannelLike
from serena.payloads.header import BasicHeader

if not hasattr(logging.Logger, "trace"):
    # TRACE_LEVEL = 5
    logging.addLevelName(5, "TRACE")

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(5):
            self._log(5, message, args, **kws)

    logging.Logger.trace = trace
    del trace

del logging
