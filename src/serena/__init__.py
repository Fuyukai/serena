import logging

if not hasattr(logging.Logger, "trace"):
    # TRACE_LEVEL = 5
    logging.addLevelName(5, "TRACE")

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(5):
            self._log(5, message, args, **kws)

    logging.Logger.trace = trace
    del trace

del logging
