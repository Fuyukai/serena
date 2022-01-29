.. _getting-started:

Automatic API Docs
==================

These are API docs for the public-facing parts of Serena.

Connection API
--------------

.. autofunction:: serena.open_connection
    :async-with:

.. autoclass:: serena.AMQPConnection
    :members:

Channel-like API
----------------

This is shared between ``Channel`` and ``ChannelPool``.

.. autoclass:: serena.mixin.ChannelLike
    :members:

Specific APIs
-------------

.. automethod:: serena.Channel.wait_until_closed

Message API
-----------

.. autoclass:: serena.AMQPMessage
    :members:

