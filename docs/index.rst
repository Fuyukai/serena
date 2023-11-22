.. Serena documentation master file, created by
   sphinx-quickstart on Fri Jan 14 22:41:11 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Serena's documentation!
==================================

Serena is a structurally concurrent AMQP 0-9-1 client library, for usage with the `AnyIO`_ library.

.. contents::

.. toctree::
    :maxdepth: 3

    api.rst
    history.rst

Requirements
------------

1. Serena requires at least Python 3.11.

2. Serena is primarily tested against RabbitMQ, but any broker that supports the `errata`_ version
   of AMQP is supported.

3. The AMQP server must support *publisher confirms*. (RabbitMQ does support this.)


Installation
------------

Serena can be installed from PyPI:

.. code-block:: fish

    $ poetry add serena

It can then be imported from the ``serena`` package.

Basic Usage
-----------

Serena is a relatively thin wrapper around the AMQP 0-9-1 specification, and as such you should
familiarise yourself with the `AMQP Model`_ before using this library.

First, you need to open a connection. You can do this with the :meth:`.open_connection` context
manager provided, like so:

.. code-block:: python

    from serena import open_connection

    async with open_connection(
        host="127.0.0.1", port=5672,
        username="guest", password="guest",
        virtual_host="/",
    ) as conn:
        ...


The resulting :class:`.AMQPConnection` can then be used to open a channel using
:meth:`.AMQPConnection.open_channel`, like so:

.. code-block:: python

    from serena import open_connection

    async with open_connection(...) as conn:
        async with conn.open_channel() as channel:
            ...

Error Handling
--------------

There are situations where you or someone else (usually, somebody else) makes a programming error
that the server doesn't like.

In these situations, the server will return a Close message that is handled by client machinery,
and either the *connection* or the *channel* will close. Which one happens is specified in the AMQP
specification.

Either way, a :class:`.UnexpectedCloseError` will be raised:

- If the channel is closed, then it will be raised by the method itself
- If the connection is closed, then the entire connection will die and it will be raised by the
  ``async with`` block. In addition, all code inside the connection will be cancelled.

.. autoclass:: serena.UnexpectedCloseError
    :members:

Some other errors may be raised for programming errors, such as:

.. autoclass:: serena.MessageReturnedError
    :members:


Buffer Sizing
-------------

As AMQP is a multiplexed protocol, Serena internally uses several memory channels to distribute
messages from the socket to individual channel objects. Memory channels apply backpressure, which
means that new messages cannot be delivered when the channel is full.

To avoid this, Serena will automatically apply per-channel QoS to avoid buffering more items than
the channel can support. By default, this will be the floor division by three of the
``channel_buffer_size`` (default: 48) passed when opening the connection, as every publish uses
three frames minimum (Basic.Publish|Get, headers, body). This is primarily a network optimisation
as it means the server can send N messages now potentially over less TCP packets instead of needing
to a full cycle each time.

However, messages with a large body size may be split across multiple body frames which has the
potential to overflow the buffer. When this happens, ***the entire connection will block***, as
the socket cannot be read from until there is room to buffer new packets in memory.

To avoid this:

1. Increase the frame size by passing ``desired_frame_size=BIG`` to :meth:`.open_connection`.

2. Increase the frame size in the broker configuration.

3. Send smaller messages.

.. warning::

    Serena negotiates the frame size using the smallest size requested, either on the client or on
    the server side. You need to change it on both.


Channel Pooling
---------------

.. versionadded:: 0.7.1

Serena has a built-in channel pooling mechanism, which simplifies error handling (no need to wrap
your entire ``async with`` in a try/except). See :meth:`.AMQPConnection.open_channel`.

Naming
------

1. The primary protagonist of Bishōjo Senshi Sailor Moon is called Usagi Tsukino.
2. Usagi means "rabbit".
3. Her name was changed to Serena Tsukino in the 1990s DIC English dub of Sailor Moon.
4. RabbitMQ is the most popular and well-known AMQP 0-9-1 broker.

Hence, the name Serena.

.. _AnyIO: https://anyio.readthedocs.io/
.. _AMQP Model: https://www.rabbitmq.com/tutorials/amqp-concepts.html
.. _errata: https://www.rabbitmq.com/amqp-0-9-1-errata.html
