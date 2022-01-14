.. _getting-started:

Serena's API
============

Requirements
------------

1. Serena requires at least Python 3.8.

2. Serena is primarily tested against RabbitMQ, but any broker that supports the `errata`_ version
   of AMQP is supported.

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

.. autofunction:: serena.open_connection
    :async-with: conn


The resulting :class:`.AMQPConnection` can then be used to open a channel, like so:

.. code-block:: python

    from serena import open_connection

    async with open_connection(...) as conn:
        async with conn.open_channel() as channel:
            ...

It's recommended to look at :ref:`tutorials` for information on how to use this library
appropriately.

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

Connection API
--------------

.. autoclass:: serena.AMQPConnection
    :members:

Channel API
-----------

.. autoclass:: serena.Channel
    :members:

.. _AMQP Model: https://www.rabbitmq.com/tutorials/amqp-concepts.html
.. _errata: https://www.rabbitmq.com/amqp-0-9-1-errata.html
