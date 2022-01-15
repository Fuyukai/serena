.. _01-hello-world:

Hello World
===========

Introduction
------------

AMQP uses two types of peers: a *broker* and a *client*. A client is you. A broker is something
that accepts and forwards messages. You can think about it as a post office: when you put the post
that you want posting in a post box, you can be sure that the letter carrier will eventually
deliver the post to your recipient.

The primary difference between a broker and a post office is that a broker uses binary blobs of
data - messages - instead of paper.

AMQP uses certain jargon:

- Producing means nothing more than sending. A program that sends messages is a *producer*.
- A queue is the name for a post box that lives inside the AMQP broker. It's only bounded by the
  host's resources.
- A consumer is a program that waits to receive messages.

The Code
--------

Here, we will write one program that acts as both a producer and a sender, with the functionality
split across two functions that run in two separate async tasks.

The Producer
~~~~~~~~~~~~

First, the producer:

.. code-block:: python

    from serena import AMQPConnection

    async def producer(conn: AMQPConnection):
        async with conn.open_channel() as channel:
            ...

This opens a new channel, which will be used to issue AMQP commands on. Every time you want to do
a command, you need to open a new channel.

.. note::

    Channels are designed to be relatively ephemeral. You can open channels relatively freely,
    ideally with one per task.

Next, we need to make sure the recipient queue exists. If we don't do this, then the message will
be sent to nowhere in particular. Inside the same ``async with``:

.. code-block:: python

    await channel.queue_declare(name="hello")

At this point, we're ready to send a message. The message will just contain the string "Hello,
world!" and will be sent to the ``hello`` queue we just declared.

In AMQP, a message can never be directly sent to a queue; it has to go through an exchange.
Exchanges will be covered in `the third tutorial <03-pubsub>`_, but for now we will publish
to the *default* exchange, which is identified by the empty string. This exchange is special, as it
lets us specify what queue we want the message to be sent to with the ``routing_code`` field.

.. code-block:: python

    await channel.basic_publish(exchange_name="", routing_key="hello", body=b"Hello, world!")
    print("Sent 'Hello, world!'")

.. note::

    AMQP messages are binary data, so you must send :class:`bytes` or :class:`bytearray` instances
    instead of raw strings, and encode/decode them in your application as appropriate.

Receiving
~~~~~~~~~

Our second function will receive the message and print it on the screen.

.. code-block:: python

    async def consumer(conn: AMQPConnection):
        async with conn.open_channel() as channel:
            await channel.queue_declare(name="hello")

.. note::

    The queue is declared a second time in the receiver. This is good practice, as it is not
    guaranteed what order the tasks would be ran in. If the receiver task started before the
    sender task, then an error would occur if the receiver tried to read off a queue that doesn't
    exist.

    All declaration actions are *idempotent*, so this is a safe call even if the queue already
    exists.

To actually consume items from the queue, we open an asynchronous generator and start iterating
messages from it.

.. code-block:: python

    async with channel.basic_consume(queue_name="hello") as agen:
        async for message in agen:
            print("Received message:", message.body.decode("utf-8"))

.. note::

    This is an ``async with`` block as it will automatically cancel the consume action when
    the loop is exited with a ``break`` or a ``return``. Additionally, it is required to
    automatically close the underlying asynchronous generator without needing an
    :func:`contextlib.aclosing` on the end-user side.

Putting it all together
~~~~~~~~~~~~~~~~~~~~~~~

Now we just have to write a function that spawns both of these tasks.

.. code-block:: python

    from serena import open_connection

    async def main():
        async with open_connection("127.0.0.1") as connection, trio.open_nursery() as nursery:
            nursery.start_soon(producer, connection)
            nursery.start_soon(consumer, connection)

    if __name__ == "__main__":
        trio.run(main)

The final file will end up looking like this:

.. code-block:: python

    import trio

    from serena import AMQPConnection, open_connection

    async def producer(conn: AMQPConnection):
        async with conn.open_channel() as channel:
            await channel.queue_declare(name="hello")
            await channel.basic_publish(exchange_name="", routing_key="hello", body=b"Hello, world!")
            print("Sent 'Hello, world!'")


    async def consumer(conn: AMQPConnection):
        async with conn.open_channel() as channel:
            await channel.queue_declare(name="hello")

            async with channel.basic_consume(queue_name="hello") as agen:
                async for message in agen:
                    print("Received message:", message.body.decode("utf-8"))


    async def main():
        async with open_connection("127.0.0.1") as connection, trio.open_nursery() as nursery:
            nursery.start_soon(producer, connection)
            nursery.start_soon(consumer, connection)


    if __name__ == "__main__":
        trio.run(main)

Now, running the final file, you should end up with output like this:

.. code-block:: fish

    └> python 01_hello_world.py
    Sent 'Hello, world!'
    Received message: Hello, world!

.. note::

    Depending on the terminal, the messages may be in the opposite order.

The process will now hang; just Ctrl-C it to finish consuming.
