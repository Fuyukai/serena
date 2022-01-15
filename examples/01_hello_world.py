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
