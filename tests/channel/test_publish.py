import typing

import anyio
import pytest
from serena.channel import Channel
from serena.connection import AMQPConnection
from serena.enums import ReplyCode
from serena.exc import MessageReturnedError
from serena.frame import BodyFrame, Frame, HeartbeatFrame
from serena.message import AMQPMessage
from serena.payloads.header import BasicHeader, ContentHeaderFrame, ContentHeaderPayload
from serena.payloads.method import (
    BasicDeliverPayload,
    BasicGetEmptyPayload,
    BasicGetOkPayload,
    BasicReturnPayload,
    MethodFrame,
)

from tests import _open_connection

pytestmark = pytest.mark.anyio


def _return_frames(
    routing_key: str,
    body: bytes = b"x",
    *,
    channel_id: int = 1,
    header: BasicHeader | None = None,
) -> tuple[MethodFrame[BasicReturnPayload], ContentHeaderFrame, BodyFrame]:
    return_frame: MethodFrame[BasicReturnPayload] = MethodFrame(
        channel_id=channel_id,
        payload=BasicReturnPayload(
            reply_code=ReplyCode.no_route,
            reply_text="NO_ROUTE",
            exchange="",
            routing_key=routing_key,
        ),
    )
    header_frame = ContentHeaderFrame(
        channel_id=channel_id,
        payload=ContentHeaderPayload(
            class_id=return_frame.payload.klass,
            full_size=len(body),
            flags=0,
            payload=header or BasicHeader(),
        ),
    )
    body_frame = BodyFrame(channel_id=channel_id, data=body)
    return return_frame, header_frame, body_frame


def _delivery_frames(
    routing_key: str,
    body: bytes = b"x",
    *,
    channel_id: int = 2,
    header: BasicHeader | None = None,
) -> tuple[MethodFrame[BasicDeliverPayload], ContentHeaderFrame, BodyFrame]:
    deliver_frame: MethodFrame[BasicDeliverPayload] = MethodFrame(
        channel_id=channel_id,
        payload=BasicDeliverPayload(
            consumer_tag="consumer",
            delivery_tag=1,
            redelivered=False,
            exchange_name="",
            routing_key=routing_key,
        ),
    )
    header_frame = ContentHeaderFrame(
        channel_id=channel_id,
        payload=ContentHeaderPayload(
            class_id=deliver_frame.payload.klass,
            full_size=len(body),
            flags=0,
            payload=header or BasicHeader(),
        ),
    )
    body_frame = BodyFrame(channel_id=channel_id, data=body)
    return deliver_frame, header_frame, body_frame


def _get_ok_frames(
    routing_key: str,
    body: bytes = b"x",
    *,
    channel_id: int = 1,
    header: BasicHeader | None = None,
) -> tuple[MethodFrame[BasicGetOkPayload], ContentHeaderFrame, BodyFrame]:
    get_ok_frame: MethodFrame[BasicGetOkPayload] = MethodFrame(
        channel_id=channel_id,
        payload=BasicGetOkPayload(
            delivery_tag=1,
            redelivered=False,
            exchange_name="",
            routing_key=routing_key,
            message_count=0,
        ),
    )
    header_frame = ContentHeaderFrame(
        channel_id=channel_id,
        payload=ContentHeaderPayload(
            class_id=get_ok_frame.payload.klass,
            full_size=len(body),
            flags=0,
            payload=header or BasicHeader(),
        ),
    )
    body_frame = BodyFrame(channel_id=channel_id, data=body)
    return get_ok_frame, header_frame, body_frame


async def _read_delivery_message(frames: list[Frame]) -> AMQPMessage | None:
    channel = Channel(1, typing.cast(typing.Any, object()), stream_buffer_size=8)
    received = None
    message_received = anyio.Event()

    async with anyio.create_task_group() as task_group:
        async def receive_message() -> None:
            nonlocal received
            received = await channel._receive_delivery_message()
            message_received.set()

        task_group.start_soon(receive_message)
        for frame in frames:
            await channel._enqueue_delivery(frame)

        with anyio.fail_after(1):
            await message_received.wait()

        task_group.cancel_scope.cancel()

    return received


async def test_basic_publish():
    """
    Tests publishing a message to a queue, and getting the message back.
    """

    async with _open_connection() as conn, conn.open_channel() as channel:
        queue = await channel.queue_declare(name="", exclusive=True)

        result = await channel.basic_get(queue.name)
        assert result is None
        await channel.basic_publish("", routing_key=queue.name, body=b"test")
        result = await channel.basic_get(queue.name)

        assert result is not None
        await result.ack()  # satisfy rabbitmq
        assert result.body == b"test"


async def test_consumption():
    """
    Tests consuming asynchronously.
    """

    async with _open_connection() as conn, conn.open_channel() as channel:
        queue = await channel.queue_declare(name="", exclusive=True)

        counter = 0
        for _i in range(0, 10):
            await channel.basic_publish("", routing_key=queue.name, body=b"test")
            counter += 1

        messages: list[AMQPMessage] = []
        queue = await channel.queue_declare(name=queue.name, passive=True)
        assert queue.message_count == 10

        async with channel.basic_consume(queue_name=queue.name) as agen:
            async for message in agen:
                messages.append(message)
                counter -= 1
                if counter <= 0:
                    break

        queue = await channel.queue_declare(name=queue.name, passive=True)
        assert queue.message_count == 0
        assert len(messages) == 10


async def test_acks():
    """
    Tests message acknowledgement.
    """

    async with _open_connection() as conn, conn.open_channel() as channel:
        queue = await channel.queue_declare(name="", exclusive=True)
        await channel.basic_publish("", routing_key=queue.name, body=b"test")

        # very cool amqp feature is that reject() will just get the server to immediately
        # requeue it.
        # so we have to use get instead of basic consume
        msg = await channel.basic_get(queue.name)
        assert msg is not None
        await msg.reject(requeue=True)

        assert (await channel.queue_declare(name=queue.name, passive=True)).message_count == 1
        msg = await channel.basic_get(queue.name)
        assert msg

        await msg.ack()
        assert (await channel.queue_declare(name=queue.name, passive=True)).message_count == 0


async def test_publishing_headers():
    """
    Tests publishing header data.
    """

    async with _open_connection() as conn, conn.open_channel() as channel:
        queue = await channel.queue_declare("", exclusive=True)
        headers = BasicHeader(message_id="123456")

        await channel.basic_publish("", routing_key=queue.name, body=b"", header=headers)

        message = await channel.basic_get(queue=queue.name, no_ack=True)
        assert message
        assert message.header == headers


async def test_return():
    """
    Tests message returning.
    """

    async with _open_connection() as conn:
        async with conn.open_channel() as channel:
            with pytest.raises(MessageReturnedError) as e:
                await channel.basic_publish("", routing_key="non-existent-queue", body=b"")

            # shouldn't close the channel
            assert channel.open

        assert e.value.reply_code == ReplyCode.no_route


async def test_return_preserves_returned_body_and_header():
    async with _open_connection() as conn, conn.open_channel() as channel:
        header = BasicHeader(message_id="returned-message")

        with pytest.raises(MessageReturnedError) as e:
            await channel.basic_publish(
                "",
                routing_key="non-existent-queue",
                body=b"returned body",
                header=header,
            )

        assert e.value.reply_code == ReplyCode.no_route
        assert e.value.body == b"returned body"
        assert e.value.header == header


async def test_basic_get_empty_frame_returns_none():
    message = await _read_delivery_message(
        [MethodFrame(channel_id=1, payload=BasicGetEmptyPayload(reserved_1=""))]
    )

    assert message is None


async def test_basic_get_ok_reassembles_empty_body():
    method_frame, header_frame, _body_frame = _get_ok_frames("queue", b"")

    message = await _read_delivery_message([method_frame, header_frame])

    assert message is not None
    assert message.body == b""
    assert message.envelope.routing_key == "queue"
    assert message.envelope.delivery_tag == 1


async def test_basic_get_ok_reassembles_body_across_multiple_frames():
    header = BasicHeader(message_id="get-message")
    method_frame, header_frame, _body_frame = _get_ok_frames("queue", b"hello world", header=header)

    message = await _read_delivery_message(
        [
            method_frame,
            header_frame,
            BodyFrame(channel_id=1, data=b"hello "),
            BodyFrame(channel_id=1, data=b"world"),
        ]
    )

    assert message is not None
    assert message.body == b"hello world"
    assert message.header == header
    assert message.envelope.routing_key == "queue"


async def test_basic_deliver_reassembles_empty_body_and_envelope():
    method_frame, header_frame, _body_frame = _delivery_frames("queue", b"", channel_id=1)

    message = await _read_delivery_message([method_frame, header_frame])

    assert message is not None
    assert message.body == b""
    assert message.envelope.consumer_tag == "consumer"
    assert message.envelope.routing_key == "queue"
    assert message.envelope.delivery_tag == 1


async def test_basic_deliver_reassembles_body_and_headers_across_multiple_frames():
    header = BasicHeader(message_id="delivered-message")
    method_frame, header_frame, _body_frame = _delivery_frames(
        "queue", b"hello world", channel_id=1, header=header
    )

    message = await _read_delivery_message(
        [
            method_frame,
            header_frame,
            BodyFrame(channel_id=1, data=b"hello "),
            BodyFrame(channel_id=1, data=b"world"),
        ]
    )

    assert message is not None
    assert message.body == b"hello world"
    assert message.header == header
    assert message.envelope.consumer_tag == "consumer"
    assert message.envelope.routing_key == "queue"


async def test_empty_return_content_frames_do_not_buffer_as_deliveries():
    connection = AMQPConnection(typing.cast(typing.Any, object()), channel_buffer_size=8)
    connection._actual_heartbeat_interval = 10
    channel = Channel(1, connection, stream_buffer_size=8)
    connection._channel_channels[1] = channel

    send_frames, receive_frames = anyio.create_memory_object_stream[Frame](10)
    heartbeat_read = anyio.Event()

    async def read_single_frame() -> Frame:
        frame = await receive_frames.receive()
        if isinstance(frame, HeartbeatFrame):
            heartbeat_read.set()
        return frame

    connection._read_single_frame = read_single_frame  # type: ignore[method-assign]

    received_return: MethodFrame[BasicReturnPayload] | None = None
    receiver_ready = anyio.Event()
    return_received = anyio.Event()

    async def receive_return() -> None:
        nonlocal received_return
        receiver_ready.set()
        frame = await channel._receive_frame()
        assert isinstance(frame.payload, BasicReturnPayload)
        received_return = typing.cast(MethodFrame[BasicReturnPayload], frame)
        return_received.set()

    return_frame, header_frame, _body_frame = _return_frames("missing-queue", b"")

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(connection._listen_for_messages)
        task_group.start_soon(receive_return)

        await receiver_ready.wait()
        await send_frames.send(return_frame)
        await send_frames.send(header_frame)
        await send_frames.send(HeartbeatFrame(channel_id=0))

        with anyio.fail_after(1):
            await return_received.wait()
            await heartbeat_read.wait()

        assert received_return == return_frame
        assert channel.current_buffer_size == 0

        task_group.cancel_scope.cancel()


async def test_multi_frame_return_content_frames_do_not_buffer_as_deliveries():
    connection = AMQPConnection(typing.cast(typing.Any, object()), channel_buffer_size=8)
    connection._actual_heartbeat_interval = 10
    channel = Channel(1, connection, stream_buffer_size=8)
    connection._channel_channels[1] = channel

    send_frames, receive_frames = anyio.create_memory_object_stream[Frame](10)
    heartbeat_read = anyio.Event()

    async def read_single_frame() -> Frame:
        frame = await receive_frames.receive()
        if isinstance(frame, HeartbeatFrame):
            heartbeat_read.set()
        return frame

    connection._read_single_frame = read_single_frame  # type: ignore[method-assign]

    received_return: MethodFrame[BasicReturnPayload] | None = None
    receiver_ready = anyio.Event()
    return_received = anyio.Event()

    async def receive_return() -> None:
        nonlocal received_return
        receiver_ready.set()
        frame = await channel._receive_frame()
        assert isinstance(frame.payload, BasicReturnPayload)
        received_return = typing.cast(MethodFrame[BasicReturnPayload], frame)
        return_received.set()

    return_frame, header_frame, _body_frame = _return_frames("missing-queue", b"hello world")

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(connection._listen_for_messages)
        task_group.start_soon(receive_return)

        await receiver_ready.wait()
        await send_frames.send(return_frame)
        await send_frames.send(header_frame)
        await send_frames.send(BodyFrame(channel_id=1, data=b"hello "))
        await send_frames.send(BodyFrame(channel_id=1, data=b"world"))
        await send_frames.send(HeartbeatFrame(channel_id=0))

        with anyio.fail_after(1):
            await return_received.wait()
            await heartbeat_read.wait()

        assert received_return == return_frame
        assert channel.current_buffer_size == 0

        task_group.cancel_scope.cancel()


async def test_return_content_frames_do_not_buffer_as_deliveries():
    connection = AMQPConnection(typing.cast(typing.Any, object()), channel_buffer_size=8)
    connection._actual_heartbeat_interval = 10
    channel = Channel(1, connection, stream_buffer_size=8)
    connection._channel_channels[1] = channel

    send_frames, receive_frames = anyio.create_memory_object_stream[Frame](10)
    heartbeat_read = anyio.Event()
    second_heartbeat_read = anyio.Event()
    heartbeat_count = 0

    async def read_single_frame() -> Frame:
        nonlocal heartbeat_count
        frame = await receive_frames.receive()
        if isinstance(frame, HeartbeatFrame):
            heartbeat_count += 1
            heartbeat_read.set()
            if heartbeat_count == 2:
                second_heartbeat_read.set()
        return frame

    connection._read_single_frame = read_single_frame  # type: ignore[method-assign]

    received_return: MethodFrame[BasicReturnPayload] | None = None
    receiver_ready = anyio.Event()
    return_received = anyio.Event()

    async def receive_return() -> None:
        nonlocal received_return
        receiver_ready.set()
        frame = await channel._receive_frame()
        assert isinstance(frame.payload, BasicReturnPayload)
        received_return = typing.cast(MethodFrame[BasicReturnPayload], frame)
        return_received.set()

    return_frame, header_frame, body_frame = _return_frames("missing-queue")

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(connection._listen_for_messages)
        task_group.start_soon(receive_return)

        await receiver_ready.wait()
        await send_frames.send(return_frame)
        await send_frames.send(HeartbeatFrame(channel_id=0))
        await send_frames.send(header_frame)
        await send_frames.send(body_frame)
        await send_frames.send(HeartbeatFrame(channel_id=0))

        with anyio.fail_after(1):
            await return_received.wait()
            await heartbeat_read.wait()
            await second_heartbeat_read.wait()

        assert received_return == return_frame
        assert channel.current_buffer_size == 0

        task_group.cancel_scope.cancel()


async def test_many_returned_publishes_do_not_block_connection_reader():
    connection = AMQPConnection(typing.cast(typing.Any, object()), channel_buffer_size=8)
    connection._actual_heartbeat_interval = 10
    channel = Channel(1, connection, stream_buffer_size=8)
    connection._channel_channels[1] = channel

    send_frames, receive_frames = anyio.create_memory_object_stream[Frame](100)
    heartbeat_read = anyio.Event()

    async def read_single_frame() -> Frame:
        frame = await receive_frames.receive()
        if isinstance(frame, HeartbeatFrame):
            heartbeat_read.set()
        return frame

    connection._read_single_frame = read_single_frame  # type: ignore[method-assign]

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(connection._listen_for_messages)

        for index in range(channel.max_buffer_size):
            for frame in _return_frames(f"missing-queue-{index}"):
                await send_frames.send(frame)
        await send_frames.send(HeartbeatFrame(channel_id=0))

        with anyio.fail_after(1):
            await heartbeat_read.wait()

        assert channel.current_buffer_size == 0

        task_group.cancel_scope.cancel()


async def test_returned_message_does_not_interfere_with_delivery_on_another_channel():
    connection = AMQPConnection(typing.cast(typing.Any, object()), channel_buffer_size=8)
    connection._actual_heartbeat_interval = 10
    return_channel = Channel(1, connection, stream_buffer_size=8)
    delivery_channel = Channel(2, connection, stream_buffer_size=8)
    connection._channel_channels[1] = return_channel
    connection._channel_channels[2] = delivery_channel

    send_frames, receive_frames = anyio.create_memory_object_stream[Frame](20)
    heartbeat_read = anyio.Event()

    async def read_single_frame() -> Frame:
        frame = await receive_frames.receive()
        if isinstance(frame, HeartbeatFrame):
            heartbeat_read.set()
        return frame

    connection._read_single_frame = read_single_frame  # type: ignore[method-assign]

    received_return: MethodFrame[BasicReturnPayload] | None = None
    return_receiver_ready = anyio.Event()
    return_received = anyio.Event()

    async def receive_return() -> None:
        nonlocal received_return
        return_receiver_ready.set()
        frame = await return_channel._receive_frame()
        assert isinstance(frame.payload, BasicReturnPayload)
        received_return = typing.cast(MethodFrame[BasicReturnPayload], frame)
        return_received.set()

    received_delivery: AMQPMessage | None = None
    delivery_received = anyio.Event()

    async def receive_delivery() -> None:
        nonlocal received_delivery
        received_delivery = await delivery_channel._receive_delivery_message()
        delivery_received.set()

    return_frame, return_header, return_body = _return_frames("missing-queue", b"returned")
    deliver_frame, deliver_header, deliver_body = _delivery_frames("queue", b"delivered")

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(connection._listen_for_messages)
        task_group.start_soon(receive_return)
        task_group.start_soon(receive_delivery)

        await return_receiver_ready.wait()
        await send_frames.send(return_frame)
        await send_frames.send(deliver_frame)
        await send_frames.send(deliver_header)
        await send_frames.send(return_header)
        await send_frames.send(deliver_body)
        await send_frames.send(return_body)
        await send_frames.send(HeartbeatFrame(channel_id=0))

        with anyio.fail_after(1):
            await return_received.wait()
            await delivery_received.wait()
            await heartbeat_read.wait()

        assert received_return == return_frame
        assert received_delivery is not None
        assert received_delivery.body == b"delivered"
        assert return_channel.current_buffer_size == 0
        assert delivery_channel.current_buffer_size == 0

        task_group.cancel_scope.cancel()
