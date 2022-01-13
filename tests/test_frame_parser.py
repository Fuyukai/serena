from serena.frameparser import NEED_DATA, FrameParser
from serena.payloads.method import MethodFrame

data = (
    b"\x01\x00\x00\x00\x00\x02\x01\x00\n\x00\n\x00\t\x00\x00\x01\xdc\x0ccapabilitiesF\x00\x00\x00"
    b"\xc7\x12publisher_confirmst\x01\x1aexchange_exchange_bindingst\x01\nbasic.nackt\x01\x16"
    b"consumer_cancel_notifyt\x01\x12connection.blockedt\x01\x13consumer_prioritiest\x01\x1c"
    b"authentication_failure_closet\x01\x10per_consumer_qost\x01\x0fdirect_reply_tot\x01\x0c"
    b"cluster_nameS\x00\x00\x00\x1crabbit@antiskill.localdomain\tcopyrightS\x00\x00\x007Copyright"
    b" (c) 2007-2021 VMware, Inc. or its affiliates.\x0binformationS\x00\x00\x009Licensed under the"
    b" MPL 2.0. Website: https://rabbitmq.com\x08platformS\x00\x00\x00\x0fErlang/OTP"
    b" 24.2\x07productS\x00\x00\x00\x08RabbitMQ\x07versionS\x00\x00\x00\x063.8.22\x00\x00\x00\x0e"
    b"AMQPLAIN PLAIN\x00\x00\x00\x05en_US\xce"
)


def test_double_chunk_truncation():
    """
    Tests frame truncation when receiving a frame in two parts.
    """

    parser = FrameParser()
    pt1_1 = data[0:100]
    parser.receive_data(pt1_1)
    assert parser.next_frame() == NEED_DATA
    pt1_2 = data[100:]
    parser.receive_data(pt1_2)
    assert isinstance(parser.next_frame(), MethodFrame)


def test_triple_chunk_truncation():
    """
    Tests frame truncation when receiving multiple unfinished chunks.
    """

    parser = FrameParser()
    chunks = data[0:100], data[100:200], data[200:300], data[300:400]
    for chunk in chunks:
        parser.receive_data(chunk)
        assert parser.next_frame() == NEED_DATA

    parser.receive_data(data[400:])
    assert isinstance(parser.next_frame(), MethodFrame)


def test_overrunning_frame():
    """
    Tests when a frame overruns.
    """

    parser = FrameParser()
    new_data = data + data
    parser.receive_data(new_data)

    assert isinstance(parser.next_frame(), MethodFrame)
    assert isinstance(parser.next_frame(), MethodFrame)
    assert parser.next_frame() == NEED_DATA


def test_overrunning_with_partial_frame():
    """
    Tests an overrunning frame with partial data on the end.
    """

    parser = FrameParser()
    new_data = data + data[:150]
    parser.receive_data(new_data)

    assert isinstance(parser.next_frame(), MethodFrame)
    assert parser.next_frame() == NEED_DATA
    parser.receive_data(data[150:])
    assert isinstance(parser.next_frame(), MethodFrame)
