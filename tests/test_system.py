"""Full system tests for MDP implementation
"""
from contextlib import contextmanager
from threading import Thread

from majortomo import Client, Worker, WorkerRequestsIterator
from majortomo import Broker

DEFAULT_BROKER_URL = 'inproc://mdp-tests'
HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 3.0


def test_full_system_messages_delivered():
    """Test that the entire system with one worker and client, and verify that all messages are
    delivered in order
    """
    messages = [str(i).encode('ascii') for i in range(1000)]
    replies = []
    with broker_thread(), worker_thread(), Client(broker_url=DEFAULT_BROKER_URL) as client:
        for msg in messages:
            client.send(b'my-service', msg)
            replies.append(client.recv_all_as_list(timeout=1.0)[0])

    assert replies == messages


def test_full_system_messages_delivered_multiframe():
    """Verify multi-frame messages are properly delivered
    """
    messages = [[str(i * j).encode('ascii') for j in range(5)] for i in range(50)]
    replies = []
    with broker_thread(), worker_thread(), Client(broker_url=DEFAULT_BROKER_URL) as client:
        for msg in messages:
            client.send(b'my-service', *msg)
            replies.append(client.recv_all_as_list(timeout=1.0))

    assert replies == messages


def test_full_system_messages_delivered_multiple_workers():
    """Verify messages are properly processed in a multi-worker environment
    """
    messages = set([str(i).encode('ascii') for i in range(300)])
    replies = set()

    worker1 = worker_thread()
    worker2 = worker_thread()
    worker3 = worker_thread()

    with broker_thread(), Client(broker_url=DEFAULT_BROKER_URL) as client, worker1, worker2, worker3:
        for msg in messages:
            client.send(b'my-service', msg)
            replies.add(client.recv_all_as_list(timeout=1.0)[0])

    assert replies == messages


def test_full_system_messages_delivered_multiple_workers_and_clients():
    """Verify multi-frame messages are properly delivered
    """
    c1_messages = [str(i).encode('ascii') for i in range(30)]
    c2_messages = [str(i).encode('ascii') for i in range(30, 60)]

    c1_replies = set()
    c2_replies = set()

    client1 = Client(broker_url=DEFAULT_BROKER_URL)
    client2 = Client(broker_url=DEFAULT_BROKER_URL)

    worker1 = worker_thread()
    worker2 = worker_thread()

    with broker_thread(), client1, client2, worker1, worker2:
        for i in range(30):
            client1.send(b'my-service', c1_messages[i])
            client2.send(b'my-service', c2_messages[i])
            c1_replies.add(client1.recv_all_as_list(timeout=1.0)[0])
            c2_replies.add(client2.recv_all_as_list(timeout=1.0)[0])

    assert c1_replies == set(c1_messages)
    assert c2_replies == set(c2_messages)


def test_full_system_partial_multiframe_replies():
    """Verify multi-frame messages with partial replies are delivered properly
    """
    message_parts = [[b''.join([str(i * j).encode('ascii') for j in range(5)]) for i in range(5)] for _ in range(5)]

    def handler(_):
        return (p for p in message_parts)

    worker = worker_thread(handler_func=handler, send_partials=True)

    reply = []
    with broker_thread(), worker, Client(broker_url=DEFAULT_BROKER_URL) as client:
        client.send(b'my-service', b'get-parts')
        for _ in message_parts[0:-1]:
            reply.append(client.recv_part(timeout=1.0))
        reply.append(client.recv_part(timeout=1.0))

    assert message_parts == reply


def test_full_system_partial_multiframe_replies_receive_all():
    """Verify multi-frame messages with partial replies are delivered properly
    """
    message_parts = [[b''.join([str(i * j).encode('ascii') for j in range(5)]) for i in range(5)] for _ in range(5)]

    def handler(_):
        return (p for p in message_parts)

    worker = worker_thread(handler_func=handler, send_partials=True)

    with broker_thread(), worker, Client(broker_url=DEFAULT_BROKER_URL) as client:
        client.send(b'my-service', b'get-parts')
        reply = list(client.recv_all())

    assert message_parts == reply


@contextmanager
def broker_thread(bind=DEFAULT_BROKER_URL):
    """Run Broker in a thread, with context manager
    """
    broker = Broker(bind=bind, heartbeat_interval=HEARTBEAT_INTERVAL, heartbeat_timeout=HEARTBEAT_TIMEOUT)
    thread = Thread(target=broker.run, name='BrokerThread')
    try:
        thread.start()
        yield
    finally:
        broker.stop()
        thread.join(timeout=3.0)


@contextmanager
def worker_thread(connect=DEFAULT_BROKER_URL, service_name='my-service', handler_func=lambda r: r, send_partials=False):
    worker = Worker(broker_url=connect, service_name=service_name, heartbeat_interval=HEARTBEAT_INTERVAL,
                    heartbeat_timeout=HEARTBEAT_TIMEOUT, zmq_linger=500)

    worker_iter = WorkerRequestsIterator(worker)

    def _worker_thread():
        for request in worker_iter:
            response = list(handler_func(request))
            if send_partials and len(response) > 1:
                partials = response[0:-1]
                final = response[-1]
                worker_iter.send_reply_from_iterable(partials, final=final)
            else:
                worker_iter.send_reply_final(response)

    thread = Thread(target=_worker_thread, name='WorkerThread')
    try:
        thread.start()
        yield worker_iter
    finally:
        worker_iter.stop()
        thread.join(timeout=3.0)
