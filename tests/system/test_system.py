"""Full system tests for MDP implementation
"""

# Copyright (c) 2018 Shoppimon LTD
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from majortomo import Client

from .conftest import run_broker, run_worker


def test_full_system_messages_delivered(socket_url):
    """Test that the entire system with one worker and client, and verify that all messages are
    delivered in order
    """
    messages = [str(i).encode('ascii') for i in range(1000)]
    replies = []
    with run_broker(socket_url), run_worker(socket_url), Client(broker_url=socket_url) as client:
        for msg in messages:
            client.send(b'my-service', msg)
            replies.append(client.recv_all_as_list(timeout=1.0)[0])

    assert replies == messages


def test_full_system_messages_delivered_multiframe(socket_url):
    """Verify multi-frame messages are properly delivered
    """
    messages = [[str(i * j).encode('ascii') for j in range(5)] for i in range(50)]
    replies = []
    with run_broker(socket_url), run_worker(socket_url), Client(broker_url=socket_url) as client:
        for msg in messages:
            client.send(b'my-service', *msg)
            replies.append(client.recv_all_as_list(timeout=1.0))

    assert replies == messages


def test_full_system_messages_delivered_multiple_workers(socket_url):
    """Verify messages are properly processed in a multi-worker environment
    """
    messages = set([str(i).encode('ascii') for i in range(300)])
    replies = set()

    worker1 = run_worker(socket_url)
    worker2 = run_worker(socket_url)
    worker3 = run_worker(socket_url)

    with run_broker(socket_url), Client(broker_url=socket_url) as client, worker1, worker2, worker3:
        for msg in messages:
            client.send(b'my-service', msg)
            replies.add(client.recv_all_as_list(timeout=1.0)[0])

    assert replies == messages


def test_full_system_messages_delivered_multiple_workers_and_clients(socket_url):
    """Verify multi-frame messages are properly delivered
    """
    c1_messages = [str(i).encode('ascii') for i in range(30)]
    c2_messages = [str(i).encode('ascii') for i in range(30, 60)]

    c1_replies = set()
    c2_replies = set()

    client1 = Client(broker_url=socket_url)
    client2 = Client(broker_url=socket_url)

    worker1 = run_worker(socket_url)
    worker2 = run_worker(socket_url)

    with run_broker(socket_url), client1, client2, worker1, worker2:
        for i in range(30):
            client1.send(b'my-service', c1_messages[i])
            client2.send(b'my-service', c2_messages[i])
            c1_replies.add(client1.recv_all_as_list(timeout=1.0)[0])
            c2_replies.add(client2.recv_all_as_list(timeout=1.0)[0])

    assert c1_replies == set(c1_messages)
    assert c2_replies == set(c2_messages)


def test_full_system_partial_multiframe_replies(socket_url):
    """Verify multi-frame messages with partial replies are delivered properly
    """
    message_parts = [[b''.join([str(i * j).encode('ascii') for j in range(5)]) for i in range(5)] for _ in range(5)]

    def handler(_):
        return (p for p in message_parts)

    worker = run_worker(socket_url, handler_func=handler, send_partials=True)

    reply = []
    with run_broker(socket_url), worker, Client(broker_url=socket_url) as client:
        client.send(b'my-service', b'get-parts')
        for _ in message_parts[0:-1]:
            reply.append(client.recv_part(timeout=1.0))
        reply.append(client.recv_part(timeout=1.0))

    assert message_parts == reply


def test_full_system_partial_multiframe_replies_receive_all(socket_url):
    """Verify multi-frame messages with partial replies are delivered properly
    """
    message_parts = [[b''.join([str(i * j).encode('ascii') for j in range(5)]) for i in range(5)] for _ in range(5)]

    def handler(_):
        return (p for p in message_parts)

    worker = run_worker(socket_url, handler_func=handler, send_partials=True)

    with run_broker(socket_url), worker, Client(broker_url=socket_url) as client:
        client.send(b'my-service', b'get-parts')
        reply = list(client.recv_all())

    assert message_parts == reply
