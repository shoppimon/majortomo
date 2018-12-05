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

import time

from pytest import mark, raises

from majortomo import broker
from majortomo import error


def test_broker_can_bind_again_after_close():
    b = broker.Broker('inproc://foobar')
    b.bind()
    b.close()
    b.bind()
    b.close()


def test_broker_can_only_bind_once():
    b = broker.Broker('inproc://foobar')
    try:
        b.bind()
        with raises(RuntimeError):
            b.bind()
    finally:
        b.close()


def test_worker_is_expired():
    now = time.time()
    w = broker.Worker(b'foo', b'bar', now + 5.0, now + 1.0)
    assert w.is_expired(now) is False
    assert w.is_expired(now + 4.9) is False
    assert w.is_expired(now + 10.0) is True
    assert w.is_expired(now + 5.0) is True


def test_worker_is_heartbeat():
    now = time.time()
    w = broker.Worker(b'foo', b'bar', now + 5.0, now + 1.0)
    assert w.is_heartbeat(now) is False
    assert w.is_heartbeat(now + 0.5) is False
    assert w.is_heartbeat(now + 2.0) is True
    assert w.is_heartbeat(now + 1.0) is True


def test_service_worker_added():
    svc = broker.Service()
    assert 0 == svc.available_workers
    svc.add_worker(broker.Worker(b'w1', b's1', time.time(), time.time()))
    assert 1 == svc.available_workers
    svc.add_worker(broker.Worker(b'w2', b's1', time.time(), time.time()))
    assert 2 == svc.available_workers


def test_service_worker_removed():
    svc = broker.Service()
    assert 0 == svc.available_workers
    svc.add_worker(broker.Worker(b'w1', b's1', time.time(), time.time()))
    svc.add_worker(broker.Worker(b'w2', b's1', time.time(), time.time()))
    assert 2 == svc.available_workers
    svc.remove_worker(b'w1')
    svc.remove_worker(b'w2')
    assert 0 == svc.available_workers


def test_service_request_queued():
    svc = broker.Service()
    assert 0 == svc.queued_requests
    svc.queue_request(b'c1', [b'f1', b'f2'])
    assert 1 == svc.queued_requests
    svc.queue_request(b'c1', [b'f1', b'f2'])
    assert 2 == svc.queued_requests


def test_service_request_dequeued():
    message = [b'f1', b'f2']
    worker = broker.Worker(b'w1', b's1', time.time(), time.time())
    client = b'c1'

    svc = broker.Service()
    svc.add_worker(worker)

    dequeued = list(svc.dequeue_pending())
    assert 0 == len(dequeued)

    svc.queue_request(client, message)
    dequeued = list(svc.dequeue_pending())
    assert 1 == len(dequeued)

    assert dequeued[0] == (message, worker, client)


def test_service_dequeue_no_workers():
    svc = broker.Service()
    svc.queue_request(b'c1', [b'f1', b'f2'])
    svc.queue_request(b'c1', [b'f1', b'f2'])
    svc.add_worker(broker.Worker(b'w1', b's1', time.time(), time.time()))

    assert 1 == svc.available_workers
    assert 2 == svc.queued_requests

    dequeued = list(svc.dequeue_pending())
    assert 1 == len(dequeued)

    assert 0 == svc.available_workers
    assert 1 == svc.queued_requests

    dequeued = list(svc.dequeue_pending())
    assert 0 == len(dequeued)


def test_service_dequeue_no_requests():
    svc = broker.Service()
    svc.queue_request(b'c1', [b'f1', b'f2'])
    svc.add_worker(broker.Worker(b'w1', b's1', time.time(), time.time()))
    svc.add_worker(broker.Worker(b'w2', b's1', time.time(), time.time()))

    assert 2 == svc.available_workers
    assert 1 == svc.queued_requests

    dequeued = list(svc.dequeue_pending())
    assert 1 == len(dequeued)

    assert 1 == svc.available_workers
    assert 0 == svc.queued_requests

    dequeued = list(svc.dequeue_pending())
    assert 0 == len(dequeued)


def test_service_container_yields_expired_workers():
    now = time.time()
    container = broker.ServicesContainer()
    container.add_worker(b'w1-good', b'service-1', now + 5, now)
    container.add_worker(b'w2-good', b'service-2', now + 1, now)
    container.add_worker(b'w3-exp', b'service-1', now - 0.5, now)
    container.add_worker(b'w4-exp', b'service-2', now - 1000, now)

    expired = list(container.expired_workers())
    assert 2 == len(expired)
    for w in expired:
        assert w.id in {b'w3-exp', b'w4-exp'}


def test_service_container_yields_expired_busy_workers():
    now = time.time()
    container = broker.ServicesContainer()
    container.add_worker(b'w1-good', b'service-1', now + 5, now)
    container.add_worker(b'w2-good', b'service-2', now + 1, now)
    container.add_worker(b'w3-exp', b'service-1', now - 0.5, now)
    container.add_worker(b'w4-exp', b'service-2', now - 1000, now)
    container.set_worker_busy(b'w2-good', now - 1)

    expired = list(container.expired_workers())
    assert 3 == len(expired)


def test_service_container_worker_removed():
    now = time.time()
    container = broker.ServicesContainer()
    container.add_worker(b'w1-good', b'service-1', now + 5, now)
    container.add_worker(b'w3-exp', b'service-1', now - 0.5, now)

    assert 1 == len(list(container.expired_workers()))

    for w in container.expired_workers():
        container.remove_worker(w.id)

    assert 0 == len(list(container.expired_workers()))


def test_service_container_busy_worker_removed():
    now = time.time()
    container = broker.ServicesContainer()
    container.add_worker(b'w1-good', b'service-1', now + 5, now)
    container.set_worker_busy(b'w1-good', now + 900.0)

    assert 0 == len(list(container.expired_workers()))

    container.remove_worker(b'w1-good')

    with raises(error.StateError):
        container.set_worker_available(b'w1-good', now + 5, now + 1)


def test_service_container_is_worker_busy():
    now = time.time()
    container = broker.ServicesContainer()
    container.add_worker(b'w1-good', b'service-1', now + 5, now)

    assert container.is_busy_worker(b'w1-good') is False

    container.set_worker_busy(b'w1-good', now + 900.0)
    assert container.is_busy_worker(b'w1-good') is True

    assert container.is_busy_worker(b'unknown-worker') is False


def test_service_container_non_existing_worker_removed():
    container = broker.ServicesContainer()
    with raises(error.StateError):
        container.remove_worker(b'w1-good')


def test_service_container_yields_heartbeat_workers():
    now = time.time()
    container = broker.ServicesContainer()
    container.add_worker(b'w1-good', b'service-1', now + 5, now + 1)
    container.add_worker(b'w2-good', b'service-2', now + 5, now + 2)
    container.add_worker(b'w3-time', b'service-1', now + 5, now)
    container.add_worker(b'w4-time', b'service-2', now + 5, now - 1)

    expired = list(container.heartbeat_workers())
    assert 2 == len(expired)
    for w in expired:
        assert w.id in {b'w3-time', b'w4-time'}


@mark.parametrize('id_,expected', [
    (b'\x01\x02\x03\x04', 16909060),
    ('\x01\x02\x03\x04', 16909060)
])
def test_id_to_int(id_, expected):
    assert expected == broker.id_to_int(id_)
