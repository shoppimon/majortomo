"""Testing Configuration for System Tests
"""
import logging
import os
import sys
from contextlib import contextmanager
from multiprocessing import Process
from subprocess import Popen
from tempfile import mkstemp

import pytest
import zmq

from majortomo import Worker, WorkerRequestsIterator
from majortomo.config import default_config

try:
    from importlib import reload
except ImportError:
    # Python 2.7 has 'reload' as a builtin function
    pass

HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 3.0


@pytest.fixture()
def socket_url():
    fh, path = mkstemp(prefix='majortomo-test-')
    os.close(fh)
    try:
        yield 'ipc://' + path
    finally:
        os.unlink(path)


@contextmanager
def run_broker(bind):
    """Run Broker in a thread, with context manager
    """
    command = [sys.executable, '-m', 'majortomo.broker',
               '-b', bind,
               '-i', str(HEARTBEAT_INTERVAL),
               '-t', str(HEARTBEAT_TIMEOUT),
               '--debug']

    broker_proc = Popen(command)

    try:
        yield
    finally:
        broker_proc.terminate()
        broker_proc.wait()


@contextmanager
def run_worker(connect, service_name='my-service', handler_func=lambda r: r, send_partials=False):

    def _worker_process():
        # Reset logging
        logging.shutdown()
        reload(logging)
        logging.basicConfig(level=logging.DEBUG, format=default_config['logging']['formatters']['default']['format'])

        # Reset ZMQ context
        zmq.Context.instance().destroy()

        logging.debug("Starting worker process for service %s", service_name)
        worker = Worker(broker_url=connect, service_name=service_name, heartbeat_interval=HEARTBEAT_INTERVAL,
                        heartbeat_timeout=HEARTBEAT_TIMEOUT, zmq_linger=500)
        worker_iter = WorkerRequestsIterator(worker)
        worker_iter.stop_on_signal()

        for request in worker_iter:
                response = list(handler_func(request))
                if send_partials and len(response) > 1:
                    partials = response[0:-1]
                    final = response[-1]
                    worker_iter.send_reply_from_iterable(partials, final=final)
                else:
                    worker_iter.send_reply_final(response)

        logging.debug("Stopping worker process for service %s", service_name)

    proc = Process(target=_worker_process)
    try:
        proc.start()
        yield
        if not proc.is_alive():
            raise RuntimeError("Worker process exited prematurely with code %d", proc.exitcode)
    finally:
        proc.terminate()
        proc.join()
