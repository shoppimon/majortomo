"""Majordomo / `MDP/0.2 <https://rfc.zeromq.org/spec:18/MDP/>`_ broker implementation.

Typically, call :py:func:`.main` to run the broker process.
"""

import argparse
import logging
import logging.config
import signal
import time
from collections import OrderedDict, defaultdict, deque
from itertools import chain
from typing import DefaultDict, Dict, Generator, List, Optional, Tuple, Union  # noqa: F401

import figcan
import yaml
import zmq

from majortomo import error, protocol
from majortomo.config import DEFAULT_BIND_URL, default_config


class Broker:

    def __init__(self, bind=DEFAULT_BIND_URL, heartbeat_interval=protocol.DEFAULT_HEARTBEAT_INTERVAL,
                 heartbeat_timeout=protocol.DEFAULT_HEARTBEAT_TIMEOUT,
                 busy_worker_timeout=protocol.DEFAULT_BUSY_WORKER_TIMEOUT,
                 zmq_context=None):
        # type: (str, float, float, float, Optional[zmq.Context]) -> None
        self._log = logging.getLogger(__name__)
        self._bind_url = bind
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._busy_worker_timeout = busy_worker_timeout

        self._context = zmq_context if zmq_context else zmq.Context.instance()
        self._socket = None  # type: zmq.Socket
        self._services = ServicesContainer()
        self._stop = False

    def run(self):
        """Run in a main loop handling all incoming requests, until `stop()` is called
        """
        self._log.info("MDP Broker starting up")
        self._stop = False
        self.bind()

        try:
            while not self._stop:
                try:
                    message = self.receive()
                    if message:
                        self.handle_message(message)
                except Exception:
                    self._log.exception("Message handling failed")

        finally:
            self.close()
            self._log.info("MDP Broker shutting down")

    def stop(self):
        """Stop the broker's main loop
        """
        self._stop = True

    def bind(self):
        """Bind the ZMQ socket
        """
        if self._socket:
            raise error.StateError("Socket is already bound")

        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.rcvtimeo = int(self._heartbeat_interval * 1000)
        self._socket.bind(self._bind_url)
        self._log.info("Broker listening on %s", self._bind_url)

    def close(self):
        if not self._socket:
            return
        self._socket.disconnect(self._bind_url)
        self._socket.close()
        self._socket = None
        self._log.info("Broker socket closing")

    def receive(self):
        """Run until a message is received
        """
        while True:
            self._purge_expired_workers()
            self._send_heartbeat()
            try:
                frames = self._socket.recv_multipart()
            except zmq.error.Again:
                if self._socket is None or self._stop:
                    self._log.debug("Socket has been closed or broker has been shut down, breaking from recv loop")
                    break
                continue

            self._log.debug("Got message of %d frames", len(frames))
            try:
                return self._parse_incoming_message(frames)
            except error.ProtocolError as e:
                self._log.warning(str(e))
                continue

    def handle_message(self, message):
        # type: (protocol.Message) -> None
        """Handle incoming message
        """
        if message.header == protocol.WORKER_HEADER:
            self._handle_worker_message(message)
        elif message.header == protocol.CLIENT_HEADER:
            self._handle_client_message(message)
        else:
            raise error.ProtocolError("Unexpected protocol header: {}".format(message.header))

        self._dispatch_queued_messages()

    def _handle_worker_message(self, message):
        # type: (protocol.Message) -> None
        """Handle message from a worker
        """
        if message.command == protocol.HEARTBEAT:
            self._handle_worker_heartbeat(message.client)
        elif message.command == protocol.READY:
            self._handle_worker_ready(message.client, message.message[0])
        elif message.command == protocol.FINAL:
            self._handle_worker_final(message.client, message.message[0], message.message[2:])
        elif message.command == protocol.PARTIAL:
            self._handle_worker_partial(message.client, message.message[0], message.message[2:])
        elif message.command == protocol.DISCONNECT:
            self._handle_worker_disconnect(message.client)
        else:
            self._send_to_worker(message.client, protocol.DISCONNECT)
            raise error.ProtocolError("Unexpected command from worker: {}".format(message.command))

    def _handle_worker_heartbeat(self, worker_id):
        # type: (bytes) -> None
        """Heartbeat from worker
        """
        worker = self._services.get_worker(worker_id)
        if worker is None:
            if not self._services.is_busy_worker(worker_id):
                self._log.warning("Got HEARTBEAT from unknown worker: %d", id_to_int(worker_id))
                self._send_to_worker(worker_id, protocol.DISCONNECT)
            return

        self._log.debug("Got HEARTBEAT from worker: %d", id_to_int(worker_id))
        worker.expire_at = time.time() + self._heartbeat_timeout

    def _handle_worker_ready(self, worker_id, service):
        # type: (bytes, bytes) -> None
        self._log.info("Got READY from worker: %d", id_to_int(worker_id))
        now = time.time()
        self._services.add_worker(worker_id, service, now + self._heartbeat_timeout, now + self._heartbeat_interval)

    def _handle_worker_partial(self, worker_id, client_id, body):
        # type: (bytes, bytes, List[bytes]) -> None
        self._log.info("Got PARTIAL from worker: %d to client: %d", id_to_int(worker_id), id_to_int(client_id))
        self._send_to_client(client_id, protocol.PARTIAL, body)

    def _handle_worker_final(self, worker_id, client_id, body):
        # type: (bytes, bytes, List[bytes]) -> None
        self._log.info("Got FINAL from worker: %d to client: %d", id_to_int(worker_id), id_to_int(client_id))
        self._send_to_client(client_id, protocol.FINAL, body)
        now = time.time()
        self._services.set_worker_available(worker_id, now + self._heartbeat_timeout, now + self._heartbeat_interval)

    def _handle_worker_disconnect(self, worker_id):
        # type: (bytes) -> None
        self._log.info("Got DISCONNECT from worker: %d", id_to_int(worker_id))
        try:
            self._services.remove_worker(worker_id)
        except KeyError:
            self._log.info("Got DISCONNECT from unknown worker: %d; ignoring", id_to_int(worker_id))

    def _handle_client_message(self, message):
        # type: (protocol.Message) -> None
        """Handle message from a client
        """
        assert message.command == protocol.REQUEST
        if len(message.message) < 2:
            raise error.ProtocolError("Client REQUEST message is expected to be at least 2 frames long, got {}".format(
                len(message.message)))

        service_name = message.message[0]
        body = message.message[1:]

        # TODO: Plug-in MMA handling

        self._log.debug("Queueing client request from %d to %s", id_to_int(message.client),
                        service_name.decode('ascii'))
        self._services.queue_client_request(message.client, service_name, body)

    def _dispatch_queued_messages(self):
        """Dispatch all queued messages to available workers
        """
        expire_at = time.time() + self._busy_worker_timeout
        for message, worker, client in self._services.dequeue_pending():
            self._send_to_worker(worker.id, protocol.REQUEST, [client, b''] + message)
            self._services.set_worker_busy(worker.id, expire_at=expire_at)

    def _purge_expired_workers(self):
        """Purge expired workers from the services container
        """
        for worker in list(self._services.expired_workers()):  # Copying to list as we are going to mutate
            self._log.debug("Worker %d timed out after %0.2f sec, purging", id_to_int(worker.id),
                            self._busy_worker_timeout if worker.is_busy else self._heartbeat_timeout)
            self._send_to_worker(worker.id, protocol.DISCONNECT)
            self._services.remove_worker(worker.id)

    def _send_heartbeat(self):
        """Send heartbeat to all workers that didn't get any messages recently
        """
        now = time.time()
        for worker in self._services.heartbeat_workers():
            self._log.debug("Sending heartbeat to worker: %d", id_to_int(worker.id))
            self._send_to_worker(worker.id, protocol.HEARTBEAT)
            worker.next_heartbeat = now + self._heartbeat_interval

    def _send_to_worker(self, worker_id, command, body=None):
        # type: (bytes, bytes, Optional[List[bytes]]) -> None
        """Send message to worker
        """
        if body is None:
            body = []
        self._socket.send_multipart([worker_id, b'', protocol.WORKER_HEADER, command] + body)

    def _send_to_client(self, client_id, command, body):
        # type: (bytes, bytes, List[bytes]) -> None
        """Send message to client
        """
        self._socket.send_multipart([client_id, b'', protocol.CLIENT_HEADER, command] + body)

    @staticmethod
    def _parse_incoming_message(frames):
        # type: (List[bytes]) -> protocol.Message
        """Parse and verify incoming message
        """
        if len(frames) < 4:
            raise error.ProtocolError("Unexpected message length: expecting at least 4 frames, got {}".format(
                len(frames)))

        if frames[1] != b'':
            raise error.ProtocolError("Expecting empty frame 1, got {} bytes".format(len(frames[1])))

        return protocol.Message(client=frames[0], header=frames[2], command=frames[3], message=frames[4:])


class Worker:
    """Worker objects represent a connected / known MDP worker process
    """
    def __init__(self, worker_id, service, expire_at, next_heartbeat):
        # type: (bytes, bytes, float, float) -> None
        self.id = worker_id
        self.service = service
        self.expire_at = expire_at
        self.next_heartbeat = next_heartbeat
        self.is_busy = False

    def is_expired(self, now=None):
        # type: (Optional[float]) -> bool
        """Check if worker is expired
        """
        if now is None:
            now = time.time()
        return now >= self.expire_at

    def is_heartbeat(self, now=None):
        # type: (Optional[float]) -> bool
        """Check if worker is due for sending a heartbeat message
        """
        if now is None:
            now = time.time()
        return now >= self.next_heartbeat


class Service:
    """Service objects manage all workers that can handle a specific service, as well as a queue of MDP Client
    requests to be handled by this service
    """
    def __init__(self):
        self._queue = deque()
        self._workers = OrderedDict()

    def queue_request(self, client, request_body):
        # type: (bytes, List[bytes]) -> None
        """Queue a client request
        """
        self._queue.append((client, request_body))

    def add_worker(self, worker):
        # type: (Worker) -> None
        """Add a Worker to the service
        """
        self._workers[worker.id] = worker

    def remove_worker(self, worker_id):
        # type: (bytes) -> None
        """Remove a worker from the service
        """
        del self._workers[worker_id]

    def dequeue_pending(self):
        # type: () -> Generator[Tuple[List[bytes], Worker, bytes], None, None]
        """Dequeue pending workers and requests to be handled by them
        """
        while len(self._queue) and len(self._workers):
            client, message = self._queue.popleft()
            _, worker = self._workers.popitem(last=False)
            yield message, worker, client

    @property
    def queued_requests(self):
        # type: () -> int
        """Number of queued requests for this service
        """
        return len(self._queue)

    @property
    def available_workers(self):
        # type: () -> int
        """Number of available workers for this service
        """
        return len(self._workers)


class ServicesContainer:
    """A container for all services managed by the broker
    """
    def __init__(self, busy_workers_timeout=protocol.DEFAULT_BUSY_WORKER_TIMEOUT):
        # type: (float) -> None
        self._busy_workers_timeout = busy_workers_timeout
        self._services = defaultdict(Service)  # type: DefaultDict[bytes, Service]
        self._workers = OrderedDict()  # type: OrderedDict[bytes, Worker]
        self._busy_workers = dict()  # type: Dict[bytes, Worker]

    def queue_client_request(self, client, service, body):
        # type: (bytes, bytes, List[bytes]) -> None
        """Queue a request from a client
        """
        self._services[service].queue_request(client, body)

    def dequeue_pending(self):
        # type: () -> Generator[Tuple[List[bytes], Worker, bytes], None, None]
        """Pop ready message-worker pairs from all service queues so they can be dispatched
        """
        for service in self._services.values():
            for message, worker, client in service.dequeue_pending():
                yield message, worker, client

    def get_worker(self, worker_id):
        # type: (bytes) -> Optional[Worker]
        """Get a worker by ID if exists
        """
        return self._workers.get(worker_id, None)

    def add_worker(self, worker_id, service, expire_at, next_heartbeat):
        # type: (bytes, bytes, float, float) -> Worker
        """Add a worker to the list of available workers
        """
        if worker_id in self._workers:
            raise error.StateError("Worker '{}' has already sent READY message".format(worker_id))
        worker = Worker(worker_id, service, expire_at, next_heartbeat)
        self._workers[worker_id] = worker
        self._services[service].add_worker(worker)
        return worker

    def set_worker_busy(self, worker_id, expire_at):
        # type: (bytes, float) -> Worker
        """Mark a worker as busy - that is currently processing a request and not available for more work
        """
        if worker_id not in self._workers:
            raise error.StateError("Worker id {} is not in the list of available workers".format(id_to_int(worker_id)))
        worker = self._workers.pop(worker_id)
        worker.is_busy = True
        worker.expire_at = expire_at
        self._busy_workers[worker.id] = worker
        return worker

    def set_worker_available(self, worker_id, expire_at, next_heartbeat):
        # type: (bytes, float, float) -> Worker
        """Mark a worker that was busy processing a request as back and available again
        """
        if worker_id not in self._busy_workers:
            raise error.StateError("Worker id {} is not previously known or has expired".format(id_to_int(worker_id)))
        worker = self._busy_workers.pop(worker_id)
        worker.is_busy = False
        worker.expire_at = expire_at
        worker.next_heartbeat = next_heartbeat
        self._workers[worker_id] = worker
        self._services[worker.service].add_worker(worker)
        return worker

    def is_busy_worker(self, worker_id):
        # type: (bytes) -> bool
        """Return True if the given worker_id is of a known busy worker
        """
        return worker_id in self._busy_workers

    def remove_worker(self, worker_id):
        # type: (bytes) -> None
        """Remove a worker from the list of known workers
        """
        try:
            worker = self._workers[worker_id]
            self._services[worker.service].remove_worker(worker_id)
            del self._workers[worker_id]
        except KeyError:
            try:
                del self._busy_workers[worker_id]
            except KeyError:
                raise error.StateError("Worker id {} is not known".format(worker_id))

    def heartbeat_workers(self):
        # type: () -> Generator[Worker, None, None]
        """Get iterator of workers waiting for a heartbeat
        """
        now = time.time()
        return (w for w in self._workers.values() if w.is_heartbeat(now))

    def expired_workers(self):
        # type: () -> Generator[Worker, None, None]
        """Get iterator of workers that have expired (no heartbeat received in too long) and busy workers that
        have not returned in too long
        """
        now = time.time()
        return (w for w in chain(self._workers.values(), self._busy_workers.values()) if w.is_expired(now))


def id_to_int(id_):
    # type: (Union[bytes, str]) -> int
    """Convert a ZMQ client ID to printable integer

    This is needed to log client IDs while maintaining Python cross-version compatibility (so we can't use bytes.hex()
    for example)
    """
    i = 0
    for c in id_:
        if not isinstance(c, int):
            c = ord(c)
        i = (i << 8) + c
    return i


def _parse_args():
    """Command line argument parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bind-url', help='Bind URL', default=DEFAULT_BIND_URL)
    parser.add_argument('-i', '--heartbeat-interval', help='Heartbeat interval (sec)', type=float,
                        default=protocol.DEFAULT_HEARTBEAT_INTERVAL)
    parser.add_argument('-t', '--heartbeat-timeout', help='Heartbeat timeout (sec)', type=float,
                        default=protocol.DEFAULT_HEARTBEAT_TIMEOUT)
    parser.add_argument('-B', '--busy-worker-timeout', help='Timeout for busy workers (sec)', type=float,
                        default=protocol.DEFAULT_BUSY_WORKER_TIMEOUT)
    parser.add_argument('-c', '--config', help='Configuration file path', type=argparse.FileType(mode='rb'),
                        required=False)
    parser.add_argument('-V', '--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('-D', '--debug', action='store_true', help='Enable debug-level logging')

    args = parser.parse_args()

    if args.heartbeat_timeout % args.heartbeat_interval != 0.0:
        raise parser.error("--heartbeat-timeout ({}) must be a multiple of --heartbeat-interval ({})".format(
            args.heartbeat_timeout, args.heartbeat_interval))

    return args


def _load_config(args):
    # type: (argparse.Namespace) -> figcan.Configuration
    """Load configuration
    """
    cfg = figcan.Configuration(default_config)
    if args.config:
        cfg.apply(yaml.safe_load(args.config))

    level = 'DEBUG' if args.debug else 'INFO' if args.verbose else None
    if level:
        cfg.apply({"logging": {"handlers": {"console": {"level": level}}, "root": {"level": level}}})

    return cfg


def main():
    """Run the broker from command line
    """
    args = _parse_args()

    config = _load_config(args)
    logging.config.dictConfig(config['logging'])

    broker = Broker(bind=args.bind_url,
                    heartbeat_interval=args.heartbeat_interval,
                    heartbeat_timeout=args.heartbeat_timeout,
                    busy_worker_timeout=args.busy_worker_timeout)

    def _sig_handler(sig, _):
        logging.debug("Got signal %d, stopping broker", sig)
        broker.stop()

    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT, _sig_handler)

    broker.run()


if __name__ == '__main__':
    main()
