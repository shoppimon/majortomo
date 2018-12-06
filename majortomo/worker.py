"""MDP 0.2 Worker implementation
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

import logging
import signal
import time
from typing import Generator, Iterable, List, Optional, Tuple  # noqa: F401

import zmq

from majortomo import error
from majortomo import protocol as p

DEFAULT_ZMQ_LINGER = 2500


class Worker(object):
    """MDP 0.2 Worker implementation
    """

    def __init__(self, broker_url, service_name, heartbeat_interval=p.DEFAULT_HEARTBEAT_INTERVAL,
                 heartbeat_timeout=p.DEFAULT_HEARTBEAT_TIMEOUT, zmq_context=None, zmq_linger=DEFAULT_ZMQ_LINGER):
        # type: (str, str, float, float, Optional[zmq.Context], int) -> None
        self.broker_url = broker_url
        self.service_name = service_name.encode('ascii')
        self.heartbeat_interval = heartbeat_interval

        self._socket = None  # type: zmq.Socket
        self._poller = None  # type: zmq.Poller
        self._zmq_context = zmq_context if zmq_context else zmq.Context.instance()
        self._linger = zmq_linger
        self._log = logging.getLogger(__name__)
        self._heartbeat_timeout = heartbeat_timeout
        self._last_broker_hb = 0.0
        self._last_sent_message = 0.0

    def connect(self, reconnect=False):
        # type: (bool) -> None
        if self.is_connected():
            if not reconnect:
                return
            self._disconnect()

        # Set up socket
        self._socket = self._zmq_context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, self._linger)
        self._socket.connect(self.broker_url)
        self._log.debug("Connected to broker on ZMQ DEALER socket at %s", self.broker_url)

        self._poller = zmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)

        self._send_ready()
        self._last_broker_hb = time.time()

    def wait_for_request(self):
        # type: () -> Tuple[bytes, List[bytes]]
        """Wait for a REQUEST command from the broker and return the client address and message body frames.

        Will internally handle timeouts, heartbeats and check for protocol errors and disconnect commands.
        """
        command, frames = self._receive()

        if command == p.DISCONNECT:
            self._log.debug("Got DISCONNECT from broker; Disconnecting")
            self._disconnect()
            raise error.Disconnected("Disconnected on message from broker")

        elif command != p.REQUEST:
            raise error.ProtocolError("Unexpected message type from broker: {}".format(command))

        if len(frames) < 3:
            raise error.ProtocolError("Unexpected REQUEST message size, got {} frames, expecting at least 3".format(
                len(frames)))

        client_addr = frames[0]
        request = frames[2:]
        return client_addr, request

    def send_reply_final(self, client, frames):
        # type: (bytes, List[bytes]) -> None
        """Send final reply to client

        FINAL reply means the client will not expect any additional parts to the reply. This should be used
        when the entire reply is ready to be delivered.
        """
        self._send_to_client(client, p.FINAL, *frames)

    def send_reply_partial(self, client, frames):
        # type: (bytes, List[bytes]) -> None
        """Send the given set of frames as a partial reply to client

        PARTIAL reply means the client will expect zero or more additional PARTIAL reply messages following
        this one, with exactly one terminating FINAL reply following. This should be used if parts of the
        reply are ready to be sent, and the client is capable of processing them while the worker is still
        at work on the rest of the reply.
        """
        self._send_to_client(client, p.PARTIAL, *frames)

    def send_reply_from_iterable(self, client, frames_iter, final=None):
        # type: (bytes, Iterable[List[bytes]], List[bytes]) -> None
        """Send multiple partial replies from an iterator as PARTIAL replies to client.

        If `final` is provided, it will be sent as the FINAL reply after all PARTIAL replies are sent.
        """
        for part in frames_iter:
            self.send_reply_partial(client, part)
        if final:
            self.send_reply_final(client, final)

    def close(self):
        if not self.is_connected():
            return
        self._send_disconnect()
        self._disconnect()

    def is_connected(self):
        return self._socket is not None

    def _disconnect(self):
        if not self.is_connected():
            return
        self._socket.disconnect(self.broker_url)
        self._socket = None
        self._last_sent_message -= self.heartbeat_interval

    def _receive(self):
        # type: () -> Tuple[bytes, List[bytes]]
        """Poll on the socket until a command is received

        Will handle timeouts and heartbeats internally without returning
        """
        while True:
            if self._socket is None:
                raise error.Disconnected("Worker is disconnected")

            self._check_send_heartbeat()
            poll_timeout = self._get_poll_timeout()
            socks = dict(self._poller.poll(timeout=poll_timeout))
            if socks.get(self._socket) == zmq.POLLIN:
                message = self._socket.recv_multipart()
                self._log.debug("Got message of %d frames", len(message))
            else:
                self._log.debug("Receive timed out after %d ms", poll_timeout)
                if (time.time() - self._last_broker_hb) > self._heartbeat_timeout:
                    # We're not connected anymore?
                    self._log.info("Got no heartbeat in %d sec, disconnecting and reconnecting socket",
                                   self._heartbeat_timeout)
                    self.connect(reconnect=True)
                continue

            command, frames = self._verify_message(message)
            self._last_broker_hb = time.time()

            if command == p.HEARTBEAT:
                self._log.debug("Got heartbeat message from broker")
                continue

            return command, frames

    def _send_ready(self):
        self._send(p.READY, self.service_name)

    def _send_disconnect(self):
        self._send(p.DISCONNECT)

    def _check_send_heartbeat(self):
        if time.time() - self._last_sent_message >= self.heartbeat_interval:
            self._log.debug("Sending HEARTBEAT to broker")
            self._send(p.HEARTBEAT)

    def _send_to_client(self, client, message_type, *frames):
        self._send(message_type, client, b'', *frames)

    def _send(self, message_type, *args):
        # type: (bytes, *bytes) -> None
        self._socket.send_multipart((b'', p.WORKER_HEADER, message_type) + args)
        self._last_sent_message = time.time()

    def _get_poll_timeout(self):
        # type: () -> int
        """Return the poll timeout for the current iteration in milliseconds
        """
        return max(0, int((time.time() - self._last_sent_message + self.heartbeat_interval) * 1000))

    @staticmethod
    def _verify_message(message):
        # type: (List[bytes]) -> Tuple[bytes, List[bytes]]
        if len(message) < 3:
            raise error.ProtocolError("Unexpected message length, expecting at least 3 frames, got {}".format(
                len(message)))

        if message.pop(0) != b'':
            raise error.ProtocolError("Expecting first message frame to be empty")

        if message[0] != p.WORKER_HEADER:
            print(message)
            raise error.ProtocolError("Unexpected protocol header [{}], expecting [{}]".format(
                message[0], p.WORKER_HEADER))

        if message[1] not in {p.DISCONNECT, p.HEARTBEAT, p.REQUEST}:
            raise error.ProtocolError("Unexpected message type [{}], expecting either HEARTBEAT, REQUEST or "
                                      "DISCONNECT".format(message[1]))

        return message[1], message[2:]

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class WorkerRequestsIterator(object):
    """An iterator that allows simple, high-level API over workers
    """

    def __init__(self, worker):
        # type: (Worker) -> None
        self.worker = worker
        self._last_client = None  # type: Optional[bytes]
        self._log = logging.getLogger(__name__)
        self._stop = False

    def __iter__(self):
        # type: () -> Generator[List[bytes], None, None]
        with self.worker:
            while not self._stop:
                try:
                    self._last_client, request = self.worker.wait_for_request()
                    yield request

                except error.ProtocolError as e:
                    self._log.warning("Protocol error: %s, dropping request", str(e))
                    continue

                except error.Disconnected:
                    self._log.info("Worker disconnected")
                    break

    def stop(self):
        self.worker.close()
        self._stop = True

    def send_reply_final(self, *args, **kwargs):
        self._send_reply(self.worker.send_reply_final, *args, **kwargs)
        self._last_client = None

    def send_reply_partial(self, *args, **kwargs):
        self._send_reply(self.worker.send_reply_partial, *args, **kwargs)

    def send_reply_from_iterable(self, frames_iter, final=None):
        self._send_reply(self.worker.send_reply_from_iterable, frames_iter, final)
        if final:
            self._last_client = None

    def stop_on_signal(self, sig_list=(signal.SIGINT, signal.SIGTERM)):
        # type: (Tuple[signal.Signals, ...]) -> None
        """Hook to POSIX OS level signals to stop iteration
        """
        def _handler(sig_num, _):
            self._log.info("%s Caught signal %d, stopping loop", self.__class__.__name__, sig_num)
            self.stop()

        for sig_num in sig_list:
            signal.signal(sig_num, _handler)

    def _send_reply(self, method, *args, **kwargs):
        if self._last_client is None:
            raise error.StateError("Cannot send reply: no client waiting")
        method(self._last_client, *args, **kwargs)
