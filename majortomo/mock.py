"""Mock MDP client for testing
"""
import logging
import time

import majortomo.error as e
from majortomo import Client


class MockMDPClient(Client):

    def __init__(self, broker_url):
        super(MockMDPClient, self).__init__(broker_url)
        self._log = logging.getLogger(__name__)
        self._reply = None
        self.raise_timeout = False

    def connect(self, reconnect=False):
        self._log.debug('Connected to MockMDPClient')

    def close(self):
        self._log.debug('Disconnected from MockMDPClient')

    def send(self, service, *args):
        self._log.debug('Service %s sent a new "send" message', service)

    def recv_part(self, timeout=None):
        if self.raise_timeout:
            time.sleep(timeout)
            raise e.Timeout("Timed out waiting for reply from broker")

        return self._reply

    def recv_all(self, timeout=None):
        return self.recv_part(timeout)

    def recv_all_as_list(self, timeout=None):
        return self.recv_part(timeout)

    def set_reply(self, *args):
        self._reply = args
