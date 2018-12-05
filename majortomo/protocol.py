"""ZeroMQ MDP 0.2 Protocol Constants common for Worker and Client
"""
from typing import List, Optional  # noqa: F401

from . import error

WORKER_HEADER = b'MDPW02'
CLIENT_HEADER = b'MDPC02'

READY = b'\001'
REQUEST = b'\002'
PARTIAL = b'\003'
FINAL = b'\004'
HEARTBEAT = b'\005'
DISCONNECT = b'\006'

DEFAULT_HEARTBEAT_INTERVAL = 2.500
DEFAULT_HEARTBEAT_TIMEOUT = 10.000
DEFAULT_BUSY_WORKER_TIMEOUT = 900.000


class Message(object):
    """Majordomo message container
    """
    ALLOWED_HEADERS = {WORKER_HEADER, CLIENT_HEADER}
    ALLOWED_COMMANDS = {WORKER_HEADER: {READY, PARTIAL, FINAL, HEARTBEAT, DISCONNECT},
                        CLIENT_HEADER: {REQUEST}}

    def __init__(self, client, header, command, message=None):
        # type: (bytes, bytes, bytes, Optional[List[bytes]]) -> None
        if header not in self.ALLOWED_HEADERS:
            raise error.ProtocolError("Unexpected protocol header: {}".format(header))

        if command not in self.ALLOWED_COMMANDS[header]:
            raise error.ProtocolError("Unexpected command: {}".format(command))

        if message is None:
            message = []

        self.client = client
        self.header = header
        self.command = command
        self.message = message
