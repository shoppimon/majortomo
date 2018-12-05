"""ZeroMQ MDP Client / Worker Errors
"""


class Error(RuntimeError):
    """Parent exception for all zmq_mdp errors
    """
    pass


class ProtocolError(Error):
    """MDP 0.2 Protocol Mismatch
    """
    pass


class Disconnected(Error):
    """We are no longer connected
    """
    pass


class StateError(Error):
    """System is in an unexpected state
    """
    pass


class Timeout(Error):
    """Operation timed out
    """
    pass
