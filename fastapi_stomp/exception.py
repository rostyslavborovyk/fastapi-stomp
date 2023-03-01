class StompError(Exception):
    """
    Base class for stomp exceptions
    """


class FrameError(StompError):
    """
    Base class for errors related to frames
    """


class ProtocolError(StompError):
    """
    Represents an error at the STOMP protocol layer.
    """


class ConfigError(StompError):
    """
    Represents an error in the configuration of the application.
    """


class AuthError(StompError):
    """
    Represents an authentication or authorization error.
    """


class ClientDisconnected(StompError):
    """
    A signal that client has disconnected (so we shouldn't try to keep reading from the client).
    """


class IncompleteFrame(FrameError):
    """The frame has incomplete body"""


class BodyNotTerminated(FrameError):
    """The frame's body is not terminated with the NULL character"""


class EmptyBuffer(FrameError):
    """The buffer is empty"""


class NoContentLength(FrameError):
    """No content length was specified in the frame"""
