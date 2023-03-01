import abc

from fastapi_stomp.util.frames import Frame


class AsyncStompConnection(abc.ABC):
    """
    An "interface" for server implementation classes to "implement".

    This class serves primarily as a means to document the API that CoilMQ will expect
    the connection object to implement.
    """

    @abc.abstractmethod
    async def receive_frame(self) -> Frame:
        """
        Uses this connection implementation to asynchronously receive a frame from the connected client.
        """

    @abc.abstractmethod
    async def send_frame(self, frame: Frame):
        """
        Uses this connection implementation to asynchronously send the specified frame to a connected client.
        """
