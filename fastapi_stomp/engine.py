from fastapi_stomp.auth import AsyncAuthenticator
from fastapi_stomp.connection import AsyncStompConnection
from fastapi_stomp.queue import AsyncQueueManager
from fastapi_stomp.topic import AsyncTopicManager

import logging


class AsyncStompEngine:
    """
    The engine provides the core business logic that we use to respond to STOMP protocol
    messages.

    This class is transport-agnostic; it exposes methods that expect STOMP frames and
    uses the attached connection to send frames to connected clients.
    """

    def __init__(
        self,
        connection: AsyncStompConnection,
        authenticator: AsyncAuthenticator,
        queue_manager: AsyncQueueManager,
        topic_manager: AsyncTopicManager,
    ):
        self.log = logging.getLogger('%s.%s' % (
            self.__class__.__module__, self.__class__.__name__))
        self.connection = connection
        self.authenticator = authenticator
        self.queue_manager = queue_manager
        self.topic_manager = topic_manager
        self.connected = False

    def unbind(self):
        """
        Unbinds this connection from queue and topic managers (freeing up resources)
        and resets state.
        """
        self.connected = False
        self.queue_manager.disconnect(self.connection)
        self.topic_manager.disconnect(self.connection)
