"""
Non-durable (yet) topic support functionality.
"""
import logging
import uuid
from copy import deepcopy

from fastapi_stomp.connection import AsyncStompConnection
from fastapi_stomp.subscription import AsyncSubscriptionManager, AsyncSubscription


from fastapi_stomp.util.frames import Frame


class AsyncTopicManager:
    """
    Class that manages distribution of messages to topic subscribers.
    """

    def __init__(self, subscriptions_manager: AsyncSubscriptionManager):
        self.log = logging.getLogger(
            '%s.%s' % (__name__, self.__class__.__name__))

        self.subscriptions_manager = subscriptions_manager

    async def close(self):
        """
        Closes all resources associated with this topic manager.
        """
        self.log.info("Shutting down topic manager.")

    async def subscribe(
        self,
        connection: AsyncStompConnection,
        destination: str,
        id: str,
    ) -> AsyncSubscription:
        """
        Subscribes a connection to the specified topic destination.
        """
        self.log.debug("Subscribing %s to %s" % (connection, destination))
        return await self.subscriptions_manager.subscribe(connection, destination, id=id)

    async def unsubscribe(
        self,
        connection: AsyncStompConnection,
        destination: str,
        id: str,
    ) -> AsyncSubscription | None:
        """
        Unsubscribes a connection from the specified topic destination.
        """
        self.log.debug("Unsubscribing %s from %s" % (connection, destination))
        return await self.subscriptions_manager.unsubscribe(connection, destination, id=id)

    async def disconnect(self, connection: AsyncStompConnection):
        """
        Removes a subscriber connection.
        """
        self.log.debug("Disconnecting %s" % connection)
        await self.subscriptions_manager.disconnect(connection)

    async def send(self, message: Frame):
        """
        Sends a message to all subscribers of destination.
        """
        dest = message.headers.get('destination')
        if not dest:
            raise ValueError(
                "Cannot send frame with no destination: %s" % message)

        message.cmd = 'message'

        message.headers.setdefault('message-id', str(uuid.uuid4()))

        bad_subscribers = set()
        for subscriber in self.subscriptions_manager.subscribers(dest):
            frame = deepcopy(message)
            frame.headers["subscription"] = subscriber.id
            try:
                await subscriber.connection.send_frame(frame)
            except Exception as e:
                self.log.exception(
                    "Error delivering message to subscriber %s; client will be disconnected." % subscriber)
                # We queue for deletion so we are not modifying the topics dict
                # while iterating over it.
                bad_subscribers.add(subscriber)

        for s in bad_subscribers:
            await self.unsubscribe(s.connection, dest, id=s.id)
