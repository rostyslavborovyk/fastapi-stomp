"""
Queue manager, queue implementation, and supporting classes.
"""
import logging
import uuid
from collections import defaultdict

from fastapi_stomp.connection import AsyncStompConnection
from fastapi_stomp.scheduler import (
    RandomQueueScheduler,
    SubscriberPriorityScheduler,
    QueuePriorityScheduler,
    RandomSubscriberScheduler,
)
from fastapi_stomp.store import AsyncQueueStore
from fastapi_stomp.subscription import AsyncSubscriptionManager, AsyncSubscription


from fastapi_stomp.util.frames import Frame


# todo rewrite the queue manager to store queue messages in the durable place
class AsyncQueueManager:
    """
    Class that manages distribution of messages to queue subscribers.
    """

    def __init__(
        self,
        store: AsyncQueueStore,
        subscriber_scheduler: SubscriberPriorityScheduler | None = None,
        queue_scheduler: QueuePriorityScheduler | None = None,
    ):
        self.log = logging.getLogger(
            '%s.%s' % (__name__, self.__class__.__name__))

        # Use default schedulers, if they're not specified
        if subscriber_scheduler is None:
            subscriber_scheduler = RandomSubscriberScheduler()

        if queue_scheduler is None:
            queue_scheduler = RandomQueueScheduler()

        self.store = store
        self.subscriber_scheduler = subscriber_scheduler
        self.queue_scheduler = queue_scheduler

        self._subscriptions = AsyncSubscriptionManager()
        self._transaction_frames: dict[AsyncSubscription, dict[str, list[Frame]]] \
            = defaultdict(lambda: defaultdict(list))
        self._pending: dict[AsyncSubscription, Frame] = {}

    async def close(self):
        """
        Closes all resources/backends associated with this queue manager.
        """
        self.log.info("Shutting down queue manager.")
        if hasattr(self.store, 'close'):
            await self.store.close()

        if hasattr(self.subscriber_scheduler, 'close'):
            self.subscriber_scheduler.close()

        if hasattr(self.queue_scheduler, 'close'):
            self.queue_scheduler.close()

    async def subscriber_count(self, destination: str | None = None) -> int:
        """
        Returns a count of the number of subscribers.

        If destination is specified then it only returns count of subscribers
        for that specific destination.
        """
        return await self._subscriptions.subscriber_count(destination=destination)

    async def subscribe(
        self,
        connection: AsyncStompConnection,
        destination: str,
        id: str,
    ):
        """
        Subscribes a connection to the specified destination (topic or queue).
        """
        self.log.debug("Subscribing %s to %s" % (connection, destination))
        await self._subscriptions.subscribe(connection, destination, id=id)

    async def unsubscribe(
        self,
        connection: AsyncStompConnection,
        destination: str,
        id: str,
    ):
        """
        Unsubscribes a connection from a destination (topic or queue).
        """
        self.log.debug("Unsubscribing %s from %s" % (connection, destination))
        await self._subscriptions.unsubscribe(connection, destination, id=id)

    async def disconnect(self, connection: AsyncStompConnection):
        """
        Removes a subscriber connection, ensuring that any pending commands get requeued.
        """
        self.log.debug("Disconnecting %s" % connection)
        for subscription, pending_frame in list(self._pending.items()):
            if subscription.connection == connection:
                await self.store.requeue(pending_frame.headers.get(
                    'destination'), pending_frame)
                del self._pending[subscription]
        await self._subscriptions.disconnect(connection)

    async def send(self, message: Frame):
        """
        Sends a MESSAGE frame to an eligible subscriber connection.

        Note that this method will modify the incoming message object to
        add a message-id header (if not present) and to change the command
        to 'MESSAGE' (if it is not).
        """
        dest = message.headers.get('destination')
        if not dest:
            raise ValueError(
                "Cannot send frame with no destination: %s" % message)

        message.cmd = 'message'

        message.headers.setdefault('message-id', str(uuid.uuid4()))

        # Grab all subscribers for this destination that do not have pending
        # frames
        subscribers = [s for s in self._subscriptions.subscribers(dest)
                       if s not in self._pending]

        if not subscribers:
            self.log.debug(
                "No eligible subscribers; adding message %s to queue %s" % (message, dest))
            await self.store.enqueue(dest, message)
        else:
            selected = self.subscriber_scheduler.choice(subscribers, message)
            self.log.debug("Delivering message %s to subscriber %s" %
                           (message, selected))
            await self._send_frame(selected, message)

    async def _send_frame(self, subscription: AsyncSubscription, frame: Frame):
        """
        Sends a frame to a specific subscription.

        (This method assumes it is being called from within a lock-guarded public
        method.)
        """
        assert subscription is not None
        assert frame is not None

        if frame.cmd == "message":
            frame.headers["subscription"] = subscription.id

        self.log.debug("Delivering frame %s to subscription %s" %
                       (frame, subscription))

        await subscription.connection.send_frame(frame)
