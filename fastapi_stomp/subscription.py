import itertools
from dataclasses import dataclass
from collections import defaultdict
import typing as t

from fastapi_stomp.connection import AsyncStompConnection

DEFAULT_SUBSCRIPTION_ID = "0"


@dataclass(frozen=True)
class AsyncSubscription:
    connection: AsyncStompConnection
    # todo consider changing id to part of uuid or some other format
    id: str

    @classmethod
    def factory(
        cls,
        connection: AsyncStompConnection,
        id: str,
    ) -> 'AsyncSubscription':
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        return cls(connection=connection, id=id)


class AsyncSubscriptionManager:
    def __init__(self):
        # todo store subscriptions in redis
        self._subscriptions: dict[str, set[AsyncSubscription]] = defaultdict(set)

    async def subscribe(
        self,
        connection: AsyncStompConnection,
        destination: str,
        id: str,
    ) -> AsyncSubscription:
        """
        Subscribes a connection to the specified destination.
        """
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        subscription = AsyncSubscription(connection=connection, id=id)
        self._subscriptions[destination].add(subscription)
        return subscription

    async def unsubscribe(
        self,
        connection: AsyncStompConnection,
        destination: str,
        id: str,
    ) -> AsyncSubscription | None:
        """
        Unsubscribes a connection from a destination.
        """
        subscriptions = self._subscriptions.get(destination, None)
        if subscriptions is None:
            return None
        if id is None:
            id = DEFAULT_SUBSCRIPTION_ID
        subscription = AsyncSubscription(connection=connection, id=id)
        try:
            subscriptions.remove(subscription)
        except KeyError:
            pass
        if not subscriptions:
            del self._subscriptions[destination]
        return subscription

    async def disconnect(self, connection: AsyncStompConnection):
        """
        Removes a client connection.
        """
        for destination, subscriptions in list(self._subscriptions.items()):
            subscriptions = {
                subscription
                for subscription in subscriptions
                if subscription.connection != connection
            }
            if subscriptions:
                self._subscriptions[destination] = subscriptions
            else:
                del self._subscriptions[destination]

    async def subscriber_count(self, destination: str | None = None) -> int:
        """
        Returns a count of the number of subscribers.

        If destination is specified then it only returns count of subscribers
        for that specific destination.
        """
        if destination:
            return len(self._subscriptions.get(destination, set()))
        else:
            return sum(map(len, self._subscriptions.values()))

    def subscribers(self, destination: str) -> set[AsyncSubscription]:
        """
        Returns subscribers to a single destination.
        """
        return self._subscriptions.get(destination, set())

    def all_destinations_subscribers(self) -> t.Iterator[tuple[str, set[AsyncSubscription]]]:
        """
        Yields all subscribers.
        """
        yield from itertools.chain(self._subscriptions.items())
