"""
Storage containers for durable queues and (planned) durable topics.
"""
import abc
import logging
import typing as t

from fastapi_stomp.util.frames import Frame


class TopicStore:
    """
    Abstract base class for non-durable topic storage.
    """


class DurableTopicStore(TopicStore):
    """
    Abstract base class for durable topic storage, to be implemented
    """


class AsyncQueueStore(metaclass=abc.ABCMeta):
    """
    Abstract base class for asynchronous queue storage.
    """

    def __init__(self):
        self.log = logging.getLogger('%s.%s' % (
            self.__module__, self.__class__.__name__))

    @abc.abstractmethod
    async def enqueue(self, destination: str, frame: Frame):
        """
        Store message (frame) for specified destinationination.
        """

    @abc.abstractmethod
    async def dequeue(self, destination: str) -> Frame | None:
        """
        Removes and returns an item from the queue (or C{None} if no items in queue).
        """

    async def requeue(self, destination: str, frame: Frame):
        """
        Requeue a message (frame) for storing at specified destinationination.
        """
        await self.enqueue(destination, frame)

    async def size(self, destination: str) -> int:
        """
        Size of the queue for specified destination.
        """
        raise NotImplementedError

    async def has_frames(self, destination: str) -> bool:
        """
        Whether specified destination has any frames.

        Default implementation uses L{QueueStore.size} to determine if there
        are any frames in queue.  Subclasses may choose to optimize this.
        """
        return await self.size(destination) > 0

    async def destinations(self) -> set[str]:
        """
        Provides a set of destinations (queue "addresses") available.
        """
        raise NotImplementedError

    async def close(self):
        """
        May be implemented to perform any necessary cleanup operations when store is closed.
        """

    def frames(self, destination: str) -> t.AsyncIterator[Frame]:
        """
        Returns an async iterator for frames in specified queue.

        The iterator simply wraps calls to L{dequeue} method, so the order of the
        frames from the iterator will be the reverse of the order in which the
        frames were enqueued.
        """
        return AsyncQueueFrameIterator(self, destination)


class AsyncQueueFrameIterator(t.AsyncIterator[Frame]):
    """
    Provides an AsyncIterable over the frames for a specified destination in a queue.
    """

    def __init__(self, store: AsyncQueueStore, destination: str):
        self.store = store
        self.destination = destination

    def __aiter__(self):
        return self

    async def __anext__(self) -> Frame:
        frame = await self.store.dequeue(self.destination)
        if not frame:
            raise StopAsyncIteration()
        return frame

    async def len(self) -> int:
        return await self.store.size(self.destination)
