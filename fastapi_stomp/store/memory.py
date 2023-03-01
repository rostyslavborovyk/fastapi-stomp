from collections import defaultdict, deque

from fastapi_stomp.store import AsyncQueueStore
from fastapi_stomp.util.frames import Frame


class AsyncMemoryQueue(AsyncQueueStore):
    """
    A QueueStore implementation that stores messages in memory, may be used for testing purposes
    """

    def __init__(self):
        AsyncQueueStore.__init__(self)
        self._messages = defaultdict(deque)

    async def enqueue(self, destination: str, frame: Frame):
        self._messages[destination].appendleft(frame)

    async def dequeue(self, destination):
        try:
            return self._messages[destination].pop()
        except IndexError:
            return None

    async def size(self, destination):
        """
        Size of the queue for specified destination.
        """
        return len(self._messages[destination])

    async def has_frames(self, destination):
        """ Whether this queue has frames for the specified destination. """
        return bool(self._messages[destination])

    async def destinations(self):
        """
        Provides a list of destinations (queue "addresses") available.
        """
        return set(self._messages.keys())
