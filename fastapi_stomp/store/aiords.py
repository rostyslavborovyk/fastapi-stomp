try:
    import aioredis
except ImportError:  # pragma: no cover
    import sys; sys.exit('please, install aioredis package to use asynchronous redis-store')

import pickle

from fastapi_stomp.store import AsyncQueueStore


class AsyncRedisQueueStore(AsyncQueueStore):
    """
    Simple Queue with asynchronous Redis Backend
    """
    def __init__(self, redis_conn=None):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__db: aioredis.Redis = redis_conn or aioredis.Redis()
        # self.key = '{0}:{1}'.format(namespace, name)
        super(AsyncRedisQueueStore, self).__init__()

    async def enqueue(self, destination, frame):
        await self.__db.rpush(destination, pickle.dumps(frame))

    async def dequeue(self, destination):
        item = await self.__db.lpop(destination)
        if item:
            return pickle.loads(item)

    async def requeue(self, destination, frame):
        await self.enqueue(destination, frame)

    async def size(self, destination) -> int:
        return await self.__db.llen(destination)

    async def has_frames(self, destination: str) -> bool:
        size = await self.size(destination)
        return size > 0

    async def destinations(self):
        return await self.__db.keys()
