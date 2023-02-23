try:
    import aioredis
except ImportError:  # pragma: no cover
    import sys; sys.exit('please, install aioredis package to use asynchronous redis-store')
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle

from coilmq.store import QueueStore
from coilmq.util.concurrency import synchronized
from coilmq.config import config


lock = threading.RLock()


def make_redis_store(cfg=None):
    return AsyncRedisQueueStore(
        redis_conn=aioredis.Redis(**dict((cfg or config).items('redis'))))


class AsyncRedisQueueStore(QueueStore):
    """Simple Queue with asynchronous Redis Backend"""
    def __init__(self, redis_conn=None):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__db: aioredis.Redis = redis_conn or aioredis.Redis()
        # self.key = '{0}:{1}'.format(namespace, name)
        super(AsyncRedisQueueStore, self).__init__()

    @synchronized(lock)
    async def enqueue(self, destination, frame):
        await self.__db.rpush(destination, pickle.dumps(frame))

    @synchronized(lock)
    async def dequeue(self, destination):
        item = await self.__db.lpop(destination)
        if item:
            return pickle.loads(item)

    @synchronized(lock)
    async def requeue(self, destination, frame):
        await self.enqueue(destination, frame)

    @synchronized(lock)
    async def size(self, destination):
        return await self.__db.llen(destination)

    @synchronized(lock)
    async def has_frames(self, destination):
        return await self.size(destination) > 0

    @synchronized(lock)
    async def destinations(self):
        return await self.__db.keys()
