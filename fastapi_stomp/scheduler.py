"""
Classes that provide delivery scheduler implementations.

The default implementation used by the system for determining which subscriber
(connection) should receive a message is simply a random choice but favoring
reliable subscribers.  Developers can write their own delivery schedulers, which 
should implement the methods defined in QueuePriorityScheduler if they would
like to customize the behavior.
"""
import abc
import random


class SubscriberPriorityScheduler(metaclass=abc.ABCMeta):
    """ Abstract base class for choosing which recipient (subscriber) should receive a message. """

    @abc.abstractmethod
    def choice(self, subscribers, message):
        """
        Chooses which subscriber (from list) should recieve specified message.
        """


class QueuePriorityScheduler:
    """
    Abstract base class for objects that provide a way to prioritize the queues.
    """

    def choice(self, queues, connection):
        """
        Choose which queue to select for messages to specified connection.
        """
        raise NotImplementedError


class RandomSubscriberScheduler(SubscriberPriorityScheduler):
    """ A delivery scheduler that chooses a random subscriber for message recipient. """

    def choice(self, subscribers, message):
        """
        Chooses a random connection from subscribers to deliver specified message.
        """
        if not subscribers:
            return None
        return random.choice(subscribers)


class RandomQueueScheduler(QueuePriorityScheduler):
    """
    Implementation of L{QueuePriorityScheduler} that selects a random queue from the list.
    """

    def choice(self, queues, connection):
        """
        Chooses a random queue for messages to specified connection.
        """
        if not queues:
            return None
        return random.choice(list(queues.keys()))
