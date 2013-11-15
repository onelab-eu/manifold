from manifold.core.node           import Node
from manifold.core.pool_consumers import PoolConsumers

class Producer(Node):

    def __init__(self, consumers = None, max_consumers = 1):
        self._pool_consumers = PoolConsumers(consumers, max_consumers = max_consumers)

    def add_consumer(self, consumer):
        self._pool_consumers.add(consumer)

    def add_consumers(self, consumers):
        for consumer in consumers:
            self.add_consumer(consumer)
