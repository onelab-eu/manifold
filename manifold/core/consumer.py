from manifold.core.node           import Node
from manifold.core.pool_producers import PoolProducers

class Consumer(Node):

    def __init__(self, producers = None, max_producers = 1):
        self._pool_producers = PoolProducers(producers, max_producers = max_producers)

    def add_producer(self, producer):
        self._pool_producers.add(producer)

    def add_producers(self, producers):
        for producer in producers:
            self.add_producer(producer)
