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

    def send(self, packet):
        """
        A Consumer sends queries to Producers
        """
        if packet.get_type() not in [Packet.TYPE_QUERY]:
            raise ValueError, "Invalid packet type for producer: %s" % Packet.get_type_name(packet.get_type())

        self._pool_producers.receive(packet)
        
    # ???
    def receive(self, packet):
        pass
