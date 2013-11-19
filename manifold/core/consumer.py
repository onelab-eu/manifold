from manifold.core.node           import Node
from manifold.core.packet         import Packet
from manifold.core.pool_producers import PoolProducers

class Consumer(Node):

    def __init__(self, producers = None, max_producers = 1, has_parent_producer = False):
        Node.__init__(self)
        self._pool_producers = PoolProducers(producers, max_producers = max_producers)
        self._has_parent_producer = has_parent_producer

    def add_producer(self, producer):
        self._pool_producers.add(producer)

    def add_producers(self, producers):
        for producer in producers:
            self.add_producer(producer)

    def get_producers(self):
        return set(self._pool_producers)

    def get_producer(self):
        # XXX only if max_producer == 1

        num = len(self._pool_producers)
        if num == 0:
            return None
        elif num == 1:
            return iter(self._pool_producers).next()
        else:
            raise Exception, "More than 1 producer"

    def send(self, packet):
        """
        A Consumer sends queries to Producers
        """
        if packet.get_type() not in [Packet.TYPE_QUERY]:
            raise ValueError, "Invalid packet type for producer: %s" % Packet.get_type_name(packet.get_type())

        self._pool_producers.receive(packet)
        
    # ???
    def receive(self, packet):
        print "Consumer::receive", packet
        pass
