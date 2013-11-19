from manifold.core.node           import Node
from manifold.core.packet         import Packet
from manifold.core.pool_producers import PoolProducers

class Consumer(Node):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, producers = None, max_producers = 1, has_parent_producer = False):
        Node.__init__(self)
        self._pool_producers = PoolProducers(producers, max_producers = max_producers)

        for producer in self.get_producers():
            producer.add_consumer(self, cascade = False)

        self._has_parent_producer = has_parent_producer
    

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_producers(self):
        return set(self._pool_producers)

    def get_max_producers(self):
        return self._pool_producers.get_max_producers()

    def clear_producers(self, cascade = True):
        if cascade:
            for producer in self._pool_producers:
                producer.del_consumer(self, cascade = False)
        self._pool_producers.clear()

    def add_producer(self, producer, cascade = True):
        self._pool_producers.add(producer)
        if cascade:
            producer.add_consumer(self, cascade = False)

    def del_producer(self, producer, cascade = True):
        self._pool_producers.remove(producer)
        if cascade:
            producer.del_customer(self, cascade = False)

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def add_producers(self, producers):
        for producer in producers:
            self.add_producer(producer)

    def update_producers(self, function):
        raise Exception, "Not implemented"

    # max_producers == 1

    def get_producer(self):
        if self.get_max_producers() != 1:
            max = self.get_max_producers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call get_producer with max_producers != 1 (=%s)" % max_str

        num = len(self._pool_producers)
        if num == 0:
            return None

        return iter(self.get_producers()).next()

    def set_producer(self, producer):
        self.clear_producers()
        self.add_producer(producer)

    def set_producers(self, consumers):
        self.clear_producers()
        self.add_producers(producers)

    def update_producer(self, function):
        if self.get_max_producers() != 1:
            max = self.get_max_producers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call update_producer with max_producers != 1 (=%s)" % max_str

        self.set_producer(function(self.get_producer()))
        

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def send(self, packet):
        """
        A Consumer sends queries to Producers
        """
        if packet.get_type() not in [Packet.TYPE_QUERY]:
            raise ValueError, "Invalid packet type for producer: %s" % Packet.get_type_name(packet.get_type())

        self._pool_producers.receive(packet)
        
    def receive(self, packet):
        raise Exception, "Not implemented"
