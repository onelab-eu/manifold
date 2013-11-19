from manifold.core.node           import Node
from manifold.core.packet         import Packet
from manifold.core.pool_consumers import PoolConsumers
from manifold.util.log            import Log

class Producer(Node):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, consumers = None, max_consumers = None):
        Node.__init__(self)
        self._pool_consumers = PoolConsumers(consumers, max_consumers = max_consumers)

        for consumer in self.get_consumers():
            consumer.add_producer(self, cascade = False)


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_consumers(self):
        return set(self._pool_consumers)

    def get_max_consumers(self):
        return self._pool_consumers.get_max_consumers()

    def clear_consumers(self, cascade = True):
        if cascade:
            for consumer in self._pool_consumers:
                consumer.del_producer(self, cascade = False)
        self._pool_consumers.clear()

    def add_consumer(self, consumer, cascade = True):
        self._pool_consumers.add(consumer)
        if cascade:
            consumer.add_producer(self, cascade = False)

    def del_consumer(self, consumer, cascade = True):
        self._pool_consumers.remove(consumer)
        if cascade:
            customer.del_producer(self, cascade = False)

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def add_consumers(self, consumers):
        for consumer in consumers:
            self.add_consumer(consumer)

    def update_consumers(self, function):
        raise Exception, "Not implemented"

    # max_consumers == 1

    def get_consumer(self):
        if self.get_max_consumers() == 1:
            max = self.get_max_consumers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call get_consumer with max_consumers != 1 (=%s)" % max_str

        num = len(self._pool_consumers)
        if num == 0:
            return None

        return iter(self.get_consumers()).next()

    def set_consumer(self, consumer):
        self.clear_consumers()
        self.add_consumer(consumer)

    def set_consumers(self, consumers):
        self.clear_consumers()
        self.add_consumers(consumers)

    def update_consumer(self, function):
        if self.get_max_consumers() != 1:
            max = self.get_max_consumers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call update_consumer with max_consumers != 1 (=%s)" % max_str

        self.set_consumer(function(self.get_consumer()))


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def send(self, packet):
        # A producer sends results/error to its consumers
        if packet.get_type() not in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            raise ValueError, "Invalid packet type for consumer: %s" % \
                    Packet.get_type_name(packet.get_type())

        #if self.get_identifier():
        #    Log.record("[#%04d] [ %r ]" % (identifier, record))
        #else:
        Log.record("[ %r ]" % packet)

        self._pool_consumers.receive(packet)
        
    def receive(self, packet):
        # A Producer only receives QueryPackets
        if packet.get_type() not in [Packet.TYPE_QUERY]:
            raise ValueError, "Invalid packet type received in producer: %s" % \
                    Packet.get_type_name(packet.get_type()) 
