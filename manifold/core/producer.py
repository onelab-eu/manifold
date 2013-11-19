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


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_consumers(self):
        return set(self._pool_consumers)

    def get_consumer(self):
        # XXX only if max_consumer == 1

        num = len(self._pool_consumers)
        if num == 0:
            return None
        elif num == 1:
            return iter(self._pool_consumers).next()
        else:
            raise Exception, "More than 1 consumer"

    def clear_consumers(self):
        self._pool_consumers.clear()

    def add_consumer(self, consumer):
        self._pool_consumers.add(consumer)

    # Helpers

    def add_consumers(self, consumers):
        for consumer in consumers:
            self.add_consumer(consumer)

    def set_consumer(self, consumer):
        self.clear_consumers()
        self.add_consumer(consumer)

    def set_consumers(self, consumers):
        self.clear_consumers()
        self.add_consumers(consumers)


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
