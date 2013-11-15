from manifold.core.packet import Packet

class PoolProducers(set):
    """
    A pool of producers.
    A producer receives Queries
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------
    
    def __init__(self, producers=None, max_producers=None):
        """
        Constructor.
        """
        if not producers:
            producers = set()
        if not isinstance(producers, (list, set)):
            producers = [producers]
        super(PoolProducers, self).__init__(set(producers))

        self._max_producers = max_producers


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def add(self, producer):
        if len(self._producers) >= self._max_producers:
            raise Exception, "Cannot add producer: maximum (%d) reached." % self._max_producers
        super(PoolProducers).add(producer)

    def receive(self, packet):
        if packet.get_type() not in [Packet.TYPE_QUERY]:
            raise ValueError, "Invalid packet type for producer: %s" % Packet.get_type_name(packet.get_type())

        for producer in self:
            producer.receive(packet)
