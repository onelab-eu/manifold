from manifold.core.packet import Packet

class PoolConsumers(set):
    """
    A pool of consumers.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, consumers=None, max_consumers=None):
        """
        Constructor
        """
        if not consumers:
            consumers = set()
        super(PoolConsumers, self).__init__(set(consumers))

        self._max_consumers = max_consumers


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def add(self, consumer):
        if len(self._consumers) >= self._max_consumers:
            raise Exception, "Cannot add consumer: maximum (%d) reached." % self._max_consumers
        super(PoolConsumers, self).add(consumer)

    def send(self, packet):
        assert packet.get_type() in [Packet.TYPE_RECORD, Packet.TYPE_ERROR], "Invalid packet type for consumer: %s" % Packet.get_type_name(packet.get_type())

        for consumer in self:
            consumer.receive(packet)
