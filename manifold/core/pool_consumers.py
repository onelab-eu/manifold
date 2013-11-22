from manifold.core.packet import Packet

class PoolConsumers(set):
    """
    A pool of consumers.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, consumers = None, max_consumers = None):
        """
        Constructor
        Args:
            consumers: A list or a set of Consumer instances (children of this Node).
            max_consumers: A strictly positive integer or None (maximum
                 number of children, pass None if not bounded).
        """
        if not consumers:
            consumers = list()
        if not isinstance(consumers, (list, set)):
            consumers = [consumers]

        super(PoolConsumers, self).__init__(set(consumers))

        self._max_consumers = max_consumers


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_max_consumers(self):
        return self._max_consumers

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def add(self, consumer):
        if self._max_consumers and len(self) >= self._max_consumers:
            raise Exception, "Cannot add consumer: maximum (%d) reached." % self._max_consumers
        set.add(self, consumer)

    def receive(self, packet):
        if packet.get_type() not in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            raise "Invalid packet type for consumer: %s" % Packet.get_type_name(packet.get_type())
        
        for consumer in self:
            consumer.receive(packet)
