from manifold.core.pool_consumers import PoolConsumers
from manifold.core.pool_producers import PoolProducers

class Socket(Producer, Consumer):
    def __init__(self, packet, router):
        """
        packet:
            The packet should have a valid producer otherwise the result
        packets won't be able to reach anybody.
        """

        # A socket serves only one query
        Producer.__init__(self, packet.get_receiver(), max_consumers = 1)
        Consumer.__init__(self, max_producer  = 1)
    
        self._packet = packet
