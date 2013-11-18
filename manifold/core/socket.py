# -*- coding: utf-8 -*-

from manifold.core.packet   import Packet
from manifold.core.relay    import Relay

class Socket(Relay):
    def __init__(self, packet, router):
        """
        packet:
            The packet should have a valid producer otherwise the result
        packets won't be able to reach anybody.
        """

        # A socket serves only one query
        Relay.__init__(self, consumers=packet.get_receiver(), max_consumers = 1, max_producers = 1)
    
        self._packet = packet

    def receive(self, packet):
        self.send(packet)
