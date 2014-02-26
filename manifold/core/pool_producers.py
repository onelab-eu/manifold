#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.packet   import Packet
from manifold.util.log      import Log 
from manifold.util.type     import accepts, returns

class PoolProducers(set):
    """
    A pool of producers.
    A producer receives Query Packets.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------
    
    def __init__(self, producers = None, max_producers = None):
        """
        Constructor.
        """
        assert not max_producers or isinstance(max_producers, int), "max_producers not an int: %r" % max_producers
        assert not max_producers or max_producers >= 0
 
        if not producers:
            producers = set()
        if not isinstance(producers, (list, set)):
            producers = [producers]

        super(PoolProducers, self).__init__(set(producers))

        self._max_producers = max_producers

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(int)
    def get_max_producers(self):
        return self._max_producers

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def add(self, producer):
        if self._max_producers and len(self) >= self._max_producers:
            raise Exception, "Cannot add producer: maximum (%d) reached." % self._max_producers
        set.add(self, producer)

    def receive(self, packet):
        if packet.get_protocol() not in [Packet.PROTOCOL_QUERY]:
            raise ValueError, "Invalid packet type for producer: %s" % Packet.get_protocol_name(packet.get_protocol())

        for producer in self:
            producer.receive(packet)
