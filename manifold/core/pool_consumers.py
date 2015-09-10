#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
        assert not consumers # issue with slot_ids
        if not consumers:
            consumers = list()
        if not isinstance(consumers, (list, set)):
            consumers = [consumers]

        super(PoolConsumers, self).__init__(set(consumers))

        self._max_consumers = max_consumers
        self._slot_ids = dict()


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_max_consumers(self):
        return self._max_consumers

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def add(self, consumer, slot_id):
        if self._max_consumers and len(self) >= self._max_consumers:
            raise Exception, "Cannot add consumer: maximum (%d) reached." % self._max_consumers
        self._slot_ids[consumer] = slot_id
        set.add(self, consumer)

    def receive(self, packet):
        if packet.get_protocol() not in [Packet.PROTOCOL_QUERY, Packet.PROTOCOL_ERROR]:
            raise Exception, "Invalid packet type for consumer: %s" % Packet.get_protocol_name(packet.get_protocol())

        for consumer in self:
            consumer.receive(packet, slot_id = self._slot_ids[consumer])
