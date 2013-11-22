#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Producer is a Node:
#  - sending RECORD (or ERROR) Packets to its Consumers
#  - receiving QUERY Packets
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from manifold.core.node           import Node
from manifold.core.packet         import Packet
from manifold.core.pool_consumers import PoolConsumers
from manifold.util.log            import Log
from manifold.util.type           import accepts, returns

class Producer(Node):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, consumers = None, max_consumers = None):
        """
        Constructor.
        Args:
            consumers: A list or a set of Consumer instances (children of this Node).
            max_consumers: A strictly positive integer or None (maximum
                number of children, pass None if not bounded).
        """
        Node.__init__(self)
        self._pool_consumers = PoolConsumers(consumers, max_consumers = max_consumers)

        for consumer in self.get_consumers():
            consumer.add_producer(self, cascade = False)

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(set)
    def get_consumers(self):
        """
        Retrieve the Consumers linked to this Producer.
        Returns:
            A set of Consumer instances.
        """
        return set(self._pool_consumers)

    @returns(int)
    def get_max_consumers(self):
        """
        Returns:
            The maximum number of consumers allowed for this Producer or
            None if this value is unbounded.
        """
        return self._pool_consumers.get_max_consumers()

    def clear_consumers(self, cascade = True):
        """
        Unlink all the Consumers of this Producer.
        Args:
            cascade: A boolean set to true to unlink 'self'
                to the producers set of 'consumer'.
        """
        if cascade:
            for consumer in self._pool_consumers:
                consumer.del_producer(self, cascade = False)
        self._pool_consumers.clear()

    def add_consumer(self, consumer, cascade = True):
        """
        Link a Consumer to this Producer.
        Args:
            consumer: A Consumer instance.
            cascade: A boolean set to true to add 'self'
                to the producers set of 'consumer'. 
        """
        self._pool_consumers.add(consumer)
        if cascade:
            consumer.add_producer(self, cascade = False)

    def del_consumer(self, consumer, cascade = True):
        """
        Unlink a Consumer from this Producer.
        Args:
            consumer: A Consumer instance.
            cascade: A boolean set to true to remove 'self'
                to the producers set of 'consumer'.
        """
        self._pool_consumers.remove(consumer)
        if cascade:
            customer.del_producer(self, cascade = False)

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def add_consumers(self, consumers):
        """
        Add Consumers to this Producer.
        Args:
            consumers: An Iterable (set, list...) containing one or more
                Consumer instance(s).
        """
        if len(consumers) > self.get_max_consumers() + len(self.get_consumers()):
            raise ValueError("Trying to add too many Consumers (%s) to Producer %s" % (consumers, self))
        for consumer in consumers:
            self.add_consumer(consumer)

    def update_consumers(self, function):
        raise Exception, "Producer::update_consumers: Not implemented"

    # max_consumers == 1

    #@returns(Consumer)
    def get_consumer(self):
        """
        Retrieve THE Consumer of this Producer having at most one Consumer.
        Raises:
            Exception: if the node may have several Consumers.
        Returns:
            The corresponding Consumer instance (if any), None otherwise.
        """
        if self.get_max_consumers() == 1:
            max = self.get_max_consumers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call get_consumer with max_consumers != 1 (=%s)" % max_str

        num = len(self._pool_consumers)
        if num == 0:
            return None

        return iter(self.get_consumers()).next()

    def set_consumer(self, consumer):
        """
        Set THE Consumer of this Producer having at most one Consumer.
        Args:
            consumer: A Consumer instance.
        Raises:
            Exception: if the node may have several Consumers.
        """
        if self.get_max_consumers() != 1:
            raise Exception, "Cannot call set_consumer with max_consumers != 1"
        self.clear_consumers()
        self.add_consumer(consumer)

    def set_consumers(self, consumers):
        """
        Set the Consumers of this Producer.
        Args:
            consumers: An Iterable (set, list...) containing one or more
                Consumer instance(s).
        """
        self.clear_consumers()
        self.add_consumers(consumers)

    def update_consumer(self, function):
        if self.get_max_consumers() != 1:
            max = self.get_max_consumers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call update_consumer with max_consumers != 1 (= %s)" % max_str

        self.set_consumer(function(self.get_consumer()))

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def send(self, packet):
        """
        Send an ERROR or RECORD Packet from this Producer towards its Consumer(s).
        Args:
            packet: A ERROR or RECORD Packet.
        """
        # A producer sends Record/Error Packets to its consumers
        if packet.get_type() not in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            raise ValueError, "Invalid packet type for consumer: %s" % \
                Packet.get_type_name(packet.get_type())

        if self.get_identifier():
            Log.record("[#%04d] [ %r ]" % (self.get_identifier(), packet))
        else:
            Log.record(packet)

        self._pool_consumers.receive(packet)
        
    def receive(self, packet):
        """
        Handle a QUERY Packet from a Consumer. 
        This method should be overloaded by its child class(es).
        Args:
            packet: A QUERY Packet.
        """
        assert isinstance(packet, Packet)
        if packet.get_type() not in [Packet.TYPE_QUERY]:
            raise ValueError, "Invalid packet type received in producer: %s" % \
                Packet.get_type_name(packet.get_type()) 
