#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Consumer is a Node:
#  - sending QUERY Packets to its Producers
#  - receiving a RECORD Packets (or an ERROR Packet) 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                        import StringTypes

from manifold.core.node           import Node
from manifold.core.packet         import Packet
from manifold.core.pool_producers import PoolProducers
from manifold.util.log            import Log
from manifold.util.type           import accepts, returns

class Consumer(Node):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, producers = None, max_producers = 1, has_parent_producer = False):
        """
        Constructor.
        Args:
            consumers: A list or a set of Producer instances (children of this Node).
            max_producers: A strictly positive integer or None (maximum
                number of parents, pass None if not bounded).
            has_consumer_producer: (temporary fix for LeftJoin)
        """
        Node.__init__(self)
        self._pool_producers = PoolProducers(producers, max_producers = max_producers)

        for producer in self.get_producers():
            producer.add_consumer(self, cascade = False)

        self._has_parent_producer = has_parent_producer

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(set)
    def get_producers(self):
        """
        Retrieve the Producers linked to this Consumer.
        Returns:
            A set of Producer instances.
        """
        return set(self._pool_producers)

    @returns(int)
    def get_num_producers(self):
        """
        Returns:
            The number of Producers related to this Consumer.
        """
        return len(self.get_producers())

    @returns(int)
    def get_max_producers(self):
        """
        Returns:
            The maximum number of Producers allowed for this Consumer or
            None if this value is unbounded.
        """
        return self._pool_producers.get_max_producers()

    def clear_producers(self, cascade = True):
        """
        Unlink this Consumer from its Producers.
        Args:
            cascade: A boolean set to true to unlink those
                Producers from self.
        """
        if cascade:
            for producer in self._pool_producers:
                producer.del_consumer(self, cascade = False)
        self._pool_producers.clear()

    def add_producer(self, producer, cascade = True):
        self._pool_producers.add(producer)
        if cascade:
            producer.add_consumer(self, cascade = False)

    def del_producer(self, producer, cascade = True):
        self._pool_producers.remove(producer)
        if cascade:
            producer.del_consumer(self, cascade = False)

    @returns(StringTypes)
    def format_producer_ids(self):
        """
        Returns:
            A String containing the ids of the Producers of this Consumer.
        """
        return "{[%s]}" % "], [".join(("%r" % producer.get_identifier() for producer in self.get_producers())) 
 
    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Consumer.
        """
        return "%s[%s](Producers: %s)" % (
            self.__class__.__name__,
            self.get_identifier(),
            self.format_producer_ids()
        )

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Consumer.
        """
        return self.__repr__() 

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def add_producers(self, producers):
        for producer in producers:
            self.add_producer(producer)

    def update_producers(self, function):
        raise Exception, "Not implemented"

    #@returns(Producer)
    def get_producer(self):
        if self.get_max_producers() != 1:
            max = self.get_max_producers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call get_producer with max_producers != 1 (=%s)" % max_str

        num = len(self._pool_producers)
        if num == 0:
            return None

        return iter(self.get_producers()).next()

    def set_producer(self, producer, cascade = True):
        self.clear_producers()
        self.add_producer(producer, cascade)

    def set_producers(self, producers):
        self.clear_producers()
        self.add_producers(producers)

    def update_producer(self, function):
        if self.get_max_producers() != 1:
            max = self.get_max_producers()
            max_str = "%d" % max if max else 'UNLIMITED'
            raise Exception, "Cannot call update_producer with max_producers != 1 (=%s)" % max_str

        self.set_producer(function(self.get_producer()))

    def release(self):
        """
        Unlink this Consumer from its Producers.
        Recusively release in cascade Producers (resp. Consumers)
        having no more Consumer (resp. Producer)
        """
        for producer in self.get_producers():
            producer.del_consumer(self, cascade = True)
            # This Consumer has only this child Producer, so we can
            # safely release this childless Consumer.
            if producer.get_num_consumers() == 0:
                producer.release()

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def check_send(self, packet):
        """
        Check Node::send() parameters. This method should be overloaded.
        """
        super(Consumer, self).check_send(packet)
        assert packet.get_protocol() in [Packet.PROTOCOL_QUERY],\
            "Invalid packet type (%s)" % packet

    def send(self, packet):
        """
        Send a QUERY Packet from this Consumer towards its Producers(s).
        Args:
            packet: A QUERY Packet.
        """
        self.check_send(packet)
        self._pool_producers.receive(packet)
        
    def receive(self, packet):
        raise NotImplementedError("Not yet implemented")

    def debug(self, indent = 0):
        """
        Print debug information to test the path(s) from this Producer
        towards the end-Consumer(s). This function ends this recursion.
        """
        tab = " " * indent
        Log.tmp("%s%s (END CONSUMER)" % (tab, self))
