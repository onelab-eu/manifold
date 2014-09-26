#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Node is the parent class of: 
# - Consumer (see manifold/core/consumer.py)
# - Producer (see manifold/core/producer.py)
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import sys
from types                          import StringTypes

from manifold.core.packet           import Packet
from manifold.core.record           import Record
from manifold.core.pool_consumers   import PoolConsumers
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

class Node(object):
    """
    A processing node. Base object
    """

    last_identifier = 0


    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        """
        Constructor.
        """
        try:
            self._identifier
            #print "node already init"
            return
        except: pass

        Node.last_identifier += 1
        self._identifier = Node.last_identifier
        self._pool_consumers = PoolConsumers()

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(int)
    def get_identifier(self):
        """
        Returns:
            The identifier of this Node.
        """
        return self._identifier

    #---------------------------------------------------------------------------
    # Helpers
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
    def get_num_consumers(self):
        """
        Returns:
            The number of Consumers related to this Producer.
        """
        return len(self.get_consumers())

    @returns(int)
    def get_max_consumers(self):
        """
        Returns:
            The maximum number of Consumers allowed for this Producer or
            None if this value is unbounded.
        """
        return self._pool_consumers.get_max_consumers()

    def clear_consumers(self): #MANDO|, cascade = True):
        """
        Unlink this Producer from its Consumers.
        Args:
#MANDO|            cascade: A boolean set to true to unlink those
#MANDO|                Consumers from self.
        """
#MANDO|        if cascade:
#MANDO|            for consumer in self._pool_consumers:
#MANDO|                consumer.del_producer(self, cascade = False)
        self._pool_consumers.clear()

    def add_consumer(self, consumer, cascade = True, slot_id = None):
        """
        Link a Consumer to this Producer.
        Args:
            consumer: A Consumer instance.
            cascade: A boolean set to true to add 'self'
                to the producers set of 'consumer'. 
        """
        self._pool_consumers.add(consumer, slot_id = slot_id)
        if cascade:
            Log.warning("Cascade not implemented")
        #    consumer.add_producer(self, cascade = False)

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif this Socket has no Consumer.
        """
        return len(self._pool_consumers) == 0

    def del_consumer(self, consumer, cascade = True):
        """
        Unlink a Consumer from this Producer.
        Args:
            consumer: A Consumer instance.
            cascade: A boolean set to true to remove 'self'
                to the producers set of 'consumer'.
        """
        try:
            self._pool_consumers.remove(consumer)
        except Exception, e:
            Log.warning("Exception in del_consumer: %s" % str(e))
        if cascade:
            Log.warning("Cascade not implemented")
        #    consumer.del_producer(self, cascade = False)

    @returns(StringTypes)
    def format_consumer_ids(self):
        """
        Returns:
            A String containing the ids of the Consumers of this Producer.
        """
        return "{[%s]}" % "], [".join(("%r" % consumer.get_identifier() for consumer in self.get_consumers())) 
 


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def check_packet(self, packet):
        assert isinstance(packet, Packet), \
            "Invalid packet = %s (%s)" % (packet, type(packet))

    def check_receive(self, packet):
        """
        Check Node::receive() parameters. This method should be overloaded.
        """
        self.check_packet(packet)

#DEPRECATED|    def check_send(self, packet):
#DEPRECATED|        """
#DEPRECATED|        Check Node::send() parameters. This method should be overloaded.
#DEPRECATED|        """
#DEPRECATED|        self.check_packet(packet)
#DEPRECATED|
#DEPRECATED|    def send(self, packet):
#DEPRECATED|        """
#DEPRECATED|        (pure virtual method). Send a Packet.
#DEPRECATED|        Args:
#DEPRECATED|            packet: A Packet instance. 
#DEPRECATED|        """
#DEPRECATED|        raise NotImplementedError("Method 'send' must be overloaded: %s" % self.__class__.__name__)
        
    def receive(self, packet, slot_id = None):
        """
        (pure virtual method). Send a Packet.
        Args:
            packet: A Packet instance.
        """
        self.check_receive(packet)
        Log.record(packet)
        self.receive_impl(packet, slot_id = slot_id)

    def send_to(self, receiver, packet):
        # Optimization, we do not forward empty queries
        fields = packet.get_destination().get_field_names()
        if fields.is_empty():
            self.forward_upstream(Record(last=True))
            return
        #packet.set_source(self)
        receiver.receive(packet)

    def forward_upstream(self, packet):
        #self.check_send(packet)
        #packet.set_source(self)
        if packet.get_protocol() in [Packet.PROTOCOL_QUERY]:
            raise Exception("A query cannot be forwarded")
        elif packet.get_protocol() in [Packet.PROTOCOL_CREATE, Packet.PROTOCOL_ERROR]:
            self._pool_consumers.receive(packet)

    @returns(StringTypes)
    def format_node(self, indent = 0):
        """
        Args:
            ident: An integer corresponding to the current indentation.
        Returns:
            The '%s' representation of this Node
        """
        return "[%(id)04d]%(tab)s %(self)s" % {
            "id"   : self.get_identifier(),
            "tab"  : "  " * indent,
            "self" : self
        }

 
    @returns(StringTypes)
    def format_downtree_rec(self, indent, res):
        """
        (Internal use)
        Format debug information to test the path(s) from this Consumer 
        towards the end-Producer(s)
        Args:
            ident: An integer corresponding to the current indentation.
            res: The String we're crafting (rec)
        Returns:
            The String containing the corresponding down-tree.
        """
        # XXX maybe "res = ..." are not needed anymore since we are modifying
        # the string in place: res is a parameter of every function call
        #res = self.format_downtree_rec(indent, res)
        res =  "%(res)s%(self)s" % {
            "res"  : "%s\n" % res if res else "",
            "self" : self.format_node(indent + 2)
        }
        for node, data in self._iter_slots():
            res = node.format_downtree_rec(indent + 2, res)
        return res

    @returns(StringTypes)
    def format_downtree(self):
        """
        Format debug information to test the path(s) from this Consumer 
        towards the end-Producer(s)
        Returns:
            The String containing the corresponding down-tree.
        """
        res = ""
        return self.format_downtree_rec(0, res)

    @returns(StringTypes)
    def format_uptree_rec(self, indent, res):
        """
        (Internal use)
        Format debug information to test the path(s) from this Producer
        towards the end-Consumer(s)
        Args:
            ident: An integer corresponding to the current indentation.
            res: The String we're crafting (rec)
        Returns:
            The String containing the corresponding up-tree.
        """
        res = "%(res)s%(self)s" % {
            "res"  : "%s\n" % res if res else "",
            "self" : self.format_node(indent + 2)
        }
        for consumer in self.get_consumers():
            res = consumer.format_uptree_rec(indent + 2, res)
        return res

    @returns(StringTypes)
    def format_uptree(self):
        """
        Format debug information to test the path(s) from this Producer
        towards the end-Consumer(s)
        Returns:
            The String containing the corresponding up-tree.
        """
        res = ""
        return self.format_uptree_rec(0, res)

