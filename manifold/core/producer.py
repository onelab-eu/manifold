#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Producer is a Node:
#  - sending RECORD (or ERROR) Packets to its Consumers
#  - receiving QUERY Packets
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.f>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import traceback
from types                          import StringTypes

from manifold.core.code             import CORE, ERROR, GATEWAY
from manifold.core.node             import Node
from manifold.core.packet           import Packet, ErrorPacket
from manifold.core.pool_consumers   import PoolConsumers
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

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
        assert not max_consumers or (isinstance(max_consumers, int) and max_consumers >= 1),\
            "Invalid max_consumers = %s (%s)" % (max_consumers, type(max_consumers))

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

    def clear_consumers(self, cascade = True):
        """
        Unlink this Producer from its Consumers.
        Args:
            cascade: A boolean set to true to unlink those
                Consumers from self.
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
        self._pool_consumers.remove(consumer)
        if cascade:
            customer.del_producer(self, cascade = False)

    @returns(StringTypes)
    def format_consumer_ids(self):
        """
        Returns:
            A String containing the ids of the Consumers of this Producer.
        """
        return "{[%s]}" % "], [".join(("%r" % consumer.get_identifier() for consumer in self.get_consumers())) 
 
    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Producer.
        """
        return "%s[%s](Consumers: %s)" % (
            self.__class__.__name__,
            self.get_identifier(),
            self.format_consumer_ids()
        )

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Producer.
        """
        return self.__repr__() 

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
        max_consumers = self.get_max_consumers()
        if max_consumers and len(consumers) > max_consumers + len(self.get_consumers()):
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
        if self.get_max_consumers() != 1:
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
            Exception: if dthe node may have several Consumers.
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

    def release(self):
        """
        Unlink this Producer from its Consumers.
        Recusively release in cascade Consumers having no more Producer.
        """
        raise Exception, "releasing a producer is meaningless"
        for consumer in self.get_consumers():
            consumer.del_producer(self, cascade = True)
            # This Producer is the only one of this parent Consumer, so we can
            # safely release this childless Consumer.
            if consumer.get_num_producers() == 0:
                consumer.release()


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def check_send(self, packet):
        """
        (Internal usage) Check Producer::send() parameters.
        """
        # A Producer sends RECORD/ERROR Packets to its consumers
        super(Producer, self).check_send(packet)
        assert packet.get_protocol() in [Packet.PROTOCOL_RECORD, Packet.PROTOCOL_ERROR],\
            "Invalid packet type (%s)" % packet

    def check_receive(self, packet):
        """
        (Internal usage) Check Producer::receive() parameters.
        """
        # A Producer sends QUERY/ERROR Packets to its consumers
        super(Producer, self).check_receive(packet)
        assert packet.get_protocol() in [Packet.PROTOCOL_QUERY, Packet.PROTOCOL_ERROR],\
            "Invalid packet type (%s)" % packet

    def send(self, packet):
        """
        Send an ERROR or RECORD Packet from this Producer towards all
        its Consumer(s).
        Args:
            packet: An ERROR or RECORD Packet instance.
        """
        self.check_send(packet)
        Log.record(packet, self)
        self._pool_consumers.receive(packet)

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
        res = super(Producer, self).format_uptree_rec(indent, res)
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

    # TODO Rename Producer::make_error() into Producer::error()
    # and retrieve the appropriate consumers and send to them
    # the ErrorPacket that has been crafted
    @returns(ErrorPacket)
    def make_error(self, origin, description, is_fatal):
        """
        Craft an ErrorPacket carrying an error message.
        Args:
            description: The corresponding error message (String) or
                Exception.
            origin: An integer indicated who raised this error.
                Valid values are {CORE, GATEWAY}
            description: A String containing the error message.
            is_fatal: Set to True if this ErrorPacket
                must make crash the pending Query.
        Returns:
            The corresponding ErrorPacket.
        """
        assert isinstance(description, StringTypes),\
            "Invalid description = %s (%s)" % (description, type(description))
        # Note: 'origin' is ignored for the moment
        # Note: 'type'   is ignored for the moment
        assert origin in [CORE, GATEWAY],\
            "Invalid origin = %s (%s)" % (origin, type(origin))
        assert isinstance(is_fatal, bool),\
            "Invalid is_fatal = %s (%s)" % (is_fatal, type(is_fatal))

        if is_fatal:
            Log.error(description)
        else:
            Log.warning(description)
        error_packet = ErrorPacket(ERROR, origin, description, traceback.format_exc())
        error_packet.set_last(is_fatal)
        return error_packet
