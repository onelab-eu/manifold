#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An Operator Node represents a SQL-like operation and
# is used to build an OperatorGraph. Operator forwards
# QUERY Packets received from its parents (Consumers) to its
# children (Producers). Resulting RECORD or ERROR Packets
# are sent back to its parents.
#
# Note:
# - An Operator producer may be either an Operator
# instance or a Socket.
# - An Operator consumer may be either an Operator
# instance or, if this is a From instance, a Gateway. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.core.code     import CORE
from manifold.core.packet   import Packet
from manifold.core.producer import Producer
from manifold.core.query    import Query 
from manifold.core.relay    import Relay
from manifold.util.log      import Log
from manifold.util.type     import accepts, returns

# NOTES: it seem we don't need the query anymore in the operators expect From
# maybe ? Selection, projection ??

class Operator(Relay):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, producers = None, parent_producer = None, consumers = None, max_producers = None, max_consumers = None, has_parent_producer = False):
        """
        Constructor.
        Args:
            See relay::__init__()
        """
        Relay.__init__( \
            self, \
            producers = producers, \
            consumers = consumers, \
            parent_producer = parent_producer, \
            max_consumers = max_consumers, \
            max_producers = max_producers, \
            has_parent_producer = has_parent_producer \
        )

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(bool)
    def has_children_with_fullquery(self):
        """
        Returns:
            True iif this Operator or at least one of its child uses 
            fullquery Capabilities.
        """
        for producer in self.get_producers():
            if producer.has_children_with_fullquery():
                return True
        return False 

    def receive_impl(self, packet):
        """
        Handle a Packet (must be re-implemented in children classes).
        Args:
            packet: A Packet instance.
                - If this is a RECORD Packet, this Operator is supposed to recraft
                the Record nested in this Packet and send it to its Consumers (parent
                Operators) and its additional Receivers (if any). Those Receivers
                corresponds to AST roots.
                - Otherwise (ERROR Packet), this Operator should simply
                forward this Packet.
        """
        raise Exception, "Operator::receive_impl() must be overwritten in children classes"
        
#DEPRECATED|    @returns(Query)
#DEPRECATED|    def get_query(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            A Query having a SELECT and a WHERE clause homogeneous to the
#DEPRECATED|            query modeled by the tree of Operator rooted to this Operator.
#DEPRECATED|        """
#DEPRECATED|        return self.query

    def receive(self, packet):
        """
        Handle a Packet.
        Args:
            packet: A Packet instance.
        """
        self.receive_impl(packet)

    def error(self, description, is_fatal = True):
        """
        Craft an ErrorPacket carrying an error message.
        Args:
            description: The corresponding error message (String) or
                Exception.
            is_fatal: Set to True if this ErrorPacket
                must make crash the pending Query.
        """
        # Could be factorized with Gateway::error() by defining Producer::error()
        print "error packet making"
        error_packet = self.make_error(CORE, description, is_fatal)
        self.send(error_packet)
