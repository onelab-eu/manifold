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

import copy
from manifold.core.code             import CORE
from manifold.core.operator_slot    import SlotMixin 
from manifold.core.packet           import Packet
from manifold.core.query            import Query 
from manifold.core.node             import Node
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

# NOTES: it seem we don't need the query anymore in the operators expect From
# maybe ? Selection, projection ??

class Operator(Node, SlotMixin):

    def __init__(self):
        Node.__init__(self)

        # This forces all operators (provided they call the constructor of
        # Operator), 
        # to include a Mixin, otherwise an exception will be raised
        SlotMixin.__init__(self)

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def send(self, packet):
        """
        Args:
            packet: A Packet instance.
        """
        Log.record(packet)
        # TODO: we should impose source anytime a packet is issued
        #assert(packet.get_source() != None)
        self.send_impl(packet)

    def send_impl(self, packet):
        """
        Args:
            packet: A Packet instance.
        """
        raise RuntimeError("send_impl: This method must be overloaded")

    @returns(bool)
    def has_children_with_fullquery(self):
        """
        Returns:
            True iif this Operator or at least one of its child uses 
            fullquery Capabilities.
        """
        for producer, _ in self._iter_slots():
            if producer.has_children_with_fullquery():
                return True
        return False 

    def receive_impl(self, packet, slot_id = None):
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
        self.forward_upstream(packet)
        
#DEPRECATED|    @returns(Query)
#DEPRECATED|    def get_query(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            A Query having a SELECT and a WHERE clause homogeneous to the
#DEPRECATED|            query modeled by the tree of Operator rooted to this Operator.
#DEPRECATED|        """
#DEPRECATED|        return self.query
    
#---------------------------------------------------------------------------
# Default operator implementation
#
# These are the default methods applied when no more algebraic rule is
# applicable.
#---------------------------------------------------------------------------

