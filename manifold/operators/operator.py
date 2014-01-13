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

from manifold.core.packet import Packet
from manifold.core.query  import Query 
from manifold.core.relay  import Relay
from manifold.util.log    import Log
from manifold.util.type   import accepts, returns

# NOTES: it seem we don't need the query anymore in the operators expect From
# maybe ? Selection, projection ??

class Operator(Relay):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, producers = None, consumers = None, max_producers = None, max_consumers = None, has_parent_producer = False):
        """
        Constructor.
        Args:
            See relay::__init__()
        """
        Relay.__init__( \
            self, \
            producers = producers, \
            consumers = consumers, \
            max_consumers = max_consumers, \
            max_producers = max_producers, \
            has_parent_producer = has_parent_producer \
        )

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        Handle a Packet.
        Args:
            packet: A Packet instance.
                - If this is a RECORD Packet, this Operator is supposed to recraft
                the Record nested in this Packet and send it to its Consumers (parent
                Operators) and its additional Receivers (if any). Those Receivers
                corresponds to AST roots.
                - Otherwise (ERROR Packet), this Operator should simply
                forward this Packet.
        """
        raise Exception, "Operator::receive() must be overwritten in children classes"
        
    @returns(Query)
    def get_query(self):
        """
        Returns:
            A Query having a SELECT and a WHERE clause homogeneous to the
            query modeled by the tree of Operator rooted to this Operator.
        """
        return self.query

    def dump(self, indent = 0):
        """
        Dump the current node
        indent current indentation
        """
        self.tab(indent)
        print "%r (%s)" % (self, super(Operator, self).__repr__())
        for producer in self.get_producers():
            producer.dump(indent + 1)
