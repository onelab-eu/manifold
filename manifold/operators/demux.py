#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Demux Node forwards incoming RECORD Packets to 
# several parent Nodes. A Demux Operator is built
# on the top of a Node allowing at most 1 Consumer.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr> 

from types                          import StringTypes

from manifold.core.node             import Node
from manifold.core.query            import Query
from manifold.operators.operator    import Operator 
from manifold.util.type             import returns

#------------------------------------------------------------------
# Demux Operator 
#------------------------------------------------------------------

DUMPSTR_DEMUX = "DEMUX"

class Demux(Operator):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child):
        """
        Constructor
        Args:
            child A Node instance, child of this Demux Node.
        """
        raise Exception("Demux: uses obsolete get_producer()")
        assert issubclass(type(child), Node),\
            "Invalid child = %r (%r)" % (child, type(child))

        super(Demux, self).__init__(self, producers = child, max_producers = 1)
        self.query = self.get_producer().get_query().copy()

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Selection instance.
        """
        return "DEMUX"

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive_impl(self, packet):
        """
        Process an incoming Packet instance.
        Args:
            packet: A Packet instance.
        """
        # Demux Operator simply forwards any kind of Packet to its
        # Consumer(s)/Node according to the nature of the Packet.
        self.send(packet)

    @returns(Node)
    def optimize_selection(self, query, filter):
        """
        Propagate Selection Operator through this Operator.
        Args:
            filter: A Filter instance storing the WHERE clause.
        Returns:
            The root Operator of the optimized sub-AST.
        """
        Log.tmp("Not yet tested")
        self.get_producer().optimize_selection(query, filter)
        return self

    @returns(Node)
    def optimize_projection(self, query, fields):
        """
        Propagate Projection Operator through this Operator.
        Args:
            fields: A list of String correspoding the SELECTed fields.
        Returns:
            The root Operator of the optimized sub-AST.
        """
        Log.tmp("Not yet tested")
        self.get_producer().optimize_projection(query, fields)
        return self
