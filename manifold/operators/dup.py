#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Dup Node filters every incoming Record that have already traversed
# this Dup Node. It acts like "| uniq" in shell.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr> 

from types                          import StringTypes

from manifold.core.key              import Key
from manifold.core.packet           import Packet
from manifold.core.node             import Node
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.operators.operator    import Operator
from manifold.util.type             import returns

#------------------------------------------------------------------
# DUP node
#------------------------------------------------------------------

DUMPSTR_DUP = "DUP"

class Dup(Operator):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, key):
        """
        Constructor.
        Args:
            child: A Node instance, child of this Dup Node.
            key: A Key instance which describes which fields of
                incoming Records allows to detect duplicates.
        """
        raise Exception("Dup: uses obsolete get_producer()")
        #assert isinstance(key, Key),\
        #    "Invalid key %r (%r)" % (key, type(key))

        super(Dup, self).__init__(self, producers = child, max_producers = 1)
        self.query = self.get_producer().get_query().copy()

        # A set of Strings (if key is made of only one field) or a set of tuples
        # which stores the Key of each Record that have already traversed this
        # Dup Operator.
        self._seen = set()

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

    @returns(bool)
    def is_duplicate(self, record):
        """
        Returns:
            True iif this record has already been seen
        """
        return record.get_value() in self._seen
        
    def send_impl(self, packet, slot_id = None):
        """
        Process an incoming Packet instance.
        Args:
            packet: A Packet instance.
        """
        # Demux Operator simply forwards any kind of Packet to its
        # Consumer(s)/Node according to the nature of the Packet.
        if packet.get_protocol() == Packet.PROTOCOL_CREATE:
            record = packet
            if not self.is_duplicate(record):
                self.send(packet)
            elif packet.is_last():
                # This packet has been already seen, however is has
                # the LAST_RECORD flag enabled, so we send an empty
                # RECORD Packet carrying this flag.
                self.send(Packet(Packet.PROTOCOL_CREATE, True))
        else:
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
