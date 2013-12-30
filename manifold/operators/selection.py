#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Selection Operator filters incoming RECORD Packets according
# to a clause of Predicate.
#
# It acts like the WHERE Clause in SQL.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

from manifold.core.packet           import Packet
from manifold.core.producer         import Producer 
from manifold.core.query            import Query
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

DUMPSTR_SELECTION  = "WHERE %s"

#------------------------------------------------------------------
# Selection node (WHERE)
#------------------------------------------------------------------

class Selection(Operator):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, filters):
        """
        Constructor
        Args:
            child: A Node instance (the child of this Node)
            filters: A set of Predicate instances
        """
        assert issubclass(type(child), Producer),\
            "Invalid child = %r (%r)"   % (child,   type(child))
        assert isinstance(filters, set),\
            "Invalid filters = %r (%r)" % (filters, type(filters))

        self._filter = filters
        Operator.__init__(self, producers = child, max_producers = 1)

        self.query = self.get_producer().get_query().copy()
        self.query.filters |= filters
 
    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Selection instance.
        """
        return DUMPSTR_SELECTION % ' AND '.join(["%s %s %s" % f.get_str_tuple() for f in self._filter])

    def receive(self, packet):
        """
        """

        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            # XXX need to remove the filter in the query
            new_packet = packet.clone()
            packet.update_query(Query.unfilter_by, self._filter)
            self.send(new_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            record = packet

            if record.is_last() or (self._filter and self._filter.match(record)):
                self.send(record)

        else: # TYPE_ERROR
            self.send(packet)

    def dump(self, indent = 0):
        """
        Dump the current child
        Args:
            indent: The current indentation
        """
        super(Selection, self).dump(indent)
        # We have one producer for sure
        self.get_producer().dump(indent + 1)

    @returns(Producer)
    def optimize_selection(self, query, filter):
        # Concatenate both selections...
        for predicate in self._filter:
            filter.add(predicate)
        return self.get_producer().optimize_selection(query, filter)

    @returns(Producer)
    def optimize_projection(self, query, fields):
        # Do we have to add fields for filtering, if so, we have to remove them after
        # otherwise we can just swap operators
        keys = self._filter.keys()
        self.update_producer(lambda p: p.optimize_projection(query, fields | keys))
        #self.query.fields = fields
        if not keys <= fields:
            print "in selection - added projection"
            # XXX add projection that removed added_fields
            # or add projection that removes fields
            return Projection(self, fields)
        return self
