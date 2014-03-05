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

import traceback
from types                          import StringTypes

from manifold.core.destination      import Destination
from manifold.core.filter           import Filter
from manifold.core.node             import Node
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

DUMPSTR_SELECTION  = "WHERE %s"

#------------------------------------------------------------------
# Selection Operator (WHERE)
#------------------------------------------------------------------

class Selection(Operator, ChildSlotMixin):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, filter):
        """
        Constructor
        Args:
            child: A Node instance (the child of this Node)
            filter: A Filter instance. 
        """
        assert issubclass(type(child), Node),\
            "Invalid child = %r (%r)"   % (child, type(child))
        assert isinstance(filter, Filter),\
            "Invalid filter = %r (%r)" % (filter, type(filter))

        self._filter = filter
        Operator.__init__(self)
        ChildSlotMixin.__init__(self)

        self._set_child(child)

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(Destination)
    def get_destination(self):
        """
        Returns:
            The Destination corresponding to this Operator. 
        """
        d = self._get_child().get_destination()
        return d.selection(self._filter)

    @returns(Filter)
    def get_filter(self):
        return self._filter

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Selection instance.
        """
        return DUMPSTR_SELECTION % (
            ' AND '.join(["%s %s %s" % f.get_str_tuple() for f in self._filter])
        )


    def receive_impl(self, packet):
        """
        Process an incoming Packet instance.
          - If this is a RECORD Packet, forward the Packet if it's
            carried Record satisfies the Predicate(s) of this Selection
            Operator. Otherwise, drop it.
          - If this is an ERROR Packet, forward this Packet.
        Args:
            packet: A Packet instance.
        """
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            # If possible, recraft the embeded Query
            if self.has_children_with_fullquery():
                self.send(packet)
            else:
                # XXX need to remove the filter in the query
                new_packet = packet.clone()
                
                # We don't need the result to be filtered since we are doing it...
                new_packet.update_query(Query.unfilter_by, self._filter)

                # ... but we need the fields to filter on
                new_packet.update_query(Query.select, self._filter.get_field_names())
                self.send(new_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            record = packet
            if not record.is_empty() and self._filter.match(record.get_dict()):
                self.send(packet)
            elif packet.is_last():
                # This packet doesn't satisfies the Filter, however is has
                # the LAST_RECORD flag enabled, so we send an empty
                # RECORD Packet carrying this flag.
                self.send(Record(last = True))

        else: # TYPE_ERROR
            self.send(packet)

    @returns(Node)
    def optimize_selection(self, filter):
        # Concatenate both selections...
        for predicate in self._filter:
            filter.add(predicate)
        return self._get_child().optimize_selection(filter)

    @returns(Node)
    def optimize_projection(self, fields):
        # Do we have to add fields for filtering, if so, we have to remove them after
        # otherwise we can just swap operators
        keys = self._filter.keys()
        self._update_child(lambda p, d: p.optimize_projection(fields | keys))
        #self.query.fields = fields
        if not keys <= fields:
            # XXX add projection that removed added_fields
            # or add projection that removes fields
            return Projection(self, fields)
        return self

    @returns(Node)
    def reorganize_create(self):
        return self._get_child().reorganize_create()
