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

from manifold.core.address          import Address
from manifold.core.filter           import Filter
from manifold.core.field_names      import FieldNames
from manifold.core.node             import Node
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.packet_util      import packet_update_query
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
    # Constructors
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

    def copy(self):
        return Selection(self._get_child().copy(), self._filter.copy())

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(Address)
    def get_destination(self):
        """
        Returns:
            The Address corresponding to this Operator.
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
            " AND ".join(["%s %s %s" % f.get_str_tuple() for f in self._filter])
        )

    def send_impl(self, packet):
        """
        Handle an incoming QUERY_TYPES Packet and send a Packet to the child Producer.
          - If this is a RECORD Packet, forward the Packet if it's
            carried Record satisfies the Predicate(s) of this Selection
            Operator. Otherwise, drop it.
          - If this is an ERROR Packet, forward this Packet.
        Args:
            packet: A Packet instance.
        """
        # If possible, recraft the embeded Query
        if self.has_children_with_fullquery():
            self._get_child().send(packet)
        else:
            # XXX need to remove the filter in the query
            new_packet = packet.clone()

            # We don't need the result to be filtered since we are doing it...
            packet_update_query(new_packet, Query.unfilter_by, self._filter)
            # ... but we need the fields to filter on
            packet_update_query(new_packet, Query.select, self._filter.get_field_names())
            self._get_child().send(new_packet)

    def receive_impl(self, packet, slot_id = None):
        """
        Handle a RECORD or ERROR Packet issued by its Producer and forward
        a Packet to the parent Consumer.
        Args:
            packet: A Packet instance (RECORD or ERROR)
            slot_id: Unused, pass None.
        """
        packet.update_source(Address.add_filter, self._filter)
        if packet.is_empty() or self._filter.match(packet.get_dict()):
            self.forward_upstream(packet)
            return

        # We drop the packet.

        # This packet doesn't satisfies the Filter, however is has
        # the LAST_RECORD flag enabled, so we send an empty
        # RECORD Packet carrying this flag.
        if packet.is_last():
            packet.clear_data()
            self.forward_upstream(packet)

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
        keys = FieldNames(self._filter.keys())
        self._update_child(lambda p, d: p.optimize_projection(fields | keys))
        #self.query.fields = fields

        if not keys <= fields:
            # XXX add projection that removed added_fields
            # or add projection that removes fields
            return Projection(self, fields)
        return self

#DEPRECATED|    @returns(Node)
#DEPRECATED|    def reorganize_create(self):
#DEPRECATED|        return self._get_child().reorganize_create()
