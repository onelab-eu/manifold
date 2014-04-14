#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Projection Operator remove from Records the fields that
# are not queried.
#
# It acts like the SELECT Clause in SQL.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

from manifold.core.destination      import Destination
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.record           import Record, Records
from manifold.core.node             import Node
from manifold.operators.operator    import Operator
from manifold.util.log              import Log
from manifold.util.type             import returns

DUMPSTR_PROJECTION = "SELECT %s"

def do_projection(record, fields):
    """
    Take the necessary fields in dic
    """
    ret = Record()

    # 1/ split subqueries
    local = []
    subqueries = {}
    for f in fields:
        if '.' in f:
            method, subfield = f.split('.', 1)
            if not method in subqueries:
                subqueries[method] = []
            subqueries[method].append(subfield)
        else:
            local.append(f)

    # 2/ process local fields
    for l in local:
        ret[l] = record[l] if l in record else None

    # 3/ recursively process subqueries
    for method, subfields in subqueries.items():
        # record[method] is an array whose all elements must be
        # filtered according to subfields
        arr = Records()
        if not method in record:
            continue
        for x in record[method]:
            arr.append(do_projection(Record(x), subfields))
        ret[method] = arr

    ret.set_last(record.is_last())

    return ret

#------------------------------------------------------------------
# Projection Operator (SELECT)
#------------------------------------------------------------------

class Projection(Operator, ChildSlotMixin):
    """
    Projection Operator Node (cf SELECT clause in SQL)
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, fields):
        """
        Constructor.
        Args:
            child: A Node instance which will be the child of
                this Projection.
            fields: A list of Field instances corresponding to
                the fields we're selecting.
        """
        #for field in fields:
        #    assert isinstance(field, Field), "Invalid field %r (%r)" % (field, type(field))

        assert issubclass(type(child), Node),\
            "Invalid child = %r (%r)"   % (child, type(child))
        if isinstance(fields, (list, tuple, frozenset)):
            fields = Fields(fields)
        self._fields = fields

        Operator.__init__(self)
        ChildSlotMixin.__init__(self)

        self._set_child(child)

#DEPRECATED|        self.query = self._get_child().get_query().copy()
#DEPRECATED|        self.query.fields &= fields

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(set)
    def get_fields(self):
        """
        Returns:
            The set of Field instances selected in this Projection instance.
        """
        return self._fields

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Projection instance.
        """
        fields = self.get_fields()
        s = "*" if fields.is_star() else ", ".join(self.get_fields())
        return DUMPSTR_PROJECTION % s

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
        return d.projection(self._fields)

    def receive_impl(self, packet):
        """
        Process an incoming Packet instance.
          - If this is a RECORD Packet, remove every fields that are
            not SELECTed by this Operator.
          - If this is an ERROR Packet, forward this Packet.
        Args:
            packet: A Packet instance.
        """
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            self._get_child().receive(packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            record = packet
            if not record.is_empty() and not self._fields.is_star():
                record = do_projection(record, self._fields)
            self.forward_upstream(record)

        else: # TYPE_ERROR
            self.forward_upstream(packet)

    @returns(Node)
    def optimize_selection(self, filter):
        self._update_child(lambda c, d: c.optimize_selection(filter))
        return self

    @returns(Node)
    def optimize_projection(self, fields):
        # We only need the intersection of both
        return self._get_child().optimize_projection(self._fields & fields)

    @returns(Node)
    def reorganize_create(self):
        return self._get_child().reorganize_create()
