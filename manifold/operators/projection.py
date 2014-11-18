#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Projection Operator remove from Records the field_names that
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
from manifold.core.field_names      import FieldNames
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.record           import Record, Records
from manifold.core.node             import Node
from manifold.operators.operator    import Operator
from manifold.util.log              import Log
from manifold.util.type             import returns

DUMPSTR_PROJECTION = "SELECT %s"

def do_projection(record, field_names):
    """
    Take the necessary field_names in dic
    """
    ret = Record()
    ret.set_source(record.get_source())
    ret.set_destination(record.get_destination())

    # 1/ split subqueries
    local = []
    subqueries = {}
    for f in field_names:
        if '.' in f:
            method, subfield = f.split('.', 1)
            if not method in subqueries:
                subqueries[method] = []
            subqueries[method].append(subfield)
        else:
            local.append(f)

    # 2/ process local field_names
    for l in local:
        ret[l] = record[l] if l in record else None

    # 3/ recursively process subqueries
    for method, subfield_names in subqueries.items():
        # record[method] is an array whose all elements must be
        # filtered according to subfield_names
        if not method in record:
            continue
        if isinstance(record[method], Records):
            arr = Records()
            for x in record[method]:
                assert isinstance(x, Record)
                arr.append(do_projection(x, subfield_names))
        elif isinstance(record[method], Record):
            arr = do_projection(record[method], subfield_names)
        else:
            raise Exception, "Not supported"
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
    # Constructors
    #---------------------------------------------------------------------------

    def __init__(self, child, field_names):
        """
        Constructor.
        Args:
            child: A Node instance which will be the child of
                this Projection.
            field_names: A list of Field instances corresponding to
                the field_names we're selecting.
        """
        #for field in field_names:
        #    assert isinstance(field, Field), "Invalid field %r (%r)" % (field, type(field))

        assert issubclass(type(child), Node),\
            "Invalid child = %r (%r)"   % (child, type(child))
        if isinstance(field_names, (list, tuple, frozenset)):
            is_star = (len(field_names) == 0)
            field_names = FieldNames(star = True) if is_star else FieldNames(field_names)
        self._field_names = field_names

        Operator.__init__(self)
        ChildSlotMixin.__init__(self)

        self._set_child(child)

    def copy(self):
        return Projection(self._get_child().copy(), self._field_names.copy())

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(FieldNames)
    def get_field_names(self):
        """
        Returns:
            The set of Field instances selected in this Projection instance.
        """
        return self._field_names

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Projection instance.
        """
        field_names = self.get_field_names()
        s = "*" if field_names.is_star() else ", ".join(self.get_field_names())
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
        return d.projection(self._field_names)

    def send(self, packet):
        """
        Process an incoming Packet instance.
          - If this is a RECORD Packet, remove every field_names that are
            not SELECTed by this Operator.
          - If this is an ERROR Packet, forward this Packet.
        Args:
            packet: A Packet instance.
        """
        packet.update_destination(lambda d: d.add_field_names(self._field_names))
        self._get_child().send(packet)

    def receive_impl(self, packet, slot_id = None):

        if not packet.is_empty() and not self._field_names.is_star():
            packet = do_projection(packet, self._field_names)
        self.forward_upstream(packet)


    @returns(Node)
    def optimize_selection(self, filter):
        self._update_child(lambda c, d: c.optimize_selection(filter))
        return self

    @returns(Node)
    def optimize_projection(self, field_names):
        # We only need the intersection of both
        return self._get_child().optimize_projection(self._field_names & field_names)

#DEPRECATED|    @returns(Node)
#DEPRECATED|    def reorganize_create(self):
#DEPRECATED|        return self._get_child().reorganize_create()
