#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An Union Node aggregates the Record returned by several
# child Nodes.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

from manifold.core.destination      import Destination
from manifold.core.exceptions       import ManifoldInternalException
from manifold.core.fields           import Fields
from manifold.core.node             import Node
from manifold.core.operator_slot    import ChildrenSlotMixin
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.operators             import ChildStatus, ChildCallback
from manifold.operators.subquery    import SubQuery
from manifold.operators.projection  import Projection
from manifold.operators.operator    import Operator
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

#------------------------------------------------------------------
# UNION node
#------------------------------------------------------------------

class Union(Operator, ChildrenSlotMixin):
    """
    UNION operator node.
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, producers, key, distinct = True):
        """
        Constructor.
        Args:
            children: A list of Node instances, the children of
                this Union Node.
            key: A Key instance, corresponding to the key for
                elements returned from the node.
        """
        Operator.__init__(self)
        ChildrenSlotMixin.__init__(self)

        self._key      = key
        self._distinct = distinct

        # XXX ???
        self.key_list = list()

        for producer in producers:
            data = {
            }
            self._set_child(producer, data)
        self._remaining_children = self._get_num_children()

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this LeftJoin Operator.
        """
        distinct_str = ' DISTINCT' if self._distinct else ''
        return "UNION%s" % (distinct_str)

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def _get_first(self):
        for producer, _ in self._iter_slots():
            return producer
        raise ManifoldInternalException, "UNION must have at least one producer"

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(Destination)
    def get_destination(self):
        """
        Returns:
            The Destination corresponding to this Operator.
        """
        return self._get_first().get_destination()

    def receive_impl(self, packet):
        """
        Handle an incoming Packet.
        Args:
            packet: A Packet instance.
        """

        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            # We simply forward the query to all children
            for _, child, _ in self._iter_children():
                child.receive(packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            record = packet
            is_last = record.is_last()
            record.unset_last()
            do_send = True

            if not record.is_empty():

                # Ignore duplicate records
                if self._distinct:
                    key = self._key.get_field_names()
                    if key and record.has_fields(key):
                        key_value = record.get_value(key)
                        if key_value in self.key_list:
                            do_send = False
                        else:
                            self.key_list.append(key_value)

                record.unset_last()
                if do_send:
                    self.forward_upstream(record)

            if is_last:
                # In fact we don't care to know which child has completed
                self._remaining_children -= 1
                if self._remaining_children == 0:
                    self.forward_upstream(Record(last = True))

        else: # TYPE_ERROR
            self.forward_upstream(packet)

    #---------------------------------------------------------------------------
    # AST manipulations & optimization
    #---------------------------------------------------------------------------

    @returns(Node)
    def optimize_selection(self, filter):
        # UNION: apply selection to all children
        self._update_children_producers(lambda p, d: p.optimize_selection(filter))
        return self

    def optimize_projection(self, fields):
        # UNION: apply projection to all children
        # in case of UNION with duplicate elimination, we need the key
        do_parent_projection = False
        child_fields  = Fields()
        child_fields |= fields
        if self._distinct:
            key = self._key.get_field_names()
            if key not in fields: # we are not keeping the key
                do_parent_projection = True
                child_fields |= key

        self._update_children_producers(lambda p,d : p.optimize_projection(child_fields))

        if do_parent_projection:
            return Projection(self, fields)
        return self

    #---------------------------------------------------------------------------
    # Algebraic rules
    #---------------------------------------------------------------------------

    def subquery(self, ast, relation):
        """
        SQ_new o U

        Overrides the default behaviour where the SQ operator is added at the
        top.
        """

        if not relation.is_local():
            return SubQuery.make(self, ast, relation)

        # XXX Note that it can be partially local... no managed at the moment

        # SQ_new o U  =>  U o SQ_new if SQ_new is local
        self._update_children_producers(lambda p, d: p.subquery(ast, relation))

        return self

