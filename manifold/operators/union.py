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
from manifold.core.field_names      import FieldNames
from manifold.core.node             import Node
from manifold.core.operator_slot    import ChildrenSlotMixin
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Record, Records
from manifold.operators             import ChildStatus, ChildCallback
from manifold.operators.left_join   import LeftJoin
from manifold.operators.subquery    import SubQuery
from manifold.operators.projection  import Projection
from manifold.operators.rename      import Rename
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
    # Constructors
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

        # We store key fields in order (tuple)
        self._key = key
        self._key_field_names = self._key.get_field_names()
        self._distinct = distinct
        self._records_by_key = dict()

        # XXX ???
        self.key_list = list()

        #for producer in producers:
        #    data = {
        #    }
        #    self._set_child(producer, data)
        self.add_children(producers)

 
    def add_children(self, producers):
        for producer in producers:
            self._set_child(producer, data=dict())
        self._remaining_children = self._get_num_children()

    def copy(self):
        new_producers = list()
        for _, child, _ in self._iter_children():
            new_producers.append(child.copy())
        return Union(new_producers, self._key, self._distinct)

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

    def send(self, packet):
        """
        Handle an incoming Packet.
        Args:
            packet: A Packet instance.
        """
        # We simply forward the query to all children
        for _, child, _ in self._iter_children():
            child.send(packet.clone())

    def receive_impl(self, packet, slot_id = None):
        record = packet
        #is_last = record.is_last()
        #record.unset_last()
        do_send = True

        if record.is_last():
            self._remaining_children -= 1

            if self._remaining_children == 0:
                # We send all stored records before processing the current one
                for prev_record in self._records_by_key.values():
                    self.forward_upstream(prev_record)
                self.forward_upstream(record)
                return

            record.unset_last()

        # We need to keep all records until the UNION has completed
        # since they might all be completed by records coming from othr
        # children
        # TODO: This might be deduced from the query plan ?

        if record.is_empty() or not record.has_field_names(self._key_field_names):
            self.forward_upstream(packet)
        else:
            key_value = record.get_value(self._key_field_names)

            if key_value in self._records_by_key:
                prev_record = self._records_by_key[key_value]
                for k, v in record.items():
                    if not k in prev_record or not prev_record[k]:
                        prev_record[k] = v
                        continue
                    if isinstance(v, Records):
                        previous[k].extend(v) # DUPLICATES ?
                    elif isinstance(v, list):
                        Log.warning("Should be a record")
                    #else:
                    #    if not v == previous[k]:
                    #        print "W: ignored conflictual field"
                    #    # else: nothing to do
                
                # OLD CODE: 
                # self._records_by_key[key_value].update(record)
            else:
                self._records_by_key[key_value] = record

    #---------------------------------------------------------------------------
    # AST manipulations & optimization
    #---------------------------------------------------------------------------

    @returns(Node)
    def optimize_selection(self, filter):
        # UNION: apply selection to all children
        self._update_children_producers(lambda p, d: p.optimize_selection(filter))
        return self

    def optimize_projection(self, field_names):
        """
        Args:
            field_names: A FieldNames instance.
        """
        # UNION: apply projection to all children
        # in case of UNION with duplicate elimination, we need the key
        do_parent_projection = False
        child_field_names  = FieldNames()
        child_field_names |= field_names
        if self._distinct:
            if self._key.get_field_names() not in field_names: # we are not keeping the key
                do_parent_projection = True
                child_field_names |= FieldNames(self._key.get_field_names())

        self._update_children_producers(lambda p,d : p.optimize_projection(child_field_names))

        if do_parent_projection:
            return Projection(self, field_names)
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

        # We recursively extract LEFT JOIN and RENAME from subquery children

        name = relation.get_name()

        # Investigate the children first, before the parent
        if isinstance(ast, LeftJoin):
            # Update the predicate
            predicate = ast.get_predicate()
            predicate.update_key(lambda k: FieldNames.join(name, k))

            # Get following operator
            new_ast = ast._get_left()
            ast._set_left(self.subquery(new_ast, relation))
            return ast

        elif isinstance(ast, Rename):
            # Update names
            ast.update_aliases(lambda k, v: (FieldNames.join(name, k), FieldNames.join(name, v)))

            # Get following operator
            new_ast = ast._get_child()
            ast._set_child(self.subquery(new_ast, relation))
            return ast

        if not relation.is_local():
            # no need for make (extract is already done)
            return SubQuery._make(self, ast, relation)

        # SQ_new o U  =>  U o SQ_new if SQ_new is local
        self._update_children_producers(lambda p, d: p.subquery(ast.copy(), relation))

        return self
