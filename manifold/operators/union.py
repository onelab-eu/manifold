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
        self._key      = key
        self._key_fields = self._key.get_field_names()
        self._distinct = distinct
        self._records_by_key = dict()

        # XXX ???
        self.key_list = list()

        for producer in producers:
            data = {
            }
            self._set_child(producer, data)
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
                # We need to keep all records until the UNION has completed
                # since they might all be completed by records coming from othr
                # children
                # TODO: This might be deduced from the query plan ?

                #if self._key.get_field_names() and record.has_fields(self._key.get_field_names()):
                key_value = record.get_value(self._key_fields)


                if key_value in self._records_by_key:
                    prev_record = self._records_by_key[key_value]
                    for k, v in record.items():
                        if not k in prev_record:
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

#DEPRECATED|                # Ignore duplicate records
#DEPRECATED|                if self._distinct:
#DEPRECATED|                    key = self._key.get_field_names()
#DEPRECATED|                    if key and record.has_fields(key):
#DEPRECATED|                        key_value = record.get_value(key)
#DEPRECATED|                        if key_value in self.key_list:
#DEPRECATED|                            do_send = False
#DEPRECATED|                        else:
#DEPRECATED|                            self.key_list.append(key_value)
#DEPRECATED|
#DEPRECATED|                record.unset_last()
#DEPRECATED|                if do_send:
#DEPRECATED|                    self.forward_upstream(record)

            if is_last:
                # In fact we don't care to know which child has completed
                self._remaining_children -= 1
                if self._remaining_children == 0:
                    # We need to send all stored records
                    for record in self._records_by_key.values():
                        self.forward_upstream(record)
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
            if set(self._key.get_field_names()) not in fields: # we are not keeping the key
                do_parent_projection = True
                child_fields |= self._key.get_field_names()

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

        # We recursively extract LEFT JOIN and RENAME from subquery children

        name = relation.get_name()

        # Investigate the children first, before the parent
        if isinstance(ast, LeftJoin):
            # Update the predicate
            predicate = ast.get_predicate()
            predicate.update_key(lambda k: Fields.join(name, k))

            # Get following operator
            new_ast = ast._get_left()
            ast._set_left(self.subquery(new_ast, relation))
            return ast

        elif isinstance(ast, Rename):
            # Update names
            ast.update_aliases(lambda k, v: (Fields.join(name, k), Fields.join(name, v)))

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

