#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A LeftJoin combines Records collect from its left child
# and its right child and combine them.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

from manifold.core.destination      import Destination
from manifold.core.filter           import Filter
from manifold.core.node             import Node
from manifold.core.operator_slot    import LeftRightSlotMixin
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.operators.right_join  import RightJoin
from manifold.operators.selection   import Selection
from manifold.util.predicate        import Predicate, eq, included
from manifold.util.log              import Log
from manifold.util.type             import returns

# XXX No more support for list as a child
# XXX Manage callbacks
# XXX Manage query 
# XXX Do we still need inject ?

#------------------------------------------------------------------
# LEFT JOIN node
#------------------------------------------------------------------

class LeftJoin(Operator, LeftRightSlotMixin):
    """
    LEFT JOIN operator node
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, predicate, parent_producer, producers):
        """
        Constructor.
        Args:
            left_child:  A Node instance corresponding to left
                operand of the LEFT JOIN
            right_child: A Node instance corresponding to right
                operand of the LEFT JOIN
            predicate: A Predicate instance invoked to determine
                whether two records of left_child and right_child
                can be joined.
        """

        # Check parameters
        assert isinstance(predicate, Predicate), "Invalid predicate = %r (%r)" % (predicate, type(predicate))
        assert predicate.op == eq
        # In fact predicate is always : object.key, ==, VALUE

        # Initialization
        Operator.__init__(self)
        LeftRightSlots.__init__(self)

        self._set_left(parent_producer)
        self._set_right(producers)

        self._predicate = predicate

        self._left_map     = dict() 
        self._left_done    = False
        self._right_packet = None

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this LeftJoin Operator.
        """
        return "LEFT JOIN ON (%s %s %s)" % self._predicate.get_str_tuple()

#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|    # Helpers
#DEPRECATED|    #---------------------------------------------------------------------------
#DEPRECATED|
#DEPRECATED|    def _get_left(self):
#DEPRECATED|        return self._parent_producer
#DEPRECATED|
#DEPRECATED|    def _get_right(self):
#DEPRECATED|        return self.get_producer()
#DEPRECATED|
#DEPRECATED|    def _update_left(self, function):
#DEPRECATED|        return self.update_parent_producer(function)
#DEPRECATED|
#DEPRECATED|    def _update_right(self, function):
#DEPRECATED|        return self.update_producer(function)

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(Destination)
    def get_destination(self):
        """
        Returns:
            The Destination corresponding to this Operator. 
        """
        dleft  = self._get_left().get_destination()
        dright = self._get_right().get_destination()
        return dleft.left_join(dright)

    def _update_and_send_right_packet(self):
        """
        # We have all left records

        # NOTE: We used to dynamically change the query plan to
        # filter on the primary key, which is not efficient. since
        # the filter will always go deep down to the FROM node.
        """

        # keys = [2]
        # keys = [(2,)]
        keys = self._left_map.keys()

        # We build the predicate to perform the join
        predicate = Predicate(tuple(self._predicate.get_value_names()), included, self._left_map.keys())
        self._right_packet.update_query(lambda q: q.filter_by(predicate))

        self.send(self._right_packet) # XXX

    def receive_impl(self, packet):
        """
        Handle an incoming Packet.
        Args:
            packet: A Packet instance.
        """
        # Out of the Query part since it is used for a True Hack !
        left_fields = self._get_left().get_destination().get_fields()

        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            q = packet.get_query()
            # We forward the query to the left node
            # TODO : a subquery in fact

            left_key    = self._predicate.get_field_names()
            right_key    = self._predicate.get_value_names()

            right_fields = self._get_right().get_destination().get_fields()
            right_object = self._get_right().get_destination().get_object()

            left_packet        = packet.clone()
            # split filter and fields
            left_packet.update_query(lambda q: q.select(q.get_fields() & left_fields | left_key, clear = True))
            left_packet.update_query(lambda q: q.filter_by(q.get_filter().split_fields(left_fields, True), clear = True))

            #import sys
            #sys.exit(0)
            right_packet = packet.clone()
            # We should rewrite the query...
            right_packet.update_query(lambda q: q.set_object(right_object))
            right_packet.update_query(lambda q: q.select(q.get_fields() & right_fields | right_key, clear = True))
            right_packet.update_query(lambda q: q.filter_by(q.get_filter().split_fields(right_fields, True), clear = True))
            self._right_packet = right_packet

            self.send_parent(left_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            record = packet

            is_last = record.is_last()
            if is_last:
                record.unset_last()

            #if packet.get_source() == self._producers.get_parent_producer(): # XXX
            if not self._left_done:

                if not record.is_empty():
                    if not record.has_fields(self._predicate.get_field_names()):
                        Log.warning("Missing LEFTJOIN predicate %s in left record %r : forwarding" % \
                                (self._predicate, record))
                        self.send(record)

                    else:
                        # Store the result in a hash for joining later
                        hash_key = record.get_value(self._predicate.get_field_names())
                        if not hash_key in self._left_map:
                            self._left_map[hash_key] = []
                        self._left_map[hash_key].append(record)

                if is_last:
                    self._left_done = True
                    self._update_and_send_right_packet()
                    return
                

            else:
                # formerly right_callback()

                if not record.is_empty():
                    # Skip records missing information necessary to join
                    if not self._predicate.get_value_names() <= set(record.keys()) \
                    or record.has_empty_fields(self._predicate.get_value_names()):
                        Log.warning("Missing LEFTJOIN predicate %s in right record %r: ignored" % \
                                (self._predicate, record))
                        # XXX Shall we send ICMP ?
                        return

                    # We expect to receive information about keys we asked, and only these,
                    # so we are confident the key exists in the map
                    # XXX Dangers of duplicates ?
                    key = record.get_value(self._predicate.get_value_names())
                    left_records = self._left_map.pop(key)

                    for left_record in left_records:
                        left_record.update(record)
                    
                        self.send(left_record)

                if is_last:
                    # Unmapped records
                    if self._left_map:
                        # Send records in left_results that have not been joined
                        for left_record_list in self._left_map.values():
                            for left_record in left_record_list:
                                self.send(left_record)
                    # LAST MARK
                    self.send(Record(last = True))
                    


        else: # TYPE_ERROR
            self.send(packet)

    #---------------------------------------------------------------------------
    # AST manipulations & optimization
    #---------------------------------------------------------------------------

    @returns(Node)
    def optimize_selection(self, filter):
        # LEFT JOIN
        # We are pushing selections down as much as possible:
        # - selection on filters on the left: can push down in the left child
        # - selection on filters on the right: cannot push down: in fact right
        # join is possible
        # - selection on filters on the key / common fields ??? TODO
        # 
        #                                        +------- ...
        #                                       /
        #                    +---+    +---+    /
        #  FILTER -->    ----| ? |----| ⨝ |--< 
        #                    +---+    +---+    \
        #                                       +---+
        #                 top_filter            | ? |---- ...
        #                                       +---+
        #                                    child_filter == parent_producer (sic.)
        #

        left_fields  = self._get_left().get_destination().get_fields()
        right_fields = self._get_right().get_destination().get_fields()

        # We do go for a right_join ?
        # - no filters on left child => YES
        # - in case of insert => YES
        # - on join ? 
        # - filters on PK are more efficients => YES ?
        # - note that we might do a partial right join (example: where user_id = # 3 and platform = ple
        # Maybe as soon as we have filters on the right member since we can keep
        # filters on the left also

        top_filter, left_filter, right_filter = Filter(), Filter(), Filter()

        right_join = False
        if right_filter and not left_filter:
            right_join = True

        # Classify predicates...
        for predicate in filter:
            if predicate.get_field_names() < left_fields:
                left_filter.add(predicate)
                if predicate.get_field_names() < right_fields:
                    right_filter.add(predicate)
            elif right_join and predicate.get_field_names() < right_fields:
                right_filter.add(predicate)
            else:
                top_filter.add(predicate)

        # ... then apply left_ and right_filter...
        if left_filter:
            self._update_left_producer(lambda p: p.optimize_selection(left_filter))
        if right_filter:
            self._update_right_producer(lambda p: p.optimize_selection(right_filter))

        if right_join:
            # We need to be sure to have the same producers...
            # consumers should be handled by the return new_self
            left_producer   = self.get_left()
            right_producer  = self.get_right()
            self._clear()
            new_self = RightJoin(self._predicate, left_producer, right_producer)
        else:
            new_self = self

        # ... and top_filter.
        if top_filter:
            return Selection(new_self, top_filter)
        return new_self

    @returns(Node)
    def optimize_projection(self, fields):
        """
        query:
        fields: the set of fields we want after the projection

        Note: We list all the fields we want every time
        """
        # Ensure we have keys in left and right children
        # After LEFTJOIN, we might keep the left key, but never keep the right key

        # What are the keys needed in the left (resp. right) table/view
        key_left = self._predicate.get_field_names()
        key_right = self._predicate.get_value_names()

        # Fields requested on the left side = fields requested belonging in left side
        # XXX faux on perd les champs nécessaires au join bien plus haut
        left_fields  = fields & self._get_left().get_destination().get_fields()
        left_fields |= key_left

        right_fields  = fields & self._get_right().get_destination().get_fields()
        right_fields |= key_right

        self._update_left_producer( lambda l: l.optimize_projection(left_fields))
        self._update_right_producer(lambda r: r.optimize_projection(right_fields))

        # Do we need a projection on top (= we do not request the join keys)
        if left_fields | right_fields > fields:
            return Projection(self, fields)
        return self

    @returns(Node)
    def reorganize_create(self):
        # Transform into a Right Join
        # XXX we need to delete it !!!
        left_producer   = self.get_left().reorganize_create()
        right_producer  = self.get_right().reorganize_create()
        self._clear()
        return RightJoin(self._predicate, left_producer, right_producer)

