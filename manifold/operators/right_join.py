#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A RightJoin combines Records collect from its left child
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
from manifold.core.query            import Query, ACTION_CREATE, ACTION_UPDATE, ACTION_GET
from manifold.core.record           import Record
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.operators.selection   import Selection
from manifold.util.predicate        import Predicate, eq
from manifold.util.log              import Log
from manifold.util.type             import returns

#------------------------------------------------------------------
# RIGHT JOIN node
#------------------------------------------------------------------

class RightJoin(Operator, LeftRightSlotMixin):
    """
    RIGHT JOIN operator node
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, predicate, parent_producer, producers):
        """
        Constructor.
        Args:
            left_child:  A Node instance corresponding to left
                operand of the RIGHT JOIN
            right_child: A Node instance corresponding to right
                operand of the RIGHT JOIN
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
        LeftRightSlotMixin.__init__(self)

        self._set_left(parent_producer)
        self._set_right(producers)

        self._predicate = predicate

        self._right_map     = dict() 
        self._right_done    = False
        self._right_packet = None

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this RightJoin Operator.
        """
        return "RIGHT JOIN ON (%s %s %s)" % self._predicate.get_str_tuple()

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
        return dleft.right_join(dright)

    def _update_and_send_left_packet(self):
        """
        # We have all left records

        # NOTE: We used to dynamically change the query plan to
        # filter on the primary key, which is not efficient. since
        # the filter will always go deep down to the FROM node.
        """

        # keys = [2]
        # keys = [(2,)]
        keys = self._right_map.keys()

        # We build the predicate to perform the join
        # uses tuples...
        ########### OLD #### predicate = Predicate(self._predicate.get_key(), included, self._right_map.keys())
        #param = { self._predicate.get_key(): self._right_map.keys()[0]}
        #self._left_packet.update_query(lambda q: q.set(param))

        # Check whether we are propagating a CREATE QUERY
        left_action = Query.from_packet(self._left_packet).get_action()
        # XXX why testing left_action
        if left_action == ACTION_CREATE and self.right_params:
            param_key   = self._predicate.get_key()
            param_value = self._right_map.keys()
            assert len(param_value) == 1 # XXX We can insert one record at a time
            param_value = param_value[0]

            self._left_packet.update_query(lambda q: q.set({param_key: param_value}))

        elif left_action == ACTION_UPDATE:
            param_key   = self._predicate.get_key()
            param_value = self._right_map.keys()
            assert len(param_value) == 1 # XXX We can update one record at a time
            param_value = param_value[0]

            self._left_packet.update_query(lambda q: q.filter_by(param_key, eq, param_value))

        else:
            raise ManifoldInternalException("Unexpected params for '%s' action" % left_action)

        #print "SENDING LEFT PACKET", self._left_packet
        self._get_left().receive(self._left_packet)

    def send_impl(self, packet, slot_id = None):
        """
        Handle an incoming Packet.
        Args:
            packet: A Packet instance.
        """
        # Out of the Query part since it is used for a True Hack !
        right_fields = self._get_right().get_destination().get_field_names()

        if packet.get_protocol() in Packet.PROTOCOL_QUERY_TYPES:
            q = Query.from_packet(packet)
            # We forward the query to the left node
            # TODO : a subquery in fact

            left_key    = self._predicate.get_field_names()
            right_key    = self._predicate.get_value_names()

            left_fields = self._get_left().get_destination().get_field_names()
            right_object = self._get_right().get_destination().get_object()

            right_packet        = packet.clone()
            # split filter and fields
            self.right_params = {k: v for k, v in q.get_params().items() if k in right_fields}
            left_params = {k: v for k, v in q.get_params().items() if k in left_fields}

            # Right members can never be part of an update,
            # otherwise, how to know about the existence of the object (SELECT_OR_CREATE)
            #
            # We will have to find the key to be updated though !!!!
            #if q.get_action == ACTION_CREATE and not right_params:
            right_packet.update_query(lambda q: q.set_action(ACTION_GET))
            
            right_packet.update_query(lambda q: q.set_object(right_object))
            right_packet.update_query(lambda q: q.select(q.get_field_names() & right_fields | right_key, clear = True))
            right_packet.update_query(lambda q: q.filter_by(q.get_filter().split_fields(right_fields, True), clear = True))
            # We need to transform right params into Filters
            #right_packet.update_query(lambda q: q.set(self.right_params, clear = True))
            for k, v in self.right_params.items():
                right_packet.update_query(lambda q: q.filter_by(k, eq, v))

            left_packet = packet.clone()
            # We should rewrite the query...
            left_packet.update_query(lambda q: q.select(q.get_field_names() & left_fields | left_key, clear = True))
            left_packet.update_query(lambda q: q.filter_by(q.get_filter().split_fields(left_fields, True), clear = True))
            left_packet.update_query(lambda q: q.set(left_params, clear = True))
            self._left_packet = left_packet

            #print "SENDING RIGHT PACKET FIRST", right_packet
            self._get_right().receive(right_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_CREATE:
            record = packet

            is_last = record.is_last()
            #if is_last:
            #    record.unset_last()

            #if packet.get_source() == self._producers.get_producer(): # XXX
            if not self._right_done:

                # XXX We need primary key in left record (fk in right record)
                if not record.has_field_names(self._predicate.get_field_names()):
                    Log.warning("Missing RIGHTJOIN predicate %s in left record %r : discarding" % \
                            (self._predicate, record))
                    #self.send(record)

                else:
                    # Store the result in a hash for joining later
                    hash_key = record.get_value(self._predicate.get_key())
                    self._right_map[hash_key] = record

                if is_last:
                    self._right_done = True
                    self._update_and_send_left_packet()
                    return
                

            else:
                # formerly right_callback()

                # Skip records missing information necessary to join
                if not set(self._predicate.get_value_names()) <= set(record.keys()) \
                or record.has_empty_fields(self._predicate.get_value_names()):
                    Log.warning("Missing RIGHTJOIN predicate %s in right record %r: ignored" % \
                            (self._predicate, record))
                    # We send the right record as is.
                    self.forward_upstream(record)
                    return
                
                # We expect to receive information about keys we asked, and only these,
                # so we are confident the key exists in the map
                # XXX Dangers of duplicates ?
                key = record.get_value(self._predicate.get_value())
                #key = record.get_value(self._predicate.get_value_names()) # XXX WHAT IS THE KEY, A SINGLE RECORD OR A LIST LIKE LEFT JOIN ????
                # XXX XXX XXX A SINGLE !!!
                
                # We don't remove the left record since we might have several
                # right records corresponding to it
                right_record = self._right_map.get(key)
                
                record.update(right_record)
                self.forward_upstream(record)

        else: # TYPE_ERROR
            self.forward_upstream(packet)

    @returns(Node)
    def optimize_selection(self, filter):
        # RIGHT JOIN
        # We are pushing selections down as much as possible:
        # - selection on filters on the left: can push down in the left child
        # - selection on filters on the right: cannot push down
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

        left_fields  = self._get_left().get_destination().get_field_names()
        right_fields = self._get_right().get_destination().get_field_names()

        # Classify predicates...
        top_filter, left_filter, right_filter = Filter(), Filter(), Filter()
        for predicate in filter:
            if predicate.get_field_names() <= left_fields:
                left_filter.add(predicate)
            elif predicate.get_field_names() <= right_fields:
                right_filter.add(predicate)
            else:
                top_filter.add(predicate)

        # ... then apply left_ and right_filter...
        if left_filter:
            self._update_left_producer(lambda p, d: p.optimize_selection(left_filter))
        if right_filter:
            self._update_right_producer(lambda p, d: p.optimize_selection(right_filter))

        # ... and top_filter.
        if top_filter:
            # XXX This should never occur
            return Selection(self, top_filter)
        return self

    @returns(Node)
    def optimize_projection(self, fields):
        """
        query:
        fields: the set of fields we want after the projection

        Note: We list all the fields we want every time
        """
        # Ensure we have keys in left and right children
        # After RIGHTJOIN, we might keep the left key, but never keep the right key

        # What are the keys needed in the left (resp. right) table/view
        key_left = self._predicate.get_field_names()
        key_right = self._predicate.get_value_names()

        # Fields requested on the left side = fields requested belonging in left side
        left_fields  = fields & self._get_left().get_destination().get_field_names()
        left_fields |= key_left

        right_fields  = fields & self._get_right().get_destination().get_field_names()
        right_fields |= key_right

        self._update_left_producer(lambda p, d: p.optimize_projection(left_fields))
        self._update_right_producer(lambda p, d: p.optimize_projection(right_fields))

        # Do we need a projection on top (= we do not request the join keys)
        if left_fields | right_fields > fields:
            return Projection(self, fields)
        return self
            
#DEPRECATED|    @returns(Node)
#DEPRECATED|    def reorganize_create(self):
#DEPRECATED|        self._update_left( lambda l: l.reorganize_create())
#DEPRECATED|        self._update_right(lambda r: r.reorganize_create())
#DEPRECATED|        return self
