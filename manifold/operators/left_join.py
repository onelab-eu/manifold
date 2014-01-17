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

from manifold.core.filter           import Filter
from manifold.core.packet           import Packet
from manifold.core.producer         import Producer
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
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

class LeftJoin(Operator):
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
        super(LeftJoin, self).__init__(producers, parent_producer = parent_producer, max_producers = 1, has_parent_producer = True)
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

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def _get_left(self):
        return self._parent_producer

    def _get_right(self):
        return self.get_producer()

    def _update_left(self, function):
        self._parent_producer = function(self._parent_producer)
        Log.warning("If the parent producer changes, must update reciprocal link")

    def _update_right(self, function):
        self.update_producer(function)
        Log.warning("If the parent producer changes, must update reciprocal link")

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(Query)
    def get_destination(self):
        """
        Returns:
            The Query representing AST reprensenting the AST rooted
            at this node.
        """
        dleft  = self._get_left().get_destination()
        dright = self._get_right().get_destination()

        return dleft.left_join(dright)

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

            # True hack... if the fields are the left_fields, it comes from the parent_producer
            # Not robust for incomplete records, but since we expect None values ...
            if set(packet.keys()) == left_fields:
            #if packet.get_source() == self._producers.get_parent_producer(): # XXX
                # formerly left_callback()
                if record.is_last():
                    # We have all left records

                    # NOTE: We used to dynamically change the query plan to
                    # filter on the primary key, which is not efficient. since
                    # the filter will always go deep down to the FROM node.

                    self._left_done = True

                    keys = self._left_map.keys()
                    predicate = Predicate(self._predicate.get_value(), included, self._left_map.keys())
                    self._right_packet.update_query(lambda q: q.filter_by(predicate))

                    self.send(self._right_packet) # XXX
                    return

                if not record.has_fields(self._predicate.get_field_names()):
                    Log.warning("Missing LEFTJOIN predicate %s in left record %r : forwarding" % \
                            (self._predicate, record))
                    self.send(record)
                    return

                # Store the result in a hash for joining later
                hash_key = record.get_value(self._predicate.get_key())
                if not hash_key in self._left_map:
                    self._left_map[hash_key] = []
                self._left_map[hash_key].append(record)

            else:
                # formerly right_callback()

                if record.is_last():
                    # Send records in left_results that have not been joined...
                    for left_record_list in self._left_map.values():
                        for left_record in left_record_list:
                            self.send(left_record)

                    # ... and terminates
                    self.send(record)
                    return

                # Skip records missing information necessary to join
                if not set(self._predicate.get_value()) <= set(record.keys()) \
                or record.has_empty_fields(self._predicate.get_value()):
                    Log.warning("Missing LEFTJOIN predicate %s in right record %r: ignored" % \
                            (self._predicate, record))
                    # XXX Shall we send ICMP ?
                    return
                
                # We expect to receive information about keys we asked, and only these,
                # so we are confident the key exists in the map
                # XXX Dangers of duplicates ?
                key = record.get_value(self._predicate.get_value())
                left_records = self._left_map.pop(key)
                for left_record in left_records:
                    left_record.update(record)
                    self.send(left_record)

        else: # TYPE_ERROR
            self.send(packet)

    @returns(Producer)
    def optimize_selection(self, filter):
        # LEFT JOIN
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

        left_filter = self._get_left().get_destination().get_filter()

        # Classify predicates...
        top_filter, child_filter = Filter(), Filter()
        for predicate in filter:
            if predicate.get_field_names() < left_filter:
                child_filter.add(predicate)
            else:
                top_filter.add(predicate)

        # ... then apply child_filter...
        if child_filter:
            self.update_parent_producer(lambda p: p.optimize_selection(child_filter))

        # ... and top_filter.
        if top_filter:
            return Selection(self, top_filter)
        return self

    @returns(Producer)
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
        left_fields  = fields & self._get_left().get_destination().get_fields()
        left_fields |= key_left

        right_fields  = fields & self._get_right().get_destination().get_fields()
        right_fields |= key_right

        self._update_left( lambda l: l.optimize_projection(left_fields))
        self._update_right(lambda r: r.optimize_projection(right_fields))

        # Do we need a projection on top (= we do not request the join keys)
        if left_fields | right_fields > fields:
            return Projection(self, fields)
        return self
            

    #---------------------------------------------------------------------------
    # Deprecated code 
    #---------------------------------------------------------------------------

#        if isinstance(left_child, list):
#            self.left_done = True
#            for r in left_child:
#                if isinstance(r, dict):
#                    self.left_map[Record.get_value(r, self.predicate.get_key())] = r
#                else:
#                    # r is generally a tuple
#                    self.left_map[r] = Record.from_key_value(self.predicate.get_key(), r)
#        else:
#            old_cb = left_child.get_callback()
#            #Log.tmp("Set left_callback on node ", left_child)
#            left_child.set_callback(self.left_callback)
#            self.set_callback(old_cb)
#
#        #Log.tmp("Set right_callback on node ", right_child)
#        right_child.set_callback(self.right_callback)
#
#        if isinstance(left_child, list):
#            self.query = self.right.get_query().copy()
#            # adding left fields: we know left_child is always a dict, since it
#            # holds more than the key only, since otherwise we would not have
#            # injected but only added a filter.
#            if left_child:
#                self.query.fields |= left_child[0].keys()
#        else:
#            self.query = self.left.get_query().copy()
#            self.query.filters |= self.right.get_query().filters
#            self.query.fields  |= self.right.get_query().fields
#        
#
#        for child in self.get_children():
#            # XXX can we join on filtered lists ? I'm not sure !!!
#            # XXX some asserts needed
#            # XXX NOT WORKING !!!
#            q.filters |= child.filters
#            q.fields  |= child.fields
#
#
#    @returns(list)
#    def get_children(self):
#        return [self.left, self.right]
#
#    @returns(Query)
#    def get_query(self):
#        """
#        \return The query representing AST reprensenting the AST rooted
#            at this node.
#        """
#        print "LeftJoin::get_query()"
#        q = Query(self.get_children()[0])
#        for child in self.get_children():
#            # XXX can we join on filtered lists ? I'm not sure !!!
#            # XXX some asserts needed
#            # XXX NOT WORKING !!!
#            q.filters |= child.filters
#            q.fields  |= child.fields
#        return q
#        
#    #@returns(LeftJoin)
#    def inject(self, records, key, query):
#        """
#        \brief Inject record / record keys into the node
#        \param records A list of dictionaries representing records,
#                       or a list of record keys
#        \returns This node
#        """
#
#        if not records:
#            return
#        record = records[0]
#
#        # Are the records a list of full records, or only record keys
#        is_record = isinstance(record, dict)
#
#        if is_record:
#            records_inj = []
#            for record in records:
#                proj = do_projection(record, self.left.query.fields)
#                records_inj.append(proj)
#            self.left = self.left.inject(records_inj, key, query) # XXX
#            # TODO injection in the right branch: only the subset of fields
#            # of the right branch
#            return self
#
#        # TODO Currently we support injection in the left branch only
#        # Injection makes sense in the right branch if it has the same key
#        # as the left branch.
#        self.left = self.left.inject(records, key, query) # XXX
#        return self
#
#    def start(self):
#        """
#        \brief Propagates a START message through the node
#        """
#        # If the left child is a list of record, we can run the right child
#        # right now. Otherwise, we run the right child once every records
#        # from the left child have been fetched (see left_callback)
#        node = self.right if self.left_done else self.left
#        node.start()

