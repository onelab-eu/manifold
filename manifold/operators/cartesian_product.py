#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# CartesianProduct performs a cartesian product between
# the both tables resulting of its child Nodes' RECORD
# Packets and produces the corresponding RECORD Packets.
#
# See also:
#   http://en.wikipedia.org/wiki/Cartesian_product
#
# Remark:
#   Until we figure out how to realize a streaming
#   cartesian product, we are doing a blocking version that
#   waits for all child Nodes.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from itertools                   import product, imap
from types                       import StringTypes

from manifold.core.filter        import Filter
from manifold.operators.operator import Operator
from manifold.operators          import ChildCallback, ChildStatus
from manifold.util.log           import Log
from manifold.util.predicate     import Predicate, eq
from manifold.util.type          import returns

DUMPSTR_CARTESIANPRODUCT = "CPRODUCT"

#------------------------------------------------------------------
# CartesianProduct Operator 
#------------------------------------------------------------------

class CartesianProduct(Operator):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, children_ast_relation_list, query = None):
        """
        Args:
            children_ast_relation_list: A list of pair made
                of a Query and a Relation instance
                Example:
                [
                    (
                        SELECT * AT now FROM tdmi:destination,
                        <LINK_11, destination>
                    ),
                    (
                        SELECT * AT now FROM tdmi:agent,
                        <LINK_11, agent>
                    )
                ]
            query: A Query instance
        """

        super(CartesianProduct, self).__init__()

        assert len(children_ast_relation_list) >= 2, \
            "Cannot create a CartesianProduct from %d table (2 or more required): %s" % (
                len(children_ast_relation_list), children_ast_relation_list
            )

        # Note we cannot guess the query object, so pass it
        # fields = [relation.get_relation_name(r) for r in relation]
        # NTOE: such a query should not contain any action
        self.query = query
        self.children, self.relations = [], []
        for _ast, _relation in children_ast_relation_list:
            self.children.append(_ast)
            self.relations.append(_relation)

        self.child_results = []
        self.status = ChildStatus(self._all_done)

        # Set up callbacks & prepare array for storing results from children:
        # parent result can only be propagated once all children have replied
        for i, child in enumerate(self.children):
            child.set_callback(ChildCallback(self, i))
            self.child_results.append([])

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this CartesianProduct instance.
        """
        return DUMPSTR_CARTESIANPRODUCT

    #---------------------------------------------------------------------------
    # Private methods
    #---------------------------------------------------------------------------

    def _all_done(self):
        """
        Called when all children of the current cross product are done

        Example:
        SQ agents = [{'agent_id': X1, 'agent_dummy': Y1}, {'agent_id': X2, 'agent_dummy': Y2}]
            p = ('agent', eq, 'agent_id')
        SQ dests  = [{'dest_id' : X1, 'dest_dummy' : Y1}, {'dest_id' : X2, 'dest_dummy' : Y2}]
            p = ('dest', eq, 'dest_id')
        X = [{'agent': X1, 'dest' : X1}, {'agent': X1, {'dest' : X2}, ...]
        """
        keys = set()
        for relation in self.relations:
            keys |= relation.get_predicate().get_value_names()

        def merge(dics):
            return { k: v for dic in dics for k,v in dic.items() if k in keys }

        records = imap(lambda x: merge(x), product(*self.child_results))
        records[-1].set_last()
        
        map(lambda x: self.send(x), records)

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive_impl(self, packet):
        """
        Process an incoming Packet instance.
        Args:
            packet: A Packet instance.
        """
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            # formerly start()
            raise Exception, "CartesianProduct::receive(QUERY) Not implemented"

            # We would need to do all the tracking here instead of init(), a bit
            # like cwnd

            # XXX We need to send a query to all producers
            # - parent_query
            # - child_query
            # - relation

            # Start all children
            for i, child in enumerate(self.children):
                self.status.started(i)
            for i, child in enumerate(self.children):
                child.start()

        elif packet.get_protocol() == Packet.PROTOCOL_CREATE:
            # formerly child_callback()

            # XXX child_id & source ?
            record = packet.get_record()
            if record.is_last():
                self.status.completed(child_id)
                return
            self.child_results[child_id].append(record)

        else: # TYPE_ERROR
            self.send(packet)

    def optimize_selection(self, query, filter):
        for i, child in enumerate(self.children):
            child_fields = child.get_query().get_select()
            child_filter = Filter()
            for predicate in filter:
                if predicate.get_field_names() <= child_fields:
                    child_filter.add(predicate)
            if child_filter:
               self.children[i] = child.optimize_selection(query, child_filter) 
        return self

    def optimize_projection(self, query, fields):
        for i, child in enumerate(self.children):
            child_fields = child.get_query().get_select()
            if not child_fields <= fields:
               self.children[i] = child.optimize_projection(query, child_fields & fields)
        return self
