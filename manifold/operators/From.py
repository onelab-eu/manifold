#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A QueryPlan manages an AST where each leaves are a From
# of a FromTable Operators.
#
# A From Node wraps a dummy Query having SELECT and WHERE
# clause homogenous to the query issued by the user. 
# From Nodes are useful until having optimized the QueryPlan.
# Then, each Operator using a From Node is connected to a Socket
# plugged on the appropriate Gateway.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import traceback
from types                          import StringTypes

from manifold.core.annotation       import Annotation
from manifold.core.capabilities     import Capabilities
from manifold.core.destination      import Destination
from manifold.core.key              import Key
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.gateways              import Gateway
from manifold.operators.from_table  import FromTable
from manifold.operators.operator    import Operator 
from manifold.operators.projection  import Projection
from manifold.operators.selection   import Selection 
from manifold.util.log              import Log
from manifold.util.type             import returns

class From(Operator):
    """
    From Operators are responsible to forward Packets between the AST
    and the involved Gateways Nodes. It is mostly used during the
    AST optimization phase during which we need to support get_query()
    for each Operator of the AST.
    """

    def __init__(self, gateway, query, capabilities, key):
        """
        Constructor.
        Args:
            query: A Query instance querying a Table provided by this Platform.
            annotation: An Annotation instance.
            capabilities: A Capabilities instance, set according to the metadata related
                to the Table queried by this From Node.
            key: A Key instance. 
        """
        assert isinstance(gateway, Gateway),\
            "Invalid gateway = %s (%s)" % (gateway, type(gateway))
        assert isinstance(query, Query),\
            "Invalid query = %s (%s)" % (query, type(query))
        assert isinstance(capabilities, Capabilities),\
            "Invalid capabilities = %s (%s)" % (capabilities, type(capabilities))
        assert isinstance(key, Key),\
            "Invalid key = %s (%s)" % (key, type(key))

        self._query       = query
        self.capabilities = capabilities
        self.key          = key
        self._gateway     = gateway

        # The producer will be set once a QUERY will be received
        super(From, self).__init__(max_producers = 1)


#DEPRECATED|    def add_fields_to_query(self, field_names):
#DEPRECATED|        """
#DEPRECATED|        Add field names (list of String) to the SELECT clause of the embedded query
#DEPRECATED|        Args:
#DEPRECATED|            field_names: A list of Strings corresponding to field names present in
#DEPRECATED|                the table wrapped by this From Node.
#DEPRECATED|        """
#DEPRECATED|        for field_name in field_names:
#DEPRECATED|            assert isinstance(field_name, StringTypes), "Invalid field_name = %r in field_names = %r" % (field_name, field_names)
#DEPRECATED|        self.query.fields = frozenset(set(self.query.fields) | set(field_names))

    @returns(Gateway)
    def get_gateway(self):
        """
        Returns:
            The Gateway instance used to query the Platform wrapped
            by this FROM node.
        """
        return self._gateway

    @returns(StringTypes)
    def get_platform_name(self):
        """
        Returns:
            The name of the Platform queried by this FROM node.
        """
        return self.get_gateway().get_platform_name()

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The name of the platform queried by this FROM node.
        """
        #return "%s" % self.get_query().to_sql(platform = self.get_platform_name())
        try:
            return "FROM %s:%s (%s)" % (
                self.get_platform_name(),
                self.get_query().get_from(),
                self.get_query().to_sql(platform = self.get_platform_name())
            )
        except Exception, e:
            print "Exception in repr: %s" % e

#DEPRECATED|    #@returns(From)
#DEPRECATED|    def inject(self, records, key, query):
#DEPRECATED|        """
#DEPRECATED|        Inject record / record keys into the node
#DEPRECATED|        Args:
#DEPRECATED|            records: A list of dictionaries representing records,
#DEPRECATED|                     or list of record keys
#DEPRECATED|        Returns:
#DEPRECATED|            ???
#DEPRECATED|        """
#DEPRECATED|        if not records:
#DEPRECATED|            return
#DEPRECATED|        record = records[0]
#DEPRECATED|
#DEPRECATED|        # Are the records a list of full records, or only record keys
#DEPRECATED|        is_record = isinstance(record, dict)
#DEPRECATED|
#DEPRECATED|        # If the injection does not provides all needed fields, we need to
#DEPRECATED|        # request them and join
#DEPRECATED|        provided_fields = set(record.keys()) if is_record else set([key])
#DEPRECATED|        needed_fields = self.query.fields
#DEPRECATED|        missing_fields = needed_fields - provided_fields
#DEPRECATED|
#DEPRECATED|        old_self_callback = self.get_callback()
#DEPRECATED|
#DEPRECATED|        if not missing_fields:
#DEPRECATED|            from_table = FromTable(self.query, records, key)
#DEPRECATED|            from_table.set_callback(old_self_callback)
#DEPRECATED|            return from_table
#DEPRECATED|
#DEPRECATED|        # If the inject only provide keys, add a WHERE, otherwise a WHERE+JOIN
#DEPRECATED|        if not is_record or provided_fields < set(records[0].keys()):
#DEPRECATED|            # We will filter the query by inserting a where on 
#DEPRECATED|            list_of_keys = records.keys() if is_record else records
#DEPRECATED|            predicate = Predicate(key, included, list_of_keys)
#DEPRECATED|            where = Selection(self, Filter().filter_by(predicate))
#DEPRECATED|            where.query = self.query.copy().filter_by(predicate)
#DEPRECATED|            where.set_callback(old_self_callback)
#DEPRECATED|            # XXX need reoptimization
#DEPRECATED|            return where
#DEPRECATED|        #else:
#DEPRECATED|        #    print "From::inject() - INJECTING RECORDS"
#DEPRECATED|
#DEPRECATED|        missing_fields.add(key) # |= key
#DEPRECATED|        self.query.fields = missing_fields
#DEPRECATED|
#DEPRECATED|        #parent_query = self.query.copy()
#DEPRECATED|        #parent_query.fields = provided_fields
#DEPRECATED|        #parent_from = FromTable(parent_query, records, key)
#DEPRECATED|
#DEPRECATED|        join = LeftJoin(records, self, Predicate(key, '==', key))
#DEPRECATED|        join.set_callback(old_self_callback)
#DEPRECATED|
#DEPRECATED|        return join

    @returns(Query)
    def get_query(self):
        """
        Returns:
            The Query nested in this From instance.
        """
        return self._query

    @returns(Capabilities)
    def get_capabilities(self):
        """
        Returns:
            The Capabilities nested in this From instance.
        """
        return self.capabilities

    @returns(bool)
    def has_children_with_fullquery(self):
        """
        Returns:
            True iif this Operator or at least one of its child uses 
            fullquery Capabilities.
        """
        return self.get_capabilities().fullquery

    @returns(Destination)
    def get_destination(self):
        """
        Returns:
            The Destination corresponding to this Operator. 
        """
        q = self.get_query()

        return Destination(q.get_from(), q.get_where(), q.get_select())

    def receive_impl(self, packet):
        """
        Process an incoming packet.
        Args:
            packet: A Packet instance.
        """
        #Log.debug("[FROM] packet=", packet)
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            # Register this flow in the Gateway
            self.get_gateway().add_flow(packet.get_query(), self)
            packet.set_receiver(self)
            self.get_gateway().receive(packet)
        else: #if packet.get_protocol() == Packet.PROTOCOL_RECORD:
            self.send(packet)
            
    
#    def receive_impl(self, packet):
#        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
#            query = packet.get_query()
#            from_select = self.get_query().get_select()
#            from_where = self.get_where()
#            if query.get_select() < from_select:
#                query.fields = from_select
#            query.filter_by(from_where)
#        self.send(packet)

    @returns(Operator)
    def optimize_selection(self, filter):
        """
        Propagate a WHERE clause through this From instance. 
        Args:
            filter: A Filter instance. 
        Returns:
            The updated root Node of the sub-AST.
        """
        # XXX Simplifications
        for predicate in filter:
            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
                # The result of the request is empty, no need to instanciate any gateway
                # Replace current node by an empty node
                consumers = self.get_consumers()
                self.clear_consumers() # XXX We must only unlink consumers involved in the QueryPlan that we're optimizing
                from_table = FromTable(self.get_query(), list(), self.key)
                for consumer in consumers:
                    consumer.add_producer(from_table)
                Log.warning("From: optimize_selection: empty table")
                return from_table

        if self.get_capabilities().selection:
            # Push filters into the From node
            self._query.filter_by(filter)
            #old for predicate in filter:
            #old    self.query.filters.add(predicate)
            return self
        elif self.get_capabilities().join:
            key_filter, remaining_filter = filter.split_fields(self.key.get_field_names())

            if key_filter:
                # We push only parts related to the key (unless fullquery)
                if self.get_capabilities().fullquery:
                    self._query.filter_by(filter)
                else:
                    self._query.filter_by(key_filter)

            if remaining_filter:
                # FROM (= self) becomes the new producer for Selection
                selection = Selection(self, remaining_filter)
                selection.set_producer(self)
                return selection

            else:
                return self

        else:
            # XXX New rework just like the previous elif XXX

            # Create a new Selection node
            consumers = self.get_consumers()
            self.clear_consumers() # XXX We must only unlink consumers involved in the QueryPlan that we're optimizing and create a new From node 
            selection = Selection(self, filter)
            #selection.query = self.query.copy().filter_by(filter)
            selection.set_consumers(consumers)

            if self.get_capabilities().fullquery:
                # We also push the filter down into the node
                for predicate in filter:
                    self._query.filters.add(predicate)

            return selection

    @returns(Operator)
    def optimize_projection(self, fields):
        """
        Propagate a SELECT clause through this From instance. 
        Args:
            fields: A set of String instances (queried fields).
        Returns:
            The updated root Node of the sub-AST.
        """
        if self.get_capabilities().projection:
            # Push fields into the From node
            self._query.select().select(fields)
            return self
        else:
            # Provided fields is set to None if it corresponds to SELECT *
            provided_fields = self.get_query().get_select()

            # Test whether this From node can return every queried Fields.
            if provided_fields and fields - provided_fields:
                Log.warning("From::optimize_projection: some requested fields (%s) are not provided by {%s} From node. Available fields are: {%s}" % (
                    ', '.join(list(fields - provided_fields)),
                    self.get_query().get_from(),
                    ', '.join(list(provided_fields))
                )) 

            # If this From node returns more Fields than those explicitely queried
            # (because the projection capability is not enabled), create an additional
            # Projection Node above this From Node in order to guarantee that
            # we only return queried fields
            if not provided_fields or provided_fields - fields:
                return Projection(self, fields)
                #projection.query = self.query.copy().filter_by(filter) # XXX
            return self

    @returns(Operator)
    def reorganize_create(self):
        return self
