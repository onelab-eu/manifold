#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A From Node wraps a Query performed by a User and sent
# to a given Platform through a Gateway. It allows to
# fetch Records from those Plaforms.
#
# A QueryPlan is an AST where each leaves are a From
# of a FromTable node. Those records traverse the branches
# of this tree until reaching the root Node or being
# filtered.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import traceback
from types                         import StringTypes

from manifold.core.annotation      import Annotation
from manifold.core.capabilities    import Capabilities
from manifold.core.key             import Key
from manifold.core.query           import Query
from manifold.operators.operator   import Operator 
from manifold.operators.from_table import FromTable
from manifold.operators.projection import Projection #XXX
from manifold.util.log             import Log
from manifold.util.type            import returns

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
        assert isinstance(query, Query),\
            "Invalid query = %s (%s)" % (query, type(query))
        assert isinstance(capabilities, Capabilities),\
            "Invalid capabilities = %s (%s)" % (capabilities, type(capabilities))
        assert isinstance(key, Key),\
            "Invalid key = %s (%s)" % (key, type(key))

        super(From, self).__init__(
            producers     = [gateway],
            max_producers = 1
        )

        self.query        = query
        self.capabilities = capabilities
        self.key          = key

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

    @returns(StringTypes)
    def get_platform_name(self):
        """
        Returns:
            The name of the Platform queried by this FROM node.
        """
        gateway = self.get_producer()
        return gateway.get_platform_name()

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The name of the platform queried by this FROM node.
        """
        #return "%s" % self.get_query().to_sql(platform = self.get_platform_name())
        return "FROM %s:%s" % (self.get_platform_name(), self.get_query().get_from())

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
#DEPRECATED|
#DEPRECATED|    def set_annotation(self, annotation):
#DEPRECATED|        """
#DEPRECATED|        Args:
#DEPRECATED|            annotation: An Annotation instance.
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(annotation, Annotation), "Invalid annotation = %s (%s)" % (annotation, type(annotation))
#DEPRECATED|        self.annotation = annotation
#DEPRECATED|
#DEPRECATED|    @returns(Annotation)
#DEPRECATED|    def get_annotation(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The Annotation instance related to this From Node and corresponding
#DEPRECATED|            to the Query of this Node.
#DEPRECATED|        """
#DEPRECATED|        return self.annotation
#DEPRECATED|
#DEPRECATED|    @returns(dict)
#DEPRECATED|    def get_user(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            A dictionnary containing information related to the User querying
#DEPRECATED|            the nested platform of this From Node (None if anonymous).
#DEPRECATED|        """
#DEPRECATED|        Log.warning("From::get_user is deprecated: %s" % traceback.format_exc())
#DEPRECATED|        annotation = self.get_annotation()
#DEPRECATED|        return annotation["user"] if annotation else None
#DEPRECATED|
#DEPRECATED|    @returns(dict)
#DEPRECATED|    def get_account_config(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            A dictionnary containing the account configuration of the User instanciating
#DEPRECATED|            this From Node or None.
#DEPRECATED|        """
#DEPRECATED|        Log.warning("From::get_account_config is deprecated: %s" % traceback.format_exc())
#DEPRECATED|        annotation = self.get_annotation()
#DEPRECATED|        return annotation["account_config"] if annotation else None
#DEPRECATED|
#DEPRECATED|    def start(self):
#DEPRECATED|        """
#DEPRECATED|        Propagates a START message through the node
#DEPRECATED|        """
#DEPRECATED|        if not self.gateway:
#DEPRECATED|            Log.error("No Gateway set for this From Node: %r" % self)
#DEPRECATED|            self.send(LastRecord())
#DEPRECATED|        else:
#DEPRECATED|            self.gateway.forward(
#DEPRECATED|                self.get_query(),      # Forward the nested Query to the nested Gateway.
#DEPRECATED|                self.get_annotation(), # Forward corresponding Annotation to the nested Gateway.
#DEPRECATED|                self                   # From Node acts as a Receiver. 
#DEPRECATED|            )
#DEPRECATED|
#DEPRECATED|    def set_gateway(self, gateway):
#DEPRECATED|        """
#DEPRECATED|        Associate this From Node to a given Gateway.
#DEPRECATED|        Args:
#DEPRECATED|            gateway: A instance which inherits Gateway.
#DEPRECATED|        """
#DEPRECATED|        self.gateway = gateway
#DEPRECATED|
#DEPRECATED|    def set_callback(self, callback):
#DEPRECATED|        """
#DEPRECATED|        Set the callback of this From Node to return fetched Records.
#DEPRECATED|        Args:
#DEPRECATED|            callback: A function called back whenever a Record is fetched.
#DEPRECATED|        """
#DEPRECATED|        super(From, self).set_callback(callback)
#DEPRECATED|
    @returns(Query)
    def get_query(self):
        """
        Returns:
            The Query nested in this From instance.
        """
        return self.query

    def receive(self, packet):
        self.send(packet)
        
    @returns(Operator)
    def optimize_selection(self, query, filter):
        """
        Propagate a WHERE clause through this From instance. 
        Args:
            filter: A Filter instance. 
        Returns:
            The updated root Node of the sub-AST.
        """
        Log.warning("From::optimize_selection(): not yet implemented")
        return self

        # XXX Simplifications
        for predicate in filter:
            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
                # The result of the request is empty, no need to instanciate any gateway
                # Replace current node by an empty node
                old_self_callback = self.get_callback()
                from_table = FromTable(self.query, list(), self.key)
                from_table.set_callback(old_self_callback)
                Log.warning("From: optimize_selection: empty table")
                return from_table

        if self.capabilities.selection:
            # Push filters into the From node
            self.query.filter_by(filter)
            #old for predicate in filter:
            #old    self.query.filters.add(predicate)
            return self
        else:
            # Create a new Selection node
            old_self_callback = self.get_callback()
            selection = Selection(self, filter)
            #selection.query = self.query.copy().filter_by(filter)
            selection.set_callback(old_self_callback)

            if self.capabilities.fullquery:
                # We also push the filter down into the node
                for predicate in filter:
                    self.query.filters.add(predicate)

            return selection

    @returns(Operator)
    def optimize_projection(self, query, fields):
        """
        Propagate a SELECT clause through this From instance. 
        Args:
            fields: A set of String instances (queried fields).
            query: A Query instance.
        Returns:
            The updated root Node of the sub-AST.
        """
        if self.capabilities.projection:
            # Push fields into the From node
            self.query.select().select(fields)
            return self
        else:
            provided_fields = self.get_query().get_select()

            # Test whether this From node can return every queried Fields.
            if fields - provided_fields:
                Log.warning("From::optimize_projection: some requested fields (%s) are not provided by {%s} From node. Available fields are: {%s}" % (
                    ', '.join(list(fields - provided_fields)),
                    self.get_query().get_from(),
                    ', '.join(list(provided_fields))
                )) 

            # If this From node returns more Fields than those explicitely queried
            # (because the projection capability is not enabled), create an additional
            # Projection Node above this From Node in order to guarantee that
            # we only return queried fields
            if provided_fields - fields:
                return Projection(self, fields)
                #projection.query = self.query.copy().filter_by(filter) # XXX
            return self
