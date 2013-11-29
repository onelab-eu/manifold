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
#DEPRECATED|
#DEPRECATED|import traceback
#DEPRECATED|from types                         import StringTypes
#DEPRECATED|
#DEPRECATED|from manifold.core.annotation      import Annotation
#DEPRECATED|from manifold.core.query           import Query
#DEPRECATED|from manifold.core.receiver        import Receiver
#DEPRECATED|from manifold.core.result_value    import ResultValue 
#DEPRECATED|from manifold.operators            import Node
#DEPRECATED|from manifold.operators.selection  import Selection   # XXX
#DEPRECATED|from manifold.operators.projection import Projection  # XXX
#DEPRECATED|from manifold.operators.from_table import FromTable
#DEPRECATED|from manifold.operators.projection import Projection
#DEPRECATED|from manifold.operators.selection  import Selection
#DEPRECATED|from manifold.util.log             import Log
#DEPRECATED|from manifold.util.type            import returns
#DEPRECATED|
#DEPRECATED|class From(Node, Receiver):
#DEPRECATED|    """
#DEPRECATED|    From Node are responsible to forward a Query and its corresponding User
#DEPRECATED|    to a Gateway.
#DEPRECATED|    """
#DEPRECATED|
#DEPRECATED|    def __init__(self, platform_name, query, capabilities, key):
#DEPRECATED|        """
#DEPRECATED|        Constructor.
#DEPRECATED|        Args:
#DEPRECATED|            platform_name: A String instance storing the name of the Platform queried by
#DEPRECATED|                this From Node.
#DEPRECATED|            query: A Query instance querying a Table provided by this Platform.
#DEPRECATED|            annotation: An Annotation instance.
#DEPRECATED|            capabilities: A Capabilities instance, set according to the metadata related
#DEPRECATED|                to the Table queried by this From Node.
#DEPRECATED|            key: A Key instance. 
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(platform_name, StringTypes),\
#DEPRECATED|            "Invalid platform_name = %s (%s)" % (platform_name, type(platform_name))
#DEPRECATED|        assert isinstance(query, Query), \
#DEPRECATED|            "Invalid type: query = %s (%s)" % (query, type(query))
#DEPRECATED|
#DEPRECATED|        super(From, self).__init__()
#DEPRECATED|
#DEPRECATED|        self.platform_name = platform_name
#DEPRECATED|        self.query         = query
#DEPRECATED|        Log.warning("capabilities still useful in From ?")
#DEPRECATED|        self.capabilities  = capabilities
#DEPRECATED|        self.key           = key
#DEPRECATED|        self.gateway       = None # This Gateway will be initalized by init_from_node
#DEPRECATED|
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
#DEPRECATED|
#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def get_platform_name(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The name of the Platform queried by this FROM node.
#DEPRECATED|        """
#DEPRECATED|        return self.platform_name
#DEPRECATED|
#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def __repr__(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The name of the platform queried by this FROM node.
#DEPRECATED|        """
#DEPRECATED|        return "%s" % self.get_query().to_sql(platform = self.get_platform_name())
#DEPRECATED|
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
#DEPRECATED|    @returns(Node)
#DEPRECATED|    def optimize_selection(self, filter):
#DEPRECATED|        """
#DEPRECATED|        Propagate a WHERE clause through a FROM Node.
#DEPRECATED|        Args:
#DEPRECATED|            filter: A Filter instance. 
#DEPRECATED|        Returns:
#DEPRECATED|            The updated root Node of the sub-AST.
#DEPRECATED|        """
#DEPRECATED|        # XXX Simplifications
#DEPRECATED|        for predicate in filter:
#DEPRECATED|            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
#DEPRECATED|                # The result of the request is empty, no need to instanciate any gateway
#DEPRECATED|                # Replace current node by an empty node
#DEPRECATED|                old_self_callback = self.get_callback()
#DEPRECATED|                from_table = FromTable(self.query, list(), self.key)
#DEPRECATED|                from_table.set_callback(old_self_callback)
#DEPRECATED|                return from_table
#DEPRECATED|            # XXX Note that such issues could be detected beforehand
#DEPRECATED|
#DEPRECATED|        if self.capabilities.selection:
#DEPRECATED|            # Push filters into the From node
#DEPRECATED|            self.query.filter_by(filter)
#DEPRECATED|            #old for predicate in filter:
#DEPRECATED|            #old    self.query.filters.add(predicate)
#DEPRECATED|            return self
#DEPRECATED|        else:
#DEPRECATED|            # Create a new Selection node
#DEPRECATED|            old_self_callback = self.get_callback()
#DEPRECATED|            selection = Selection(self, filter)
#DEPRECATED|            #selection.query = self.query.copy().filter_by(filter)
#DEPRECATED|            selection.set_callback(old_self_callback)
#DEPRECATED|
#DEPRECATED|            if self.capabilities.fullquery:
#DEPRECATED|                # We also push the filter down into the node
#DEPRECATED|                for p in filter:
#DEPRECATED|                    self.query.filters.add(p)
#DEPRECATED|
#DEPRECATED|            return selection
#DEPRECATED|
#DEPRECATED|    @returns(Node)
#DEPRECATED|    def optimize_projection(self, fields):
#DEPRECATED|        """
#DEPRECATED|        Propagate a SELECT clause through a FROM Node.
#DEPRECATED|        Args:
#DEPRECATED|            fields: A set of String instances (queried fields).
#DEPRECATED|        Returns:
#DEPRECATED|            The updated root Node of the sub-AST.
#DEPRECATED|        """
#DEPRECATED|        if self.capabilities.projection:
#DEPRECATED|            # Push fields into the From node
#DEPRECATED|            self.query.select().select(fields)
#DEPRECATED|            return self
#DEPRECATED|        else:
#DEPRECATED|            provided_fields = self.get_query().get_select()
#DEPRECATED|
#DEPRECATED|            # Test whether this From node can return every queried Fields.
#DEPRECATED|            if fields - provided_fields:
#DEPRECATED|                Log.warning("From::optimize_projection: some requested fields (%s) are not provided by {%s} From node. Available fields are: {%s}" % (
#DEPRECATED|                    ', '.join(list(fields - provided_fields)),
#DEPRECATED|                    self.get_query().get_from(),
#DEPRECATED|                    ', '.join(list(provided_fields))
#DEPRECATED|                )) 
#DEPRECATED|
#DEPRECATED|            # If this From node returns more Fields than those explicitely queried
#DEPRECATED|            # (because the projection capability is not enabled), create an additional
#DEPRECATED|            # Projection Node above this From Node in order to guarantee that
#DEPRECATED|            # we only return queried fields
#DEPRECATED|            if provided_fields - fields:
#DEPRECATED|                return Projection(self, fields)
#DEPRECATED|                #projection.query = self.query.copy().filter_by(filter) # XXX
#DEPRECATED|            return self
