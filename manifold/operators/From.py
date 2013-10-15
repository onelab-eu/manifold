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

from types                         import StringTypes

from manifold.core.query           import Query
from manifold.core.result_value    import ResultValue 
from manifold.gateways.gateway     import Gateway
from manifold.operators            import Node, LAST_RECORD
from manifold.operators.from_table import FromTable
from manifold.operators.projection import Projection
from manifold.operators.selection  import Selection
from manifold.util.log             import Log
from manifold.util.type            import returns

#------------------------------------------------------------------
# FROM node
#------------------------------------------------------------------

class From(Node):
    """
    From Node are responsible to forward a Query and its corresponding User
    to a Gateway.
    """

    def __init__(self, platform, query, capabilities, key):
        """
        Constructor.
        Args:
            platform: A String instance storing the name of the platform queried by
                this From Node.
            query: A Query instance querying a Table provided by this Platform.
            capabilities: A Capabilities instance, set according to the metadate related
                to the Table queried by this From Node.
            key: A Key instance. 
        """
        assert isinstance(query, Query), "Invalid type: query = %r (%r)" % (query, type(query))

        super(From, self).__init__()

        self.platform       = platform
        self.account_config = None # This dictionnary will be initalized by init_from_node
        self.query          = query
        self.capabilities   = capabilities
        self.key            = key
        self.gateway        = None # This Gateway will be initalized by init_from_node
        self.result_value   = None # A ResultValue (TODO: From should inherits Receiver)

    def set_account_config(self, account_config):
        """
        Args:
            account_config: A dictionnary instance
        """
        assert not account_config or isinstance(account_config, dict), \
            "Invalid account_config: %s (%s)" % (account_config, type(account_config))
        self.account_config = account_config

    def add_fields_to_query(self, field_names):
        """
        Add field names (list of String) to the SELECT clause of the embedded query
        Args:
            field_names: A list of Strings corresponding to field names present in
                the table wrapped by this From Node.
        """
        for field_name in field_names:
            assert isinstance(field_name, StringTypes), "Invalid field_name = %r in field_names = %r" % (field_name, field_names)
        self.query.fields = frozenset(set(self.query.fields) | set(field_names))

    @returns(StringTypes)
    def get_platform_name(self):
        """
        Returns:
            The name of the Platform queried by this FROM node.
        """
        return self.platform

    @returns(ResultValue)
    def get_result_value(self):
        """
        (TODO) remove this method and make From inherits Receiver
        Returns:
            The ResultValue related to the query embeded in this From Node.
            It could be None if the query has not been run (see From::forward())
        """
        return self.result_value

    def set_result_value(self, result_value):
        """
        (TODO) remove this method and make From inherits Receiver
        Args:
            result_value: The ResultValue related to the query embeded in this From Node.
        """
        assert not result_value or isinstance(result_value, ResultValue), "Invalid ResultValue: %s (%s)" % (result_value, type(result_value))
        assert not isinstance(self.result_value, ResultValue), "self.result_value has been already set! %s" % self.get_result_value()
        self.result_value = result_value

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The name of the platform queried by this FROM node.
        """
        return "%s" % self.get_query().to_sql(platform = self.get_platform_name())

    #@returns(From)
    def inject(self, records, key, query):
        """
        Inject record / record keys into the node
        Args:
            records: A list of dictionaries representing records,
                     or list of record keys
        Returns:
            ???
        """
        if not records:
            return
        record = records[0]

        # Are the records a list of full records, or only record keys
        is_record = isinstance(record, dict)

        # If the injection does not provides all needed fields, we need to
        # request them and join
        provided_fields = set(record.keys()) if is_record else set([key])
        needed_fields = self.query.fields
        missing_fields = needed_fields - provided_fields

        old_self_callback = self.get_callback()

        if not missing_fields:
            from_table = FromTable(self.query, records, key)
            from_table.set_callback(old_self_callback)
            return from_table

        # If the inject only provide keys, add a WHERE, otherwise a WHERE+JOIN
        if not is_record or provided_fields < set(records[0].keys()):
            # We will filter the query by inserting a where on 
            list_of_keys = records.keys() if is_record else records
            predicate = Predicate(key, included, list_of_keys)
            where = Selection(self, Filter().filter_by(predicate))
            where.query = self.query.copy().filter_by(predicate)
            where.set_callback(old_self_callback)
            # XXX need reoptimization
            return where
        #else:
        #    print "From::inject() - INJECTING RECORDS"

        missing_fields.add(key) # |= key
        self.query.fields = missing_fields

        #parent_query = self.query.copy()
        #parent_query.fields = provided_fields
        #parent_from = FromTable(parent_query, records, key)

        join = LeftJoin(records, self, Predicate(key, '==', key))
        join.set_callback(old_self_callback)

        return join

#MANDO|    def start(self):
#MANDO|        """
#MANDO|        \brief Propagates a START message through the node
#MANDO|        """
#MANDO|        # @loic Added self.send(LAST_RECORD) if no Gateway is selected, then send no result
#MANDO|        # That might mean that the user has no account for the platform
#MANDO|        if not self.gateway:
#MANDO|            self.send(LAST_RECORD)
#MANDO|            #raise Exception, "Cannot call start on a From class, expecting Gateway"
#MANDO|        else:
#MANDO|            # Initialize the query embeded by the Gateway using the one deduced from the QueryPlan
#MANDO|            self.gateway.set_query(self.get_query())
#MANDO|            self.gateway.start()

    def set_user(self, user):
        """
        Set the User querying the nested platform of this From Node.
        Args:
            user: A User instance (None if anonymous).
        """
        self.user = user

    #@returns(User)
    def get_user(self):
        """
        Returns:
            The User querying the nested platform of this From Node (None if anonymous).
        """
        return self.user

    @returns(dict)
    def get_account_config(self):
        """
        Returns:
            A dictionnary containing the account configuration of the User instanciating
            this From Node or None.
        """
        return self.account_config

    def start(self):
        """
        Propagates a START message through the node
        """
        if not self.gateway:
            Log.error("No Gateway set for this From Node: %r" % self)
            self.send(LAST_RECORD)
        else:
            self.gateway.forward(
                self.get_query(),          # Forward the nested Query to the nested Gateway.
                self.get_callback(),       # Records are returned through this Callback.
                False,                     # is_deferred
                True,                      # execute
                self.get_user(),           # The Query is performed using this User.
                self.get_account_config(), # Account configuration of this User on this Platform
                "dict",                    # Node only consider Records in the "dict" format
                self                       # From Node mimics a Receiver. 
            )
        #Log.tmp("uncomment this assert")
        #assert self.get_result_value() and isinstance(self.get_result_value(), ResultValue), \
        #    "%s::forward() does not set instance: %s (%s)" % (
        #        self.gateway.__class__.__name__,
        #        self.get_result_value(),
        #        type(self.get_result_value())
        #    )

    def set_gateway(self, gateway):
        """
        Associate this From Node to a given Gateway.
        Args:
            gateway: A instance which inherits Gateway.
        """
        self.gateway = gateway

    def set_callback(self, callback):
        """
        Set the callback of this From Node to return fetched Records.
        Args:
            callback: A function called back whenever a Record is fetched.
        """
        super(From, self).set_callback(callback)

    def optimize_selection(self, filter):
        # XXX Simplifications
        for predicate in filter:
            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
                # The result of the request is empty, no need to instanciate any gateway
                # Replace current node by an empty node
                old_self_callback = self.get_callback()
                from_table = FromTable(self.query, [], self.key)
                from_table.set_callback(old_self_callback)
                return from_table
            # XXX Note that such issues could be detected beforehand

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
                for p in filter:
                    self.query.filters.add(p)

            return selection

    def optimize_projection(self, fields):
        """
        Propagate a SELECT clause through a FROM Node.
        Args:
            fields: A set of String instances (queried fields).
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
