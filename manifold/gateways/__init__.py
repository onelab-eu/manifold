#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Register every available Gateways.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import json, os, sys, traceback
from types                         import StringTypes

from manifold.core.announce        import Announces
from manifold.core.capabilities    import Capabilities
from manifold.core.node            import Node
from manifold.core.packet          import Packet, ErrorPacket
from manifold.core.producer        import Producer
from manifold.core.query           import Query
from manifold.core.record          import Record
from manifold.core.result_value    import ResultValue
from manifold.operators.projection import Projection
from manifold.operators.selection  import Selection
from manifold.util.log             import Log
from manifold.util.plugin_factory  import PluginFactory
from manifold.util.type            import accepts, returns

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(Producer):
    
    #OBSOLETE|registry = dict() 

    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__gateway_name__'

    #---------------------------------------------------------------------------  
    # Static methods
    #---------------------------------------------------------------------------  

    @staticmethod
    def register_all():
        """
        Register each available Manifold Gateway.
        """
        current_module = sys.modules[__name__]
        PluginFactory.register(current_module)

    #---------------------------------------------------------------------------  
    # Constructor
    #---------------------------------------------------------------------------  

    def __init__(self, interface, platform_name, platform_config = None, *args, **kwargs):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform_name: A String storing name of the Platform related to this
                Gateway or None.
            platform_config: A dictionnary containing the configuration related
                to the Platform managed by this Gateway. In practice, it should
                correspond to the following value stored in the Storage verifying
                
                    SELECT config FROM local:platform WHERE platform == "platform_name"
        """
        if interface:
            Log.warning("Gateway::__init__(): interface obsolete?")
        assert isinstance(platform_name, StringTypes) or not platform_name, \
            "Invalid platform name: %s (%s)" % (platform_name,   type(platform_name))
        assert isinstance(platform_config, dict) or not platform_config, \
            "Invalid configuration: %s (%s)" % (platform_config, type(platform_config))

        Producer.__init__(self, *args, **kwargs)

        self._interface       = interface
        self._platform_name   = platform_name
        self._platform_config = platform_config

        # Both should be loaded at initialization
        self._metadata       = None
        # XXX in the meantime we support all capabilities
        self._capabilities   = dict()

    #---------------------------------------------------------------------------  
    # Accessors
    #---------------------------------------------------------------------------  

    #@returns(Interface)
    def get_interface(self):
        """
        Returns:
            The Interface instance using this Gateway. Most of time
            this is a Router instance.
        """
        Log.warning("Gateway::get_interface(): <mando> obsolete?")
        return self._interface

    @returns(dict)
    def get_config(self):
        """
        Returns:
            A dictionnary containing the configuration related to
                the Platform managed by this Gateway. 
        """
        return self._platform_config

    @returns(StringTypes)
    def get_platform_name(self):
        """
        Returns:
            The String containing the name of the platform related
            to this Gateway.
        """
        return self._platform_name

    @returns(StringTypes)
    def get_gateway_type(self):
        """
        Returns:
            The type of this Gateway (String instance)
        """
        gateway_type = self.__class__.__name__
        if gateway_type.endswith("Gateway"):
            gateway_type = gateway_type[:-7]
        return gateway_type.lower()

    @returns(list)
    def get_metadata(self):
        """
        Build metadata by loading header files
        Returns:
            The list of corresponding Announce instances
        """
        if not self._metadata:
            self._metadata = self.make_metadata()
        return self._metadata

    def get_capabilities(self, method):
        capabilities = self._capabilities.get(method, None)
        return capabilities if capabilities else Capabilities()

    def get_table(self, method):
        table, = [announce.table for announce in self._metadata if announce.table.get_name() == method]
        return table

    def set_consumer(self, consumer):
        Log.warning("what if several pending QP querying this GW")
        Log.warning("we must trigger add when create")
        return self.add_consumer(consumer)

    #---------------------------------------------------------------------------  
    # Internal methods
    #---------------------------------------------------------------------------  

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Gateway.
        """
        return "%s [%s]" % (self.get_platform_name(), self.get_gateway_type())

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Gateway.
        """
        return "FROM <Gateway %s [%s]>" % (self.get_platform_name(), self.get_gateway_type())

#DEPRECATED|    def send(self, record, callback, identifier = None):
#DEPRECATED|        """
#DEPRECATED|        Calls the parent callback with the record passed in parameter
#DEPRECATED|        In other word, this From Node send Record to its parent Node in the QueryPlan.
#DEPRECATED|        Args:
#DEPRECATED|            record: A Record (dictionnary) sent by this Gateway.
#DEPRECATED|            callback: The function called to send this record. This callback is provided
#DEPRECATED|                most of time by a From Node.
#DEPRECATED|                Prototype :
#DEPRECATED|
#DEPRECATED|                    @returns(list) # see manifold.util.callback::get_results()
#DEPRECATED|                    @accepts(dict)
#DEPRECATED|                    def callback(record)
#DEPRECATED|
#DEPRECATED|            identifier: An integer identifying the From Node which is fetching
#DEPRECATED|                this Record. This parameter is only needed for debug purpose.
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(record, Record), "Invalid Record %s (%s)" % (record, type(record))
#DEPRECATED|
#DEPRECATED|        if identifier:
#DEPRECATED|            Log.record("[#%04d] [ %r ]" % (identifier, record))
#DEPRECATED|        else:
#DEPRECATED|            Log.record("[ %r ]" % record)
#DEPRECATED|        callback(record)



    #---------------------------------------------------------------------------  
    # Methods
    #---------------------------------------------------------------------------  

    def receive(self, packet):
        """
        Handle a QUERY Packet from a Consumer. 
        This method should be overloaded by its child class(es).
        Args:
            packet: A QUERY Packet.
        """
        # Check type of the incoming Packet
        Producer.receive(self, packet)

    def dump(self, indent = 0):
        Node.dump(self, indent)

    @returns(list)
    def make_metadata(self):
        return Announces.from_dot_h(self.get_platform_name(), self.get_gateway_type())

    # TODO clean this method and plug it in Router::forward()
    @staticmethod
    @returns(dict)
    def get_variables(user, account_config):
        """
        ???
        Args:
            user: A dictionnary corresponding to the User.
            account_config: A dictionnary corresponding to the user's Account config.
        Returns:
            The corresponding dictionnary.
        """
        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        variables = dict() 
        # Authenticated user
        variables["user_email"] = user["email"]
        if user:
            for k, v in user["config"].items():
                if isinstance(v, StringTypes) and not "credential" in v:
                    variables[k] = v
        # Account information of the authenticated user
        for k, v in account_config.items():
            if isinstance(v, StringTypes) and not "credential" in v:
                variables[k] = v
        return variables

#DEPRECATED|    # TODO clean this method and plug it in Router::forward()
#DEPRECATED|    @staticmethod
#DEPRECATED|    def start(user, account_config, query):
#DEPRECATED|        """
#DEPRECATED|        ???
#DEPRECATED|        Args:
#DEPRECATED|            user: A User instance.
#DEPRECATED|            account_config: A dictionnary.
#DEPRECATED|        Returns:
#DEPRECATED|            The corresponding dictionnary.
#DEPRECATED|        """
#DEPRECATED|        Log.tmp("I'm maybe obsolete")
#DEPRECATED|        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
#DEPRECATED|        try:
#DEPRECATED|            # Replaces variables in the Query (predicate in filters and parameters)
#DEPRECATED|            filter = query.get_where()
#DEPRECATED|            params = query.get_params()
#DEPRECATED|            variables = Gateway.get_variables(user, account_config)
#DEPRECATED|
#DEPRECATED|            for predicate in filter:
#DEPRECATED|                value = predicate.get_value()
#DEPRECATED|
#DEPRECATED|                if isinstance(value, (tuple, list)):
#DEPRECATED|                    Log.warning("Ignoring tuple/list value %s (not yet implemented)" % (value,))
#DEPRECATED|                    continue
#DEPRECATED|
#DEPRECATED|                if value and isinstance(value, StringTypes) and value[0] == '$':
#DEPRECATED|                    var = value[1:]
#DEPRECATED|                    if var in variables:
#DEPRECATED|                        predicate.set_value(variables[var])
#DEPRECATED|
#DEPRECATED|            for key, value in params.items():
#DEPRECATED|                # XXX variable support not implemented for lists and tuples
#DEPRECATED|                if isinstance(value, (tuple, list)):
#DEPRECATED|                    continue
#DEPRECATED|
#DEPRECATED|                if value and isinstance(value, StringTypes) and value[0] == '$':
#DEPRECATED|                    var = value[1:]
#DEPRECATED|                    if var in variables and isinstance(variables[var], StringTypes):
#DEPRECATED|                        params[k] = variables[var]
#DEPRECATED|        except Exception, e:
#DEPRECATED|            import traceback
#DEPRECATED|            Log.warning("Exception in start", e)
#DEPRECATED|            traceback.print_exc()

    @returns(list)
    def get_result_value(self):
        """
        Retrieve the fetched records
        Returns:
            A list of ResultValue instances corresponding to the fetched records
        """
        return self.result_value
        
    def check_forward(self, query, annotation, receiver):
        """
        Checks Gateway::forward parameters.
        """
        assert isinstance(query, Query), \
            "Invalid Query: %s (%s)" % (query, type(query))
        assert isinstance(annotation, dict), \
            "Invalid Query: %s (%s)" % (annotation, type(Annotation))
        assert not receiver or receiver.set_result_value, \
            "Invalid receiver: %s (%s)" % (receiver, type(receiver))

    def forward(self, query, annotation, receiver = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            annotation: A dictionnary instance containing Query's annotation.
            receiver : A Receiver instance which collects the results of the Query.
        """
        if receiver: receiver.set_result_value(None)
        self.check_forward(query, annotation, receiver)

    @returns(list)
    def query_storage(self, query):
        """
        Run a Query on the Manifold's Storage.
        Args:
            query: A Query instance.
        Returns:
            A list of dictionnaries corresponding to each fetched Records.
        """
        return self.get_interface().execute_local_query(query)

#DEPRECATED|    def success(self, receiver, query):
#DEPRECATED|        """
#DEPRECATED|        Shorthand method that must be called by a Gateway if its forward method succeeds.
#DEPRECATED|        Args:
#DEPRECATED|            receiver: A Receiver instance or a From Node.
#DEPRECATED|            query: A Query instance:
#DEPRECATED|        """
#DEPRECATED|        if receiver:
#DEPRECATED|            assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
#DEPRECATED|            result_value = ResultValue(
#DEPRECATED|                origin = (ResultValue.GATEWAY, self.__class__.__name__, self.get_platform_name(), query),
#DEPRECATED|                type   = ResultValue.SUCCESS, 
#DEPRECATED|                code   = ResultValue.SUCCESS,
#DEPRECATED|                value  = None 
#DEPRECATED|            )
#DEPRECATED|            receiver.set_result_value(result_value)

    def error(self, query, description = ""):
        self.send(ErrorPacket())
#DEPRECATED|        """
#DEPRECATED|        Shorthand method that must be called by a Gateway if its forward method fails.
#DEPRECATED|        Args:
#DEPRECATED|            receiver: A Receiver instance or a From Node.
#DEPRECATED|            query: A Query instance:
#DEPRECATED|        """
#DEPRECATED|        Log.error("triggered due to the following query:\n%(sep)s\n%(query)s\n%(sep)s\n%(traceback)s%(sep)s\n(%(description)s)" % {
#DEPRECATED|            "sep"         : "-" * 80,
#DEPRECATED|            "query"       : query,
#DEPRECATED|            "description" : description,
#DEPRECATED|            "traceback"   : traceback.format_exc()
#DEPRECATED|        })
#DEPRECATED|        if receiver:
#DEPRECATED|            assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
#DEPRECATED|            receiver.set_result_value(
#DEPRECATED|                ResultValue(
#DEPRECATED|                    origin      = (ResultValue.GATEWAY, self.__class__.__name__, self.get_platform_name(), query),
#DEPRECATED|                    type        = ResultValue.ERROR,
#DEPRECATED|                    code        = ResultValue.ERROR,
#DEPRECATED|                    description = description, 
#DEPRECATED|                    traceback   = traceback.format_exc()
#DEPRECATED|                )
#DEPRECATED|            )


    @returns(Node)
    def optimize_selection(self, query, filter):
        """
        Propagate a WHERE clause through a FROM Node.
        Args:
            filter: A Filter instance. 
        Returns:
            The updated root Node of the sub-AST.
        """
        # XXX Simplifications
#FIXME|        for predicate in filter:
#FIXME|            if predicate.get_field_names() == self.key.get_field_names() and predicate.has_empty_value():
#FIXME|                # The result of the request is empty, no need to instanciate any gateway
#FIXME|                # Replace current node by an empty node
#FIXME|                return FromTable(query, [], self.key)
#FIXME|            # XXX Note that such issues could be detected beforehand

        Log.tmp("optimize_selection: query = %s filter = %s" % (query, filter))
        capabilities = self.get_capabilities(query.get_from())

        if capabilities.selection:
            # Push filters into the From node
            query.filter_by(filter)
            #old for predicate in filter:
            #old    self.query.filters.add(predicate)
            return self
        else:
            # Create a new Selection node
            selection = Selection(self, filter)

            # XXX fullquery ?
            if capabilities.fullquery:
                # We also push the filter down into the node
                for p in filter:
                    query.filters.add(p)

            return selection

    @returns(Node)
    def optimize_projection(self, query, fields):
        """
        Propagate a SELECT clause through a FROM Node.
        Args:
            fields: A set of String instances (queried fields).
        Returns:
            The updated root Node of the sub-AST.
        """
        Log.tmp("optimize_projection: query = %s fields = %s" % (query, fields))
        capabilities = self.get_capabilities(query.get_from())
        if capabilities.projection:
            # Push fields into the From node
            self.query.select().select(fields)
            return self
        else:
            provided_fields = self.get_table(query.get_from()).get_field_names()

            # Test whether this From node can return every queried Fields.
            if fields - provided_fields:
                Log.warning("From::optimize_projection: some requested fields (%s) are not provided by {%s} From node. Available fields are: {%s}" % (
                    ', '.join(list(fields - provided_fields)),
                    query.get_from(),
                    ', '.join(list(provided_fields))
                )) 

            # If this From node returns more Fields than those explicitely queried
            # (because the projection capability is not enabled), create an additional
            # Projection Node above this From Node in order to guarantee that
            # we only return queried fields
            if provided_fields - fields:
                # XXX fullquery ?
                return Projection(self, fields)
                #projection.query = self.query.copy().filter_by(filter) # XXX
            return self
