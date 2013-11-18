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
from types                        import StringTypes

from manifold.core.announce       import Announces
from manifold.core.packet         import ErrorPacket
from manifold.core.producer       import Producer
from manifold.core.query          import Query
from manifold.core.record         import Record
from manifold.core.result_value   import ResultValue
from manifold.util.log            import Log
from manifold.util.plugin_factory import PluginFactory
from manifold.util.type           import accepts, returns

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(Producer):
    
    registry = dict() 

    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__gateway_name__'


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
        assert isinstance(platform_name, StringTypes) or not platform_name, \
            "Invalid platform name: %s (%s)" % (platform_name,   type(platform_name))
        assert isinstance(platform_config, dict) or not platform_config, \
            "Invalid configuration: %s (%s)" % (platform_config, type(platform_config))

        Producer.__init__(self, *args, **kwargs)

        self.interface       = interface
        self.platform_name   = platform_name
        self.platform_config = platform_config

    #@returns(Interface)
    def get_interface(self):
        """
        Returns:
            The Interface instance using this Gateway. Most of time
            this is a Router instance.
        """
        return self.interface

    @returns(dict)
    def get_config(self):
        """
        Returns:
            A dictionnary containing the configuration related to
                the Platform managed by this Gateway. 
        """
        return self.platform_config

    @returns(StringTypes)
    def get_platform_name(self):
        """
        Returns:
            The String containing the name of the platform related
            to this Gateway.
        """
        return self.platform_name

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
        return Announces.from_dot_h(self.get_platform_name(), self.get_gateway_type())

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Gateway.
        """
        return "Platform<%s %s>" % (self.get_platform_name(), self.get_gateway_type())

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Gateway.
        """
        return self.__str__()

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

    # TODO clean this method and plug it in Router::forward()
    @staticmethod
    @returns(dict)
    def get_variables(user, account_config):
        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        variables = dict() 
        # Authenticated user
        variables["user_email"] = user["email"]
        for k, v in user.get_config().items():
            if isinstance(v, StringTypes) and not "credential" in v:
                variables[k] = v
        # Account information of the authenticated user
        for k, v in account_config.items():
            if isinstance(v, StringTypes) and not "credential" in v:
                variables[k] = v
        return variables

    # TODO clean this method and plug it in Router::forward()
    @staticmethod
    def start(user, account_config, query):
        """
        ???
        Args:
            user: A User instance.
            account_config: A dictionnary.
        Returns:
            The corresponding dictionnary.
        """
        Log.tmp("I'm maybe obsolete")
        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        try:
            # Replaces variables in the Query (predicate in filters and parameters)
            filter = query.get_where()
            params = query.get_params()
            variables = Gateway.get_variables(user, account_config)

            for predicate in filter:
                value = predicate.get_value()

                if isinstance(value, (tuple, list)):
                    Log.warning("Ignoring tuple/list value %s (not yet implemented)" % (value,))
                    continue

                if value and isinstance(value, StringTypes) and value[0] == '$':
                    var = value[1:]
                    if var in variables:
                        predicate.set_value(variables[var])

            for key, value in params.items():
                # XXX variable support not implemented for lists and tuples
                if isinstance(value, (tuple, list)):
                    continue

                if value and isinstance(value, StringTypes) and value[0] == '$':
                    var = value[1:]
                    if var in variables and isinstance(variables[var], StringTypes):
                        params[k] = variables[var]
        except Exception, e:
            import traceback
            Log.warning("Exception in start", e)
            traceback.print_exc()

    @returns(list)
    def get_result_value(self):
        """
        Retrieve the fetched records
        Returns:
            A list of ResultValue instances corresponding to the fetched records
        """
        return self.result_value
        
    def check_forward(self, query, annotations, callback, is_deferred, execute, account_config, receiver):
        """
        Checks Gateway::forward parameters.
        """
        assert isinstance(query, Query), \
            "Invalid Query: %s (%s)" % (query, type(query))
        assert isinstance(is_deferred, bool), \
            "Invalid execute value: %s (%s)" % (is_deferred, type(is_deferred))
        assert isinstance(execute, bool), \
            "Invalid is_deferred value: %s (%s)" % (execute, type(execute))
        assert not account_config or isinstance(account_config, dict), \
            "Invalid account_config: %s (%s)" % (account_config, type(account_config))
        assert not receiver or receiver.set_result_value, \
            "Invalid receiver: %s (%s)" % (receiver, type(receiver))

    def forward(self, query, annotations, callback, is_deferred = False, execute = True, account_config = None, receiver = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean set to True if this Query is async.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            account_config: A dictionnary containing the user's account config.
                In pratice, this is the result of the following query (run on the Storage)
                SELECT config FROM local:account WHERE user_id == user.user_id
            format: A String specifying in which format the Records must be returned.
            receiver : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        Log.warning("Remove 'callback' parameter which is receiver.get_callback()")
        Log.warning("Remove 'format' parameter which is receiver.get_callback()")
        if receiver: receiver.set_result_value(None)
        self.check_forward(query, annotations, callback, is_deferred, execute, account_config, receiver)

    @returns(list)
    def query_storage(self, query, user):
        """
        Run a Query on the Manifold's Storage.
        Args:
            query: A Query instance.
            user: A dictionnary describing the User issuing this Query.
        Returns:
            A list of dictionnaries corresponding to each fetched Records.
        """
        assert isinstance(query, Query) 
        assert query.get_from().startswith("local:"), "Invalid Query, it must query local:* (%s)"

        receiver = Receiver()
        router = self.get_interface()
        return router.execute_local_query(query)

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

    def receive(self, packet):
        # formerly forward()
        Producer.receive(self, packet)
        # Mostly implemented in children


def register_gateways():
    current_module = sys.modules[__name__]
    PluginFactory.register(current_module)

__all__ = ['Gateway']

