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
from manifold.core.annotation     import Annotation
from manifold.core.query          import Query
from manifold.core.record         import Record
from manifold.core.receiver       import Receiver
from manifold.core.result_value   import ResultValue
from manifold.util.log            import Log
from manifold.util.plugin_factory import PluginFactory
from manifold.util.type           import accepts, returns

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(object):
    
    #OBSOLETE|registry = dict() 

    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__gateway_name__'

    def __init__(self, interface, platform_name, platform_config = None):
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

    def send(self, record, receiver, identifier = None):
        """
        Calls the parent callback with the record passed in parameter
        In other word, this From Node send Record to its parent Node in the QueryPlan.
        Args:
            record: A Record (dictionnary) sent by this Gateway.
            receiver: A Receiver instance.
            identifier: An integer identifying the From Node which is fetching
                this Record. This parameter is only needed for debug purpose.
        """
        assert isinstance(record,   Record),   "Invalid Record %s (%s)"   % (record,   type(record))
        assert isinstance(receiver, Receiver), "Invalid Receiver %s (%s)" % (receiver, type(receiver))

        if identifier:
            Log.record("[#%04d] [ %r ]" % (identifier, record))
        else:
            Log.record("[ %r ]" % record)
        receiver.callback(record)

    # TODO clean this method and plug it in Router::forward()
    @staticmethod
    @returns(dict)
    def get_variables(user, account_config):
        """
        Args:
            user: A dictionnary corresponding to the User.
            account_config: A dictionnary correspoding to the user's Account config
        """
        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        variables = dict() 
        # Authenticated user
        variables["user_email"] = user["email"]
        for k, v in user["config"].items():
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
            user: A dictionnary corresponding to the User.
            account_config: A dictionnary correspoding to the user's Account config
        Returns:
            The corresponding dictionnary.
        """
        Log.tmp("I'm maybe obsolete")
        assert isinstance(user, dict),           "Invalid user : %s (%s)"           % (user, type(user))
        assert isinstance(account_config, dict), "Invalid account_config : %s (%s)" % (account_config, type(account_config))

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

    def success(self, receiver, query):
        """
        Shorthand method that must be called by a Gateway if its forward method succeeds.
        Args:
            receiver: A Receiver instance or a From Node.
            query: A Query instance:
        """
        if receiver:
            assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
            if not receiver.get_result_value():
                result_value = ResultValue(
                    origin = (ResultValue.GATEWAY, self.__class__.__name__, self.get_platform_name(), query),
                    type   = ResultValue.SUCCESS, 
                    code   = ResultValue.SUCCESS,
                    value  = None 
                )
                receiver.set_result_value(result_value)

    def error(self, receiver, query, description = ""):
        """
        Shorthand method that must be called by a Gateway if its forward method fails.
        Args:
            receiver: A Receiver instance or a From Node.
            query: A Query instance:
        """
        Log.error("triggered due to the following query:\n%(sep)s\n%(query)s\n%(sep)s\n%(traceback)s%(sep)s\n(%(description)s)" % {
            "sep"         : "-" * 80,
            "query"       : query,
            "description" : description,
            "traceback"   : traceback.format_exc()
        })
        if receiver:
            assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
            receiver.set_result_value(
                ResultValue(
                    origin      = (ResultValue.GATEWAY, self.__class__.__name__, self.get_platform_name(), query),
                    type        = ResultValue.ERROR,
                    code        = ResultValue.ERROR,
                    description = description, 
                    traceback   = traceback.format_exc()
                )
            )

def register_gateways():
    current_module = sys.modules[__name__]
    PluginFactory.register(current_module)

__all__ = ['Gateway']

