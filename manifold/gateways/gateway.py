#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class inherited by each Manifold Gateway 
#
# A Gateway handles Manifold's query and translate them to query the
# underlying source of information (for instance a database, a CSV
# file, a Web Service, etc.). Once the result is retrieved, the
# Gateway translates each "record" in a python dictionnary having one key
# per queried field and its corresponding value. At least the Gateway
# send LAST_RECORD (None) to indicates that the whole set of "records"
# has been returned.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import traceback
from types                              import StringTypes

from manifold.core.announce             import Announces
from manifold.core.query                import Query 
from manifold.core.receiver             import Receiver 
from manifold.core.result_value         import ResultValue
from manifold.util.plugin_factory       import PluginFactory
from manifold.util.type                 import accepts, returns
from manifold.util.log                  import Log

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(object):
    
    __metaclass__ = PluginFactory

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

    def send(self, record, callback, identifier = None):
        """
        Calls the parent callback with the record passed in parameter
        In other word, this From Node send Record to its parent Node in the QueryPlan.
        Args:
            record: A Record (dictionnary) sent by this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype :

                    @returns(list) # see manifold.util.callback::get_results()
                    @accepts(dict)
                    def callback(record)

            identifier: An integer identifying the From Node which is fetching
                this Record. This parameter is only needed for debug purpose.
        """
        if identifier:
            Log.record("[#%04d] [ %r ]" % (identifier, record))
        else:
            Log.record("[ %r ]" % record)
        callback(record)

    # TODO clean this method and plug it in Router::forward()
    @staticmethod
    @returns(dict)
    def get_variables(user, account_config):
        """
        Merge user and account information in a single dictionnary.
        Args:
            user: A User instance.
            account_config: A dictionnary.
        Returns:
            The corresponding dictionnary.
        """
        #assert isinstance(user, User), "Invalid user : %s (%s)" % (user, type(user))
        variables = {}
        # Authenticated user
        variables["user_email"] = user.email
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

                if value[0] == "$":
                    var = value[1:]
                    if var in variables:
                        predicate.set_value(variables[var])

            for key, value in params.items():
                # XXX variable support not implemented for lists and tuples
                if isinstance(value, (tuple, list)):
                    continue

                if value[0] == "$":
                    var = value[1:]
                    if var in variables and isinstance(variables[var], StringTypes):
                        params[k] = variables[var]
        except Exception, e:
            import traceback
            Log.warning("Exception in start", e)
            traceback.print_exc()

    def check_forward(self, query, callback, is_deferred, execute, user, account_config, format, receiver):
        """
        Checks Gateway::forward parameters.
        """
        assert isinstance(query, Query), \
            "Invalid Query: %s (%s)" % (query, type(query))
        assert isinstance(is_deferred, bool), \
            "Invalid execute value: %s (%s)" % (is_deferred, type(is_deferred))
        assert isinstance(execute, bool), \
            "Invalid is_deferred value: %s (%s)" % (execute, type(execute))
        #assert not user or isinstance(user, User), \
        #    "Invalid User: %s (%s)" % (user, type(user))
        assert not account_config or isinstance(account_config, dict), \
            "Invalid account_config: %s (%s)" % (account_config, type(account_config))
        assert format in ["dict", "object"], \
            "Invalid format: %s (%s)" % (format, type(format))
        assert not receiver or receiver.set_result_value, \
            "Invalid receiver: %s (%s)" % (receiver, type(receiver))

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, account_config = None, format = "dict", receiver = None):
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
        self.check_forward(query, callback, is_deferred, execute, user, account_config, format, receiver)

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
        router.forward(query, False, True, user, receiver)
        result_value = receiver.get_result_value()

        if result_value["code"] != ResultValue.SUCCESS:
            raise Exception("Invalid query:\n %s: %r" % (query, result_value))

        records = result_value["value"]
        return records

    def success(self, receiver, query):
        """
        Shorthand method that must be called by a Gateway if its forward method succeeds.
        Args:
            receiver: A Receiver instance or a From Node.
            query: A Query instance:
        """
        if receiver:
            assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
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

