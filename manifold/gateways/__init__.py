#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class to manage Manifold's gateways
#
# A Gateway handles Manifold's query and translate them to query the
# underlying source of information (for instance a database, a CSV
# file, a Web Service, etc.). Once the result is retrieved, the
# Gateway translates each "record" in a python dictionnary having one key
# per queried field and its corresponding value. At least the Gateway
# send "None" to indicates that the whole set of "records" has been returned.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import os, sys, json, traceback
from types                        import StringTypes
from manifold.core.result_value   import ResultValue
from manifold.core.announce       import Announces
from manifold.util.plugin_factory import PluginFactory
#from manifold.util.misc           import find_local_modules
from manifold.util.type           import accepts, returns
from manifold.util.log            import Log

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(object):
    
    registry = {}

    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__gateway_name__'

    # XXX most of these parameters should not be required to construct a gateway
    # see manifold.core.forwarder for example
    # XXX remove query
    def __init__(self, interface=None, platform=None, query=None, config=None, user_config=None, user=None):
        """
        Constructor
        \param router (Interface) reference to the router on which the gateways
        are running
        \param platform (string) name of the platform
        \param query (Query) query to be sent to the platform
        \param config (dict) platform gateway configuration
        \param userconfig (dict) user configuration (account)
        \param user (dict) user information
        \sa manifold.core.router
        \sa manifold.core.query
        """
        # XXX explain why router is needed
        # XXX document better config, user_config & user parameters
        self.interface      = interface
        self.platform       = platform
        self.query          = query
        self.config         = config
        self.user_config    = user_config
        self.user           = user

        self.identifier     = 0 # The gateway will receive the identifier from the ast FROM node
        self.callback       = None
        self.result_value   = []

    def get_variables(self):
        variables = {}
        # Authenticated user
        variables['user_email'] = self.user['email']

        user_config = self.user['config']
        if user_config:
            user_config = json.loads(user_config)

            for k, v in user_config.items():
                if isinstance(v, StringTypes) and not 'credential' in v:
                    variables[k] = v

        # Account information of the authenticated user
        if self.user_config:
            for k, v in self.user_config.items():
                if isinstance(v, StringTypes) and not 'credential' in v:
                    variables[k] = v
        return variables

    def start(self):
        try:
            # Replaces variables in the Query (predicate in filters and parameters)
            filter = self.query.get_where()
            params = self.query.get_params()
            variables = self.get_variables()

            for predicate in filter:
                value = predicate.get_value()

                # XXX variable support not implemented for lists and tuples
                if isinstance(value, (tuple, list)):
                    continue

                if value and value[0] == '$':
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

            # copy of the query to keep original field names before rename in SFA 
            self.query = self.query.copy()
        except Exception, e:
            print "Exception in start", e
            traceback.print_exc()

    @returns(StringTypes)
    def get_platform(self):
        return self.platform

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.query)

    def set_callback(self, cb):
        self.callback = cb

    def get_callback(self):
        return self.callback

    def get_query(self):
        return self.query

    def set_query(self, query):
        self.query = query

    def set_user_config(self, user_config):
        self.user_config = user_config

    def get_gateway_type(self):
        """
        Returns:
            The type of the gateway (String instance)
        """
        gateway_type = self.__class__.__name__
        if gateway_type.endswith("Gateway"):
            gateway_type = gateway_type[:-7]
        return gateway_type.lower()

    @returns(StringTypes)
    def get_platform(self):
        """
        Returns:
            The platform managed by this Gateway (String instance) 
        """
        return self.platform

    @returns(list)
    def get_metadata(self):
        """
        Build metadata by loading header files
        Returns:
            The list of corresponding Announce instances
        """
        return Announces.from_dot_h(self.get_platform(), self.get_gateway_type())

    @returns(list)
    def get_result_value(self):
        """
        Retrieve the fetched records
        Returns:
            A list of ResultValue instances corresponding to the fetched records
        """
        return self.result_value
        
    def send(self, record):
        """
        \brief calls the parent callback with the record passed in parameter
        """
        if self.identifier:
            Log.record("[#%04d] [ %r ]" % (self.identifier, record))
        self.callback(record)

    def set_identifier(self, identifier):
        self.identifier = identifier

    @returns(StringTypes)
    def make_error_message(self, msg, uuid = None):
        return "Please contact %(name)s Support <%(mail)s> and reference %(uuid)s - %(msg)s" % {
            "name" : self.config["name"]                 if "name"                 in self.config else "?",
            "mail" : self.config["mail_support_address"] if "mail_support_address" in self.config else "?",
            "uuid" : uuid                                if uuid                                  else "?",
            "msg"  : msg
        }

def register_gateways():
    current_module = sys.modules[__name__]
    PluginFactory.register(current_module)

__all__ = ['Gateway']
