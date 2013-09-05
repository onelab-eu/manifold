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

from types                              import StringTypes
from manifold.core.announce             import Announces
from manifold.core.result_value         import ResultValue
from manifold.util.plugin_factory       import PluginFactory
#from manifold.util.misc                 import find_local_modules
from manifold.util.type                 import accepts, returns
from manifold.util.log                  import Log
import traceback

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(object):
    
    registry = {}

    __metaclass__ = PluginFactory

    # XXX most of these parameters should not be required to construct a gateway
    # see manifold.core.forwarder for example
    # XXX remove query
    def __init__(self, interface=None, platform=None, query=None, config=None, user_config=None, user=None):
        """
        Constructor
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway.
            query: (Query) query to be sent to the platform (TODO: TO MOVE INTO forward)
            config: A dictionnary containing the platform configuration
            userconfig: (dict) user configuration (TODO: TO MOVE INTO forward)
            user: A User instance (TODO: TO MOVE INTO forward). 
        """
        # XXX explain why router is needed
        # XXX document better config, user_config & user parameters
        self.interface      = interface
        self.platform       = platform
        self.query          = query
        self.config         = config
        self.user_config    = user_config
        self.user           = user

        # TODO remove self.identifier
        self.identifier     = None # The Gateway will receive the identifier from the ast FROM node
        self.callback       = None
        self.result_value   = list() 

    @returns(dict)
    def get_variables(self):
        variables = dict() 
        # Authenticated user
        variables["user_email"] = self.user.email
        for k, v in self.user.get_config().items():
            if isinstance(v, StringTypes) and not "credential" in v:
                variables[k] = v
        # Account information of the authenticated user
        for k, v in self.user_config.items():
            if isinstance(v, StringTypes) and not "credential" in v:
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

                # XXX variable support not implemented for Predicates involving several values
                if not isinstance(value, StringTypes):
                    Log.warning("Gateway::start(): value type not supported %s" % value)
                    continue
                value = (value,)

                if value[0] == "$":
                    var = value[1:]
                    if var in variables:
                        predicate.set_value(variables[var])

            for key, value in params.items():

                # XXX variable support not implemented for Predicates involving several values
                if not isinstance(value, StringTypes):
                    Log.warning("Gateway::start(): value type not supported %s" % value)
                    continue
                value = (value,)
                
                if value[0] == "$":
                    var = value[1:]
                    if var in variables and isinstance(variables[var], StringTypes):
                        params[k] = variables[var]

        except Exception, e:
            print "Exception in start", e
            import traceback
            traceback.print_exc()

    @returns(StringTypes)
    def get_platform(self):
        """
        Returns:
            The String containing the name of the platform related
            to this Gateway.
        """
        return self.platform

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Gateway.
        """
        return "<%s %s>" % (self.__class__.__name__, self.query)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Gateway.
        """
        return self.__str__()

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

    @returns(StringTypes)
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
        Calls the parent callback with the Record passed in parameter.
        In other word, this From Node send Record to its parent Node in the QueryPlan.
        """
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


#-------------------------------------------------------------------------------
# List of gateways
#-------------------------------------------------------------------------------

#import os, glob
#from manifold.util.misc import find_local_modules

# XXX Remove __init__
# XXX Missing recursion for sfa
#__all__ = find_local_modules(__file__)
#[ os.path.basename(f)[:-3] for f in glob.glob(os.path.dirname(__file__)+"/*.py")]

def register():
    try:
        from manifold.gateways.postgresql       import PostgreSQLGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.tdmi             import TDMIGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.sfa              import SFAGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.maxmind          import MaxMindGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.csv              import CSVGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.manifold_xmlrpc  import ManifoldGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.sqlalchemy       import SQLAlchemyGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.oml              import OMLGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.perfsonar        import PerfSONARGateway
    except:
        Log.warning(traceback.format_exc())
        pass

register()

__all__ = ['Gateway']
