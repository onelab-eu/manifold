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
from manifold.core.result_value         import ResultValue
from manifold.core.announce             import Announces
from manifold.util.plugin_factory       import PluginFactory
#from manifold.util.misc                 import find_local_modules
from manifold.util.type                 import accepts, returns
from manifold.util.log                  import Log

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(object):
    
    registry = {}

    __metaclass__ = PluginFactory

    # XXX most of these parameters should not be required to construct a gateway
    # see manifold.core.forwarder for example
    def __init__(self, interface, platform, query, config, user_config, user):
        """
        Constructor
        \param router (THRouter) reference to the router on which the gateways
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

        self.identifier     = None # The gateway will receive the identifier from the ast FROM node
        self.callback       = None
        self.result_value   = []

    @returns(StringTypes)
    def get_platform(self):
        return self.platform

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.query)

    def set_callback(self, cb):
        self.callback = cb

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
    from manifold.gateways.postgresql       import PostgreSQLGateway
    from manifold.gateways.tdmi             import TDMIGateway
    from manifold.gateways.sfa              import SFAGateway
    from manifold.gateways.maxmind          import MaxMindGateway
    from manifold.gateways.csv              import CSVGateway
    from manifold.gateways.manifold_xmlrpc  import ManifoldGateway
    from manifold.gateways.sqlalchemy       import SQLAlchemyGateway
    from manifold.gateways.oml              import OMLGateway

register()

__all__ = ['Gateway']
