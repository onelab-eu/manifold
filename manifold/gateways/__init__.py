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
# send LAST_RECORD (None) to indicates that the whole set of "records"
# has been returned.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

from types                              import StringTypes
from manifold.core.announce             import Announces
from manifold.core.query                import Query 
from manifold.core.result_value         import ResultValue
from manifold.models.user               import User
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

#MANDO|    # XXX most of these parameters should not be required to construct a gateway
#MANDO|    # XXX remove query
#MANDO|    def __init__(self, interface=None, platform=None, query=None, config=None, user_config=None, user=None):
#MANDO|        """
#MANDO|        Constructor
#MANDO|        Args:
#MANDO|            interface: The Manifold Interface on which this Gateway is running.
#MANDO|            platform: A String storing name of the platform related to this Gateway.
#MANDO|            query: (Query) query to be sent to the platform (TOREMOVE)
#MANDO|            config: A dictionnary containing the platform configuration
#MANDO|            userconfig: (dict) user configuration (TOREMOVE)
#MANDO|            user: A User instance (TOREMOVE)
#MANDO|        """
#MANDO|        # XXX explain why router is needed
#MANDO|        # XXX document better config, user_config & user parameters
#MANDO|        self.interface      = interface
#MANDO|        self.platform       = platform
#MANDO|        self.query          = query
#MANDO|        self.config         = config
#MANDO|        self.user_config    = user_config
#MANDO|        self.user           = user
#MANDO|
#MANDO|        self.identifier     = None # The gateway will receive the identifier from the ast FROM node
#MANDO|        self.callback       = None
#MANDO|        self.result_value   = []

    def __init__(self, interface, platform, config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            config: A dictionnary containing the configuration related to this Gateway.
                It may contains the following keys:
                "name" : name of the platform's maintainer. 
                "mail" : email address of the maintainer.
        """
        assert isinstance(platform, StringTypes) or not platform, "Invalid platform name: %s (%s)" % (platform,  type(platform))
        assert isinstance(config, dict) or not config,            "Invalid configuration: %s (%s)" % (config,    type(config))

        self.interface = interface
        self.platform  = platform
        self.config    = config

    @returns(dict)
    def get_config(self):
        """
        Returns 
            A dictionnary containing the configuration related to this Gateway.
            It may contains the following keys:
                "name" : name of the platform's maintainer. 
                "mail" : email address of the maintainer.
        """
        return self.config

    @returns(StringTypes)
    def get_platform(self):
        """
        Returns:
            The String containing the name of the platform related
            to this Gateway.
        """
        return self.platform

#MANDO|    def set_callback(self, cb):
#MANDO|        self.callback = cb
#MANDO|
#MANDO|    def get_callback(self):
#MANDO|        return self.callback
#MANDO|
#MANDO|    @returns(Query)
#MANDO|    def get_query(self):
#MANDO|        return self.query
#MANDO|
#MANDO|    def set_query(self, query):
#MANDO|        self.query = query
#MANDO|
#MANDO|    def set_user_config(self, user_config):
#MANDO|        self.user_config = user_config

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

#MANDO|    @returns(list)
#MANDO|    def get_result_value(self):
#MANDO|        """
#MANDO|        Retrieve the fetched records
#MANDO|        Returns:
#MANDO|            A list of ResultValue instances corresponding to the fetched records
#MANDO|        """
#MANDO|        return self.result_value

#    @returns(StringTypes)
#    def __str__(self):
#        """
#        Returns:
#            The '%s' representation of this Gateway.
#        """
##MANDO|        return "<%s %s>" % (self.__class__.__name__, self.query)
#        return "Platform<%s %s>" % (self.get_platform(), self.get_gateway_type())
#
#    @returns(StringTypes)
#    def __repr__(self):
#        """
#        Returns:
#            The '%r' representation of this Gateway.
#        """
#        return self.__str__()

#MANDO|    def send(self, record):
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

#MANDO|    def set_identifier(self, identifier):
#MANDO|        self.identifier = identifier

    @returns(StringTypes)
    def make_error_message(self, msg, uuid = None):
        """
        Format a full error message using the platform configuration.
        Args:
            msg: A String instance containing the cause of the error
            uuid: A String identifying the error or None.
        Returns:
            The corresponding error message.
        """
        return "Please contact %(name)s Support <%(mail)s> and reference %(uuid)s - %(msg)s" % {
            "name" : self.config["name"]                 if "name"                 in self.config else "?",
            "mail" : self.config["mail_support_address"] if "mail_support_address" in self.config else "?",
            "uuid" : uuid                                if uuid                                  else "?",
            "msg"  : msg
        }

    # DUPLICATE interface.py
    def check_forward(self, query, is_deferred = False, execute = True, user = None, receiver = None):
        assert isinstance(query, Query),                  "Invalid Query: %s (%s)"             % (query,       type(query))
        assert isinstance(is_deferred, bool),             "Invalid is_deferred value: %s (%s)" % (execute,     type(execute))
        assert isinstance(execute, bool),                 "Invalid execute value: %s (%s)"     % (is_deferred, type(is_deferred))
        assert not user or isinstance(user, User),        "Invalid User: %s (%s)"              % (user,        type(user))
        assert not receiver or receiver.set_result_value, "Invalid receiver: %s (%s)"          % (receiver,    type(receiver))

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, format = "dict", receiver = None):
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
            format: A String specifying in which format the Records must be returned.
            receiver : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        receiver.set_result_value(None)
        self.check_forward(query, is_deferred, execute, user, receiver)

    def success(self, receiver, query):
        if receiver:
            assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
            receiver.set_result_value(
                ResultValue(
                    origin = (ResultValue.GATEWAY, self.__class__.__name__, self.platform, query),
                    type   = ResultValue.SUCCESS, 
                    code   = ResultValue.SUCCESS,
                    value  = None 
                )
            )

    def error(self, receiver, query, description = ""):
        Log.warning(description)
        if receiver:
            import traceback
            assert isinstance(query, Query), "Invalid Query: %s (%s)" % (query, type(query))
            receiver.set_result_value(
                ResultValue(
                    origin      = (ResultValue.GATEWAY, self.__class__.__name__, self.platform, query),
                    type        = ResultValue.ERROR,
                    code        = ResultValue.ERROR,
                    description = description, 
                    traceback   = traceback.format_exc()
                )
            )


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
