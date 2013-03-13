#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.util.plugin_factory import PluginFactory
#from manifold.util.misc import find_local_modules

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(object):
    
    registry = {}

    __metaclass__ = PluginFactory

    def __init__(self, router, platform, query, config, user_config, user):
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
        self.router      = router
        self.platform    = platform
        self.query       = query
        self.config      = config
        self.user_config = user_config
        self.user        = user

        self.callback    = None

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.query)

    def set_callback(self, cb):
        self.callback = cb


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

register()

__all__ = ['Gateway']
