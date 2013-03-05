#!/usr/bin/env python
# -*- coding: utf-8 -*-

#-------------------------------------------------------------------------------
# Generic Gateway class
#-------------------------------------------------------------------------------

class Gateway(object):
    # XXX explain why router is needed
    # XXX document better config, user_config & user parameters
    def __init__(self, router, platform, query, config, user_config, user):
        """
        Constructor
        \param router (THRouter) reference to the router on which the gateways
        are running
        \param platform (string) name of the platform
        \param query (Query) query to be sent to the platform
        \param config (dict) platform configuration
        \param userconfig (dict) user configuration (account)
        \param user (dict) user information
        \sa manifold.core.router
        \sa manifold.core.query
        """
        self.router      = router
        self.platform    = platform
        self.query       = query
        self.config      = config
        self.user_config = user_config
        self.user        = user

#-------------------------------------------------------------------------------
# List of gateways
#-------------------------------------------------------------------------------

import os
import glob

# XXX Remove __init__
# XXX Missing recursion for sfa
__all__ = find_local_modules()
[ os.path.basename(f)[:-3] for f in glob.glob(os.path.dirname(__file__)+"/*.py")]

from tophat.gateways.sfa import SFA
