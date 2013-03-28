#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# xmlrpc: daemon in charge of offering a XMLRPC interface to a router or gateway
# This file is part of the MANIFOLD project
#
# Copyright (C)2009-2013, UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Mrc-Olivier Buob  <marc-olivier.buob@lip6.fr>

import sys

from manifold.util.log          import *
from manifold.util.options      import Options
from manifold.util.daemon       import Daemon
from manifold.gateways          import Gateway
from manifold.core.query        import Query
from manifold.util.callback     import Callback
from manifold.core.ast          import AST
from manifold.core.table        import Table
from manifold.core.platform     import Platform
from manifold.core.forwarder    import Forwarder
from manifold.core.router       import Router
from manifold.core.capabilities import Capabilities
#from manifold.util.reactor_thread   import ReactorThread
from manifold.util.reactor_wrapper  import ReactorWrapper as ReactorThread
from manifold.util.storage      import DBStorage as Storage

#-------------------------------------------------------------------------------
# Class XMLRPCDaemon
#-------------------------------------------------------------------------------

class XMLRPCDaemon(Daemon):
    DEFAULTS = {
       # XMLRPC server
        'xmlrpc_port'   :   7080,

        # Gateway
        'gateway'       :   None
    }

    def __init__(self):
        """
        \brief Constructor
        """
        self.init_options()
        Logger.init_options()
        Daemon.init_options()
        Options().parse()
        
        # XXX how to avoid option conflicts : have a list of reserved ones for consistency
        # XXX can we support option groups ?
        
        Daemon.__init__(
            self,
            self.terminate
        )

    @classmethod
    def init_options(self):
        # Processing
        opt = Options()
        opt.add_option(
            "-P", "--port", dest = "xmlrpc_port",
            help = "Port on which the XMLRPC server will listen.", 
            default = 7080
        )
        # XXX router could be an additional argument
        opt.add_option(
            "-g", "--gateway", dest = "gateway",
            help = "Gateway exposed by the server, None for acting as a router.",
            default = None
        )
        opt.add_option(
            "-p", "--platform", dest = "platform",
            help = "Platform exposed by the server, None for acting as a router.",
            default = None
        )
        opt.add_option(
            "-a", "--disable-auth", action="store_true", dest = "disable_auth",
            help = "Disable authentication",
            default = False
        )
        
    def get_gateway_config(self, gateway_name):
        log_info("Hardcoded CSV|PostgreSQL configuration")
        if gateway_name == 'postgresql':
            config = {'db_user':'postgres', 'db_password':None, 'db_name':'test'}
        elif gateway_name == 'csv':
            config = {'filename': '/tmp/test.csv'}
        else:
            config = {}
        return config

    def main(self):
        """
        \brief Runs a XMLRPC server
        """
        log_info("XMLRPC server daemon (%s) started." % sys.argv[0])

        # NOTE it is important to import those files only after daemonization,
        # since they open files we cannot easily preserve
        from twisted.web        import xmlrpc, server
        #from twisted.internet   import reactor
        # This also imports manifold.util.reactor_thread that uses reactor
        from manifold.core.router       import Router

        assert not ( Options().platform and Options().gateway), "Both gateway and platform cannot be specified at commandline" 

        # This imports twisted code so we need to import it locally
        from manifold.core.xmlrpc_api import XMLRPCAPI

        # This should be configurable
        allowed_capabilities = Capabilities()
        allowed_capabilities.selection = True
        allowed_capabilities.projection = True

        # XXX We should harmonize interfaces between Router and Forwarder
        if Options().platform:
            platforms = Storage.execute(Query().get('platform'), format='object')
            # We pass a single platform to Forwarder
            platform = [p for p in platforms if p.name == Options().platform][0]
            self.interface = Forwarder(platform, allowed_capabilities)

        elif Options().gateway:
            platform = Platform(u'dummy', Options().gateway, self.get_gateway_config(Options().gateway))
            self.interface = Forwarder(platform, allowed_capabilities)

        else:
            self.interface = Router()

        try:
            ReactorThread().listenTCP(Options().xmlrpc_port, server.Site(XMLRPCAPI(self.interface, allowNone=True)))
            ReactorThread().start_reactor()
        except Exception, e:
            # TODO If database gets disconnected, we can sleep/attempt reconnection
            log_error("Error in XMLRPC API: %s" % str(e))

    def terminate(self):
        ReactorThread().stop_reactor()

def main():
    XMLRPCDaemon().start()

if __name__ == '__main__':
    main()
