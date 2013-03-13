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
            "-p", "--port", dest = "xmlrpc_port",
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
        from twisted.internet   import reactor
        # This also imports manifold.util.reactor_thread that uses reactor
        from manifold.core.router       import Router

        name = Options().gateway

        if name:
            log_info("Initializing gateway : %s" % name)
            # Gateway initialization
            # XXX neither user nor query are known in advance
        else:
            self.gw_or_router = Router()
            self.gw_or_router.__enter__()

        # Get metadata for all accessible platforms. If more than one, we will
        # need routing (supposing we don't need routing for a single one)
        # In fact this is somehow having a router everytime
        #announces = self.gw_or_router.get_metadata()
        #if not announces:
        #    raise Exception, "Gateway or router returned no announce."
        #platform_capabilities = announces[0].capabilities
        #platform_fields = {}
        #for announce in announces:
        #    print "platform_fields[", announce.table.class_name, "] = ", announce.table.fields
        #    platform_fields[announce.table.class_name] = announce.table.fields

        # This imports twisted code so we need to import it locally
        from manifold.core.xmlrpc_api import XMLRPCAPI

        platforms = [Platform(u'dummy_platform', name, self.get_gateway_config(name))]

        try:
            reactor.listenTCP(Options().xmlrpc_port, server.Site(XMLRPCAPI(platforms, allowNone=True)))
            reactor.run()
        except Exception, e:
            # TODO If database gets disconnected, we can sleep/attempt reconnection
            log_error("Error in XMLRPC API: %s" % str(e))

    def terminate(self):
        # NOTE because we had to make the other import local, we _have to_
        # reimport reactor here
        from twisted.internet import reactor
        reactor.stop()
        if not Options().gateway:
            self.gw_or_router.__exit__()


if __name__ == '__main__':
    XMLRPCDaemon().start()
