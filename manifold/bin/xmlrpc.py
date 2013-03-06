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
from manifold.auth              import Auth
from manifold.gateways          import Gateway
from manifold.core.query        import Query
from manifold.util.callback     import Callback

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
            log_info("Hardcoded PostgreSQL configuration")
            if name == 'postgresql':
                config = {'db_user':'postgres', 'db_password':None, 'db_name':'test'}
            else:
                config = {}
            args = [None, name, None, config, {}, {}]

            self.gw_or_router = Gateway.get(name)(*args)
        else:
            self.gw_or_router = Router()
            self.gw_or_router.__enter__()

        # used with XMLRPCAPI
        gw_or_router = self.gw_or_router

        class XMLRPCAPI(xmlrpc.XMLRPC):

            def authenticate(self, auth):
                user = Auth(auth).check()
                return user

            # QUERIES
            def xmlrpc_forward(self, *args):
                """
                """
                if not Options().disable_auth:
                    # The first argument should be an authentication token
                    user = self.authenticate(args[0])
                    #args = list(args)
                    args = args[1:]
                else:
                    user = None
                # The rest define the query
                query = Query(*args)

                # Can we factorize this ?
                cb = Callback()
                gw_or_router.set_callback(cb)
                gw_or_router.forward(query, deferred=False, user=user)
                cb.wait()

                return cb.results

        # We can dynamically add functions corresponding to methods from the
        # Auth class
        for k, v in vars(Auth).items():
            if not k.startswith('_'):
                setattr(XMLRPCAPI, "xmlrpc_%s" % k, v)

        try:
            reactor.listenTCP(Options().xmlrpc_port, server.Site(XMLRPCAPI(allowNone=True)))
            #reactor.callFromThread(lambda: reactor.listenTCP(Options().xmlrpc_port, server.Site(XMLRPCAPI(allowNone=True))))
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
