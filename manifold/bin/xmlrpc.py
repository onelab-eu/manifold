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
from manifold.core.router       import LocalRouter

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

        #router = THLocalRouter()
        #router.__enter__()

        class XMLRPCAPI(xmlrpc.XMLRPC):
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
                return gw_or_router.forward(*args, deferred=True, user=user)

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

    def authenticate(self, auth):
        return Auth(auth).check()


if __name__ == '__main__':
    XMLRPCDaemon().start()
