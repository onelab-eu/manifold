#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# xmlrpc: daemon in charge of offering a XMLRPC interface to a router or gateway
# instanciate an interface of type Forwarder or Router depending on the arguments given
# relies on the Class XMLRPCAPI in manifold/core/xmlrpc_api.py
#
# This file is part of the MANIFOLD project
#
# Copyright (C)2009-2013, UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loïc Baron        <loic.baron@lip6.fr>

import sys, xmlrpclib, datetime, os.path

from manifold.core.query            import Query
from manifold.core.forwarder        import Forwarder
from manifold.core.router           import Router
from manifold.core.capabilities     import Capabilities
from manifold.models.platform       import Platform
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.daemon           import Daemon
from manifold.util.type             import returns, accepts
from manifold.util.reactor_wrapper  import ReactorWrapper as ReactorThread

# Let's try to load this before twisted

from manifold.types.string      import string
from manifold.types.int         import int
from manifold.types.inet        import inet
from manifold.types.hostname    import hostname
from manifold.types.date        import date
import datetime

xmlrpclib.Marshaller.dispatch[string]   = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[int]      = xmlrpclib.Marshaller.dump_int
xmlrpclib.Marshaller.dispatch[inet]     = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[hostname] = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[date]     = lambda s: xmlrpclib.Marshaller.dump_string(str(s))

#-------------------------------------------------------------------------------
# Class XMLRPCDaemon
#-------------------------------------------------------------------------------

class XMLRPCDaemon(Daemon):
    DEFAULTS = {
        'xmlrpc_port' : 7080, # XMLRPC server port
        'gateway'     : None  # Gateway
    }

    def __init__(self):
        """
        Constructor.
        """
        # XXX how to avoid option conflicts : have a list of reserved ones for consistency
        # XXX can we support option groups ?
        
        Daemon.__init__(
            self,
            self.terminate
        )

    @classmethod
    def init_options(self):
        """
        Prepare options supported by XMLRPCDaemon.
        """
        # Processing
        opt = Options()
        opt.add_argument(
            "-P", "--port", dest = "xmlrpc_port",
            help = "Port on which the XMLRPC server will listen.", 
            default = 7080
        )
        # XXX router could be an additional argument
        opt.add_argument(
            "-g", "--gateway", dest = "gateway",
            help = "Gateway exposed by the server, None for acting as a router.",
            default = None
        )
        opt.add_argument(
            "-p", "--platform", dest = "platform",
            help = "Platform exposed by the server, None for acting as a router.",
            default = None
        )
        opt.add_argument(
            "-a", "--disable-auth", action="store_true", dest = "disable_auth",
            help = "Disable authentication",
            default = False
        )
        opt.add_argument(
            "-t", "--trusted-roots-path", dest = "trusted_roots_path",
            help = "Select the directory holding trusted root certificates",
            default = '/etc/manifold/trusted_roots/'
        )
        opt.add_argument(
            "-s", "--server-ssl-path", action="store_true", dest = "ssl_path",
            help = "Select the directory holding the server private key and certificate for SSL",
            default = '/etc/manifold/keys'
        )

    # XXX should be removed
    def get_gateway_config(self, gateway_name):
        """
        Load a default hardcoded configuration.
        """
        Log.info("Hardcoded CSV|PostgreSQL configuration")
        if gateway_name == "postgresql":
            config = {
                "db_user"     : "postgres",
                "db_password" : None,
                "db_name"     : "test"}
        elif gateway_name == "csv":
            config = {"filename" : "/tmp/test.csv"}
        else:
            config = {}
        return config
        
    def main(self):
        """
        \brief Runs a XMLRPC server
        """
        Log.info("XMLRPC server daemon (%s) started." % sys.argv[0])

        # NOTE it is important to import those files only after daemonization,
        # since they open files we cannot easily preserve
        from twisted.web        import xmlrpc, server

        # SSL support
        from OpenSSL import SSL
        from twisted.internet import ssl #, reactor
        #from twisted.internet.protocol import Factory, Protocol

        #from twisted.internet   import reactor
        # This also imports manifold.util.reactor_thread that uses reactor
        from manifold.core.router       import Router
            


        assert not (Options().platform and Options().gateway), "Both gateway and platform cannot be specified at commandline" 

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
            # XXX user
            # XXX Change Forwarded initializer
#DEPRECATED|            platform = Platform(u'dummy', Options().gateway, self.get_gateway_config(Options().gateway), 'user')
            platform = Platform(
                platform     = u'dummy',
                gateway_type = Options().gateway,
                config       = self.get_gateway_config(Options().gateway),
                auth_type    = 'user'
            )
            self.interface = Forwarder(platform, allowed_capabilities)

        else:
            self.interface = Router()

        try:
            def verifyCallback(connection, x509, errnum, errdepth, ok):
                if not ok:
                    print 'invalid cert from subject:', x509.get_subject()
                    print errnum, errdepth
                    return False
                else:
                    print "Certs are fine", x509, x509.get_subject()
                return True
            
            ssl_path = Options().ssl_path
            if not ssl_path or not os.path.exists(ssl_path):
                print ""
                print "You need to generate SSL keys and certificate in '%s' to be able to run manifold" % ssl_path
                print ""
                print "mkdir -p /etc/manifold/keys"
                print "openssl genrsa 1024 > /etc/manifold/keys/server.key"
                print "chmod 400 /etc/manifold/keys/server.key"
                print "openssl req -new -x509 -nodes -sha1 -days 365 -key /etc/manifold/keys/server.key > /etc/manifold/keys/server.cert"
                print ""
                sys.exit(0)

            server_key_file = "%s/server.key" % ssl_path
            server_crt_file = "%s/server.cert" % ssl_path
            Log.tmp("key, cert=", server_key_file, server_crt_file)
            myContextFactory = ssl.DefaultOpenSSLContextFactory(server_key_file, server_crt_file)
            
            ctx = myContextFactory.getContext()
            
            ctx.set_verify(
                SSL.VERIFY_PEER, # | SSL.VERIFY_FAIL_IF_NO_PEER_CERT,
                verifyCallback
                )
            
            # Since we have self-signed certs we have to explicitly
            # tell the server to trust them.
            #ctx.load_verify_locations("keys/ca.pem")

            trusted_roots_path = Options().trusted_roots_path
            if not trusted_roots_path or not os.path.exists(trusted_roots_path):
                Log.warning("No trusted root found in %s. You won't be able to login using SSL client certificates" % trusted_roots_path)
                
            ctx.load_verify_locations(None, ssl_path)


            #ReactorThread().listenTCP(Options().xmlrpc_port, server.Site(XMLRPCAPI(self.interface, allowNone=True)))
            ReactorThread().listenSSL(Options().xmlrpc_port, server.Site(XMLRPCAPI(self.interface, allowNone=True)), myContextFactory)
            ReactorThread().start_reactor()
        except Exception, e:
            # TODO If database gets disconnected, we can sleep/attempt reconnection
            Log.error("Error in XMLRPC API: %s" % str(e))

    def terminate(self):
        ReactorThread().stop_reactor()

def main():
    XMLRPCDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()
    
    XMLRPCDaemon().start()

if __name__ == '__main__':
    main()
