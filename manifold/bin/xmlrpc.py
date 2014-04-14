#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# XMLRPCDaemon is in charge of offering a XMLRPC interface
# to a Router or Gateway related to a
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

from manifold.core.router           import Router
from manifold.core.record           import Record
from manifold.core.capabilities     import Capabilities
from manifold.util.daemon           import Daemon
from manifold.util.filesystem       import ensure_writable_directory, ensure_keypair, ensure_certificate, mkdir
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.type             import returns, accepts
from manifold.util.reactor_thread   import ReactorThread # reactor_wrapper  import ReactorWrapper as ReactorThread

# Let's try to load this before twisted

from manifold.types.string          import string
from manifold.types.int             import int
from manifold.types.inet            import inet
from manifold.types.hostname        import hostname
from manifold.types.date            import date
import datetime

xmlrpclib.Marshaller.dispatch[string]   = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[int]      = xmlrpclib.Marshaller.dump_int
xmlrpclib.Marshaller.dispatch[inet]     = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[hostname] = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[date]     = lambda s: xmlrpclib.Marshaller.dump_string(str(s))
xmlrpclib.Marshaller.dispatch[Record]   = xmlrpclib.Marshaller.dump_struct
xmlrpclib.Marshaller.dispatch[Records]  = xmlrpclib.Marshaller.dump_list

#-------------------------------------------------------------------------------
# Class XMLRPCDaemon
#-------------------------------------------------------------------------------

class XMLRPCDaemon(Daemon):
    DEFAULTS = {
        "xmlrpc_port" : 7080, # XMLRPC server port
        "platform"    : None
    }

    def __init__(self):
        """
        Constructor.
        """
        # XXX how to avoid option conflicts : have a list of reserved ones for consistency
        # XXX can we support option groups ?

        Daemon.__init__(
            self,
            self.terminate_callback
        )

    @staticmethod
    def init_options():
        """
        Prepare options supported by XMLRPCDaemon.
        """
        # Processing
        opt = Options()
        opt.add_argument(
            "-P", "--port", dest = "xmlrpc_port",
            help = "Port on which the XMLRPC server will listen.",
            default = XMLRPCDaemon.DEFAULTS["xmlrpc_port"]
        )
# mando: we should use -p instead
#        # XXX router could be an additional argument
#        opt.add_argument(
#            "-g", "--gateway", dest = "gateway",
#            help = "Gateway exposed by the server, None for acting as a router.",
#            default = None
#        )
        opt.add_argument(
            "-p", "--platform", dest = "platform",
            help = "Platform exposed by the server, None for acting as a router.",
            default = XMLRPCDaemon.DEFAULTS["platform"]
        )
#NOT_SUPPORTED|        opt.add_argument(
#NOT_SUPPORTED|            "-a", "--disable-auth", action="store_true", dest = "disable_auth",
#NOT_SUPPORTED|            help = "Disable authentication",
#NOT_SUPPORTED|            default = False
#NOT_SUPPORTED|        )
#DEPRECATED|        opt.add_argument(
#DEPRECATED|            "-t", "--trusted-roots-path", dest = "trusted_roots_path",
#DEPRECATED|            help = "Select the directory holding trusted root certificates",
#DEPRECATED|            default = '/etc/manifold/trusted_roots/'
#DEPRECATED|        )
#DEPRECATED|        opt.add_argument(
#DEPRECATED|            "-s", "--server-ssl-path", action="store_true", dest = "ssl_path",
#DEPRECATED|            help = "Select the directory holding the server private key and certificate for SSL",
#DEPRECATED|            default = '/etc/manifold/keys'
#DEPRECATED|        )
#DEPRECATED|
#DEPRECATED|    # XXX should be removed
#DEPRECATED|    def get_platform_config(self, gateway_name):
#DEPRECATED|        """
#DEPRECATED|        Load a default hardcoded configuration.
#DEPRECATED|        """
#DEPRECATED|        Log.info("Hardcoded CSV|PostgreSQL configuration")
#DEPRECATED|        if gateway_name == "postgresql":
#DEPRECATED|            config = {
#DEPRECATED|                "db_user"     : "postgres",
#DEPRECATED|                "db_password" : None,
#DEPRECATED|                "db_name"     : "test"}
#DEPRECATED|        elif gateway_name == "csv":
#DEPRECATED|            config = {"filename" : "/tmp/test.csv"}
#DEPRECATED|        else:
#DEPRECATED|            config = dict()
#DEPRECATED|        return config

    @staticmethod
    @returns(Capabilities)
    def make_capabilities():
        """
        Build a Router instance according to the Options passed to
        the XMLRPCDaemon.
        Returns:
            The default Capabilities supported by a XMLRPCDaemon
        """
        # This should be configurable through Options singleton.
        capabilities = Capabilities()
        capabilities.selection  = True
        capabilities.projection = True
        return capabilities

    @staticmethod
    @returns(Router)
    def make_router(allowed_capabilities):
        """
        Build a Router instance according to the Options passed to
        the XMLRPCDaemon.
        Args:
            allowed_capabilities: A Capabilities defining which
                Capabilities are supported by the Router we are building.
        Returns:
            The corresponding Router instance.
        """
        assert isinstance(allowed_capabilities, Capabilities)

        from manifold.bin.config import MANIFOLD_STORAGE

        router = Router(allowed_capabilities)
#DEPRECATED|        router.set_storage(MANIFOLD_STORAGE)
#DEPRECATED|        platform_names = set([Options().platform]) if Options().platform else None
#DEPRECATED|        router.load_storage(platform_names)
        return router

    def main(self):
        """
        Run a XMLRPC server (called by Daemon::start).
        """
        Log.info("XML-RPC server daemon (%s) started." % sys.argv[0])

        # NOTE it is important to import those files only after daemonization,
        # since they open files we cannot easily preserve
        from twisted.web                import xmlrpc, server

        # SSL support
        from OpenSSL                    import SSL
        from twisted.internet           import ssl #, reactor

        #assert not (Options().platform and Options().gateway), "Both gateway and platform cannot be specified at commandline"

        # This imports twisted code so we need to import it locally
        from manifold.core.xmlrpc_api   import XMLRPCAPI

        allowed_capabilities = XMLRPCDaemon.make_capabilities()

        from manifold.clients.deferred_router import ManifoldDeferredRouterClient
        self.interface = ManifoldDeferredRouterClient()

        # SSL support

        # XXX - should the directory be passed as an option ?

        manifold_etc_dir           = os.path.dirname(Options().cfg_filename) #"/etc/manifold"
        # XXX should ssl_path be a subdirectory of /etc/manifold ?
        # ssl_path = Options().ssl_path
        manifold_keys_dir          = os.path.join(manifold_etc_dir,  "keys")
        keypair_filename           = os.path.join(manifold_keys_dir, "server.key")
        certificate_filename       = os.path.join(manifold_keys_dir, "server.cert")
        manifold_trusted_roots_dir = os.path.join(manifold_etc_dir,  "trusted_roots")

        try:
            mkdir(manifold_keys_dir)
            ensure_writable_directory(manifold_keys_dir)
            ensure_writable_directory(manifold_trusted_roots_dir)

            keypair = ensure_keypair(keypair_filename)
            subject = "manifold" # XXX Where to get the subject of the certificate ?
            certificate = ensure_certificate(certificate_filename, subject, keypair)
        except Exception, e:
            Log.error(e)
            raise e

#DEPRECATED|        if not ssl_path or not os.path.exists(ssl_path):
#DEPRECATED|            print ""
#DEPRECATED|            print "You need to generate SSL keys and certificate in "%s" to be able to run manifold" % ssl_path
#DEPRECATED|            print ""
#DEPRECATED|            print "mkdir -p /etc/manifold/keys"
#DEPRECATED|            print "openssl genrsa 1024 > /etc/manifold/keys/server.key"
#DEPRECATED|            print "chmod 400 /etc/manifold/keys/server.key"
#DEPRECATED|            print "openssl req -new -x509 -nodes -sha1 -days 365 -key /etc/manifold/keys/server.key > /etc/manifold/keys/server.cert"
#DEPRECATED|            print ""
#DEPRECATED|            sys.exit(0)

        try:
            @returns(bool)
            def verifyCallback(connection, x509, errnum, errdepth, ok):
                if not ok:
                    Log.error("Invalid certificate from subject: %(subject)s %(errnum)s %(errdepth)" % {
                        "subject"  : x509.get_subject(),
                        "errnum"   : errnum,
                        "errdepth" : errdepth
                    })
                    ret = False
                else:
                    Log.info("Certificates are fine: %(x509)s %s(subject)s" % {
                        "x509"    : x509,
                        "subject" : x509.get_subject()
                    })
                    ret = True
                return ret

            myContextFactory = ssl.DefaultOpenSSLContextFactory(keypair_filename, certificate_filename)

            ctx = myContextFactory.getContext()

            ctx.set_verify(
                SSL.VERIFY_PEER, # | SSL.VERIFY_FAIL_IF_NO_PEER_CERT,
                verifyCallback
            )

            # Since we have self-signed certs we have to explicitly
            # tell the server to trust them.
            #ctx.load_verify_locations("keys/ca.pem")

#DEPRECATED|            trusted_roots_path = Options().trusted_roots_path
#DEPRECATED|            if not trusted_roots_path or not os.path.exists(trusted_roots_path):
#DEPRECATED|                Log.warning("No trusted root found in %s. You won't be able to login using SSL client certificates" % trusted_roots_path)

            ctx.load_verify_locations(None, manifold_trusted_roots_dir) #trusted_roots_path)

            #ReactorThread().listenTCP(Options().xmlrpc_port, server.Site(XMLRPCAPI(self.interface, allowNone=True)))
            ReactorThread().listenSSL(
                Options().xmlrpc_port,
                server.Site(XMLRPCAPI(self.interface, allowNone = True)),
                myContextFactory
            )

            Log.info("Starting XMLRPCDaemon (https://localhost:%s)" % Options().xmlrpc_port)
            ReactorThread().start_reactor()
        except Exception, e:
            # TODO If database gets disconnected, we can sleep/attempt reconnection
            Log.error("XMLRPC API error: %s" % str(e))

    def terminate_callback(self):
        """
        Stop gracefully this XMLRPCDaemon instance.
        """
        Log.info("Stopping gracefully ReactorThread")
        ReactorThread().stop_reactor()

    def leave(self):
        pass

def main():
    XMLRPCDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()
    XMLRPCDaemon().start()

if __name__ == "__main__":
    main()
