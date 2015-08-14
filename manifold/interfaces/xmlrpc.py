#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import sys, xmlrpclib, datetime, os.path

from manifold.core.annotation       import Annotation
from manifold.core.router           import Router
from manifold.core.record           import Record, Records
from manifold.core.result_value     import ResultValue
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

from manifold.interfaces            import Interface

xmlrpclib.Marshaller.dispatch[string]       = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[int]          = xmlrpclib.Marshaller.dump_int
xmlrpclib.Marshaller.dispatch[inet]         = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[hostname]     = xmlrpclib.Marshaller.dump_string
xmlrpclib.Marshaller.dispatch[date]         = lambda s: xmlrpclib.Marshaller.dump_string(str(s))
xmlrpclib.Marshaller.dispatch[Record]       = xmlrpclib.Marshaller.dump_struct
xmlrpclib.Marshaller.dispatch[Records]      = xmlrpclib.Marshaller.dump_array
xmlrpclib.Marshaller.dispatch[ResultValue]  = xmlrpclib.Marshaller.dump_struct
xmlrpclib.Marshaller.dispatch[Annotation]  = xmlrpclib.Marshaller.dump_struct

import copy, traceback
from twisted.web                        import xmlrpc
#FEDORABUGS|try:
#FEDORABUGS|    from twisted.web.xmlrpc                 import withRequest
#FEDORABUGS|except:
#FEDORABUGS|    def withRequest(f):
#FEDORABUGS|        f.withRequest = True
#FEDORABUGS|        return f

from manifold.auth                      import Auth
from manifold.core.annotation           import Annotation
from manifold.core.code                 import CORE, ERROR, FORBIDDEN
#from manifold.core.deferred_receiver    import DeferredReceiver
from manifold.core.query_factory        import QueryFactory
from manifold.core.result_value         import ResultValue
from manifold.util.options              import Options
from manifold.util.log                  import Log
from manifold.util.misc                 import make_list

#-------------------------------------------------------------------------------
# Class XMLRPCAPI
#-------------------------------------------------------------------------------

DEFAULT_PORT = 7080

class XMLRPCAPI(xmlrpc.XMLRPC, object):

    #__metaclass__ = XMLRPCAPIMetaclass
    class __metaclass__(type):
        def __init__(cls, name, bases, dic):
            type.__init__(cls, name, bases, dic)

            # Dynamically add functions corresponding to methods from the # Auth class
            # XXX Shall we handle exceptions here ?
            for k, v in vars(Auth).items():
                if not k.startswith('_'):
                    def v_exc_handler(*args, **kwargs):
                        try:
                            v(*args, **kwargs)
                        except Exception, e:
                            ret = dict(ResultValue(
                               origin      = (CORE, cls.__class__.__name__),
                               type        = ERROR,
                               code        = ERROR,
                               description = str(e),
                               traceback   = traceback.format_exc()))
                            return ret
                    setattr(cls, "xmlrpc_%s" % k, v_exc_handler)

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            assert 'router' not in kwargs, "Cannot specify router argument twice"
            self._deferred_router_client = args[0]
        elif len(args) == 0:
            assert 'router' in kwargs, "router argument must be specified"
            self._deferred_router_client = kwargs['router']
        else:
            raise Exception, "Wrong arguments"
        super(XMLRPCAPI, self).__init__(**kwargs)

    def display_query(self, *args):
        # Don't show password in Server Logs
        display_args = make_list(copy.deepcopy(args))
        if 'AuthString' in display_args[0].keys():
            display_args[0]['AuthString'] = "XXXXX"
        return display_args


#    @withRequest
#    def xmlrpc_AuthCheck(self, request, annotation = None):
    def xmlrpc_AuthCheck(self, annotation = None):
        # We expect to find an authentication token in the annotation
        if annotation:
            auth = annotation.get('authentication', None)
        else:
            auth = {}

#        auth['request'] = request

        return Auth(auth, self._deferred_router_client).check()

    # QUERIES
    # xmlrpc_forward function is called by the Query of the user using xmlrpc
#    @withRequest
#    def xmlrpc_forward(self, request, query, annotation = None):
    def xmlrpc_forward(self, query, annotation = None):
        """
        """
        Log.info("Incoming XMLRPC request, query = %r, annotation = %r" % (self.display_query(query), annotation))
        if Options().disable_auth:
            Log.info("Authentication disabled by configuration")
        else:
            if not annotation or not "authentication" in annotation:
                msg = "You need to specify an authentication token in annotation"
                return dict(ResultValue.error(msg, FORBIDDEN))

            # We expect to find an authentication token in the annotation
            if annotation:
                auth = annotation.get('authentication', None)
            else:
                auth = {}

#            auth['request'] = request

            # Check login password
            try:
                # We get the router to make synchronous queries
                user = Auth(auth, self._deferred_router_client.get_router()).check()
            except Exception, e:
                import traceback
                traceback.print_exc()
                Log.warning("XMLRPCAPI::xmlrpc_forward: Authentication failed...: %s" % str(e))
                msg = "Authentication failed: %s" % e
                return dict(ResultValue.error(msg, FORBIDDEN))

        # self._deferred_router_client is a ManifoldDeferredRouterClient, it returns a deferred
        annotation = Annotation(annotation) if annotation else Annotation()
        annotation['user'] = user
        return self._deferred_router_client.forward(QueryFactory.from_dict(query), annotation)

    def _xmlrpc_action(self, action, *args):
        Log.info("_xmlrpc_action")
        # The first argument is eventually an authentication token
        if Options().disable_auth:
            query, = args
        else:
            auth, query = args

        query['action'] = action

        if Options().disable_auth:
            return self.xmlrpc_forward(query)
        else:
            return self.xmlrpc_forward(auth, query)

    def xmlrpc_Get   (self, *args): return self._xmlrpc_action('get',    *args)
    def xmlrpc_Update(self, *args): return self._xmlrpc_action('update', *args)
    def xmlrpc_Create(self, *args): return self._xmlrpc_action('create', *args)
    def xmlrpc_Delete(self, *args): return self._xmlrpc_action('delete', *args)

        # FORMER CODE FOR ROUTER
        # cb = Callback()
        # ast.callback = cb
        #
        # gw_or_router.set_callback(cb) # XXX should be removed from Gateway
        # gw_or_router.forward(query, deferred=False, user=user)
        # cb.wait()


class XMLRPCInterface(Interface):
    """
    Server interface ( = serial port)
    This is only used to create new interfaces on the flight
    """
    __interface_type__ = 'xmlrpc'

    ############################################################################
    # Constructor / Destructor

    def __init__(self, router, platform_name = None, **platform_config):
        Interface.__init__(self, router, platform_name, **platform_config)

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

        allowed_capabilities = XMLRPCInterface.make_capabilities()

        from manifold.clients.deferred_router import ManifoldDeferredRouterClient
        self.router = ManifoldDeferredRouterClient(load_storage = True)

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

            #ReactorThread().listenTCP(Options().xmlrpc_port, server.Site(XMLRPCAPI(self.router, allowNone=True)))
            ReactorThread().listenSSL(
                DEFAULT_PORT,
                server.Site(XMLRPCAPI(self.router, allowNone = True)),
                myContextFactory
            )

            Log.info("Starting XMLRPC interface (https://localhost:%s)" % DEFAULT_PORT)
            ReactorThread().start_reactor()
            while True:
                ReactorThread().join(timeout=1) 
                if not ReactorThread().is_alive():
                    break
        except Exception, e:
            # TODO If database gets disconnected, we can sleep/attempt reconnection
            Log.error("XMLRPC API error: %s" % str(e))

    def terminate(self):
        ReactorThread().stop_reactor(force = True)

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

    def send_impl(self, packet):
        destination = packet.get_destination()
        namespace = destination.get_namespace()
        object_name = destination.get_object_name()

        if namespace == 'test' and object_name == 'timeout':
            announces = Announces.from_string(announce_str)
            self.records(announces)
        pass
        
    def receive_impl(self, packet):
        pass
