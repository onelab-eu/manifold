#!/usr/bin/env python
# -*- coding: utf-8 -*-

## Fix bugs in httpclient and twisted/web/xmlrpc.py
##from twisted.protocols.tls import TLSMemoryBIOProtocol
#import twisted.protocols.tls
#class _TLSMemoryBIOProtocol(twisted.protocols.tls.TLSMemoryBIOProtocol):
#    def writeSequence(self, iovec):
#        """
#        Write a sequence of application bytes by joining them into one string
#        and passing them to L{write}.
#        """
#        iovec = [x.encode('latin-1') for x in iovec]
#        print iovec
#        self.write(b"".join(iovec))
#twisted.protocols.tls.TLSMemoryBIOProtocol = _TLSMemoryBIOProtocol
##/bugfix

import os, sys, tempfile, time
from types                        import StringTypes
from manifold.util.reactor_thread import ReactorThread
from manifold.util.log            import Log
from manifold.util.singleton      import Singleton
from twisted.internet             import ssl
from OpenSSL.crypto               import TYPE_RSA, FILETYPE_PEM
from OpenSSL.crypto               import load_certificate, load_privatekey
from twisted.internet             import defer

DEFAULT_TIMEOUT = 20

AGGREGATE_CALLS = ['GetVersion', 'ListResources']
REGISTRY_CALLS = ['GetVersion', 'Resolve', 'Update', 'Delete', 'Register']

ARG_SNIFF_CRED  = "<?xml version=\"1.0\"?>\n<signed-credential "
ARG_SNIFF_RSPEC = "<?xml version=\"1.0\"?>\n<rspec "

class CtxFactory(ssl.ClientContextFactory):

    def __init__(self, pkey, cert):
        self.pkey = pkey
        self.cert = cert

    def getContext(self):
        def infoCallback(conn, where, ret):
            # conn is a OpenSSL.SSL.Connection
            # where is a set of flags telling where in the handshake we are
            # See http://www.openssl.org/docs/ssl/SSL_CTX_set_info_callback.html

            try:
                #print "infoCallback %r %d %d" % (conn, where, ret)
                if where & ssl.SSL.SSL_CB_HANDSHAKE_START:
                    print "Handshake started"
                if where & ssl.SSL.SSL_CB_HANDSHAKE_DONE:
                    print "Handshake done"

                w = where & ~ ssl.SSL.SSL_ST_MASK
                if w & ssl.SSL.SSL_ST_CONNECT:
                    str="SSL_connect"
                elif w & ssl.SSL.SSL_ST_ACCEPT:
                    str="SSL_accept"
                else:
                    str="undefined"

                if where & ssl.SSL.SSL_CB_LOOP:
                    print "%s:%s" % (str, conn.state_string())
                elif where & ssl.SSL.SSL_CB_ALERT:
                    str = 'read' if where & ssl.SSL.SSL_CB_READ else 'write'
                    #print "SSL3 alert %s:%s:%s" % (str,
                    #        ssl.SSL.SSL_alert_type_string_long(ret),
                    #        ssl.SSL.SSL_alert_desc_string_long(ret))
                    print "SSL3 alert %s:%s" % (str, conn.state_string())
                elif where & ssl.SSL.SSL_CB_EXIT:
                    if ret == 0:
                        print "%s:failed in %s" % (str, conn.state_string())
                    elif ret < 0:
                        print "%s:error in %s" % (str, conn.state_string())
            except Exception, e:
                print "E:", e


        #self.method = ssl.SSL.SSLv23_METHOD
        self.method = ssl.SSL.TLSv1_METHOD
        ctx = ssl.ClientContextFactory.getContext(self)

        # We have no way of loading a chain from string buffer, let's do a temp file
        cert_fn = tempfile.NamedTemporaryFile(delete=False)
        cert_fn.write(self.cert) 
        cert_fn.close()
        ctx.use_certificate_chain_file(cert_fn.name)
        os.unlink(cert_fn.name)
        
        #ctx.use_certificate(load_certificate(FILETYPE_PEM, self.cert))
        ctx.use_privatekey(load_privatekey(FILETYPE_PEM, self.pkey))

        verifyFlags = ssl.SSL.VERIFY_NONE
        #verifyFlags = ssl.SSL.VERIFY_PEER #ssl.SSL.VERIFY_NONE
        #verifyFlags |= ssl.SSL.VERIFY_FAIL_IF_NO_PEER_CERT 
        #verifyFlags |= ssl.SSL.VERIFY_CLIENT_ONCE 
        def _verifyCallback(conn, cert, errno, depth, preverify_ok): 
            return preverify_ok 
        ctx.set_verify(verifyFlags, _verifyCallback) 
        #ctx.set_verify(ssl.SSL.VERIFY_PEER|ssl.SSL.VERIFY_FAIL_IF_NO_PEER_CERT|ssl.SSL.VERIFY_CLIENT_ONCE, _verifyCallback)

        #ctx.set_options(ssl.SSL.OP_NO_TLSv1 ) #| ssl.SSL.OP_NO_SSLv2 | ssl.SSL.OP_SINGLE_DH_USE )#| ssl.SSL.OP_NO_SSLv3)# | ssl.SSL.OP_SINGLE_DH_USE )
        #ctx.set_options(ssl.SSL.OP_ALL) 
        #ctx.set_options(ssl.SSL.OP_NO_TICKET) 

        #ctx.load_verify_locations(None, '/root/repos/tophat/test-ssl/crt/')
        #ctx.load_verify_locations(None, '/etc/ssl/certs/')

        #ctx.set_verify_depth(10)
        
        #server_store = ctx.get_cert_store()
        #f1 = open('/root/repos/tophat/test-ssl/myca1.pem').read()
        #f2 = open('/root/repos/tophat/test-ssl/myca2.pem').read()
        #f3 = open('/root/repos/tophat/test-ssl/myca3.pem').read()
        #f4 = open('/root/repos/tophat/test-ssl/crt/ple.pem').read()
        #server_store.add_cert(load_certificate(FILETYPE_PEM, f1));
        #server_store.add_cert(load_certificate(FILETYPE_PEM, f2));
        #server_store.add_cert(load_certificate(FILETYPE_PEM, f3));
        #server_store.add_cert(load_certificate(FILETYPE_PEM, f4));

        #ca1 = ssl.Certificate.loadPEM(open('myca1.pem').read())
        #ca2 = ssl.Certificate.loadPEM(open('myca2.pem').read())
        #store.add_cert(ca1.original) 
        #store.add_cert(ca2.original) 

        #ctx.set_info_callback(infoCallback)

        return ctx

class SFATokenMgr(object):
    """
    This singleton class is meant to regulate accesses to the different SFA API
    since some implementations of SFA such as SFAWrap are suspected to be
    broken with some configuration of concurrent connections.
    """
    __metaclass__ = Singleton

    BLACKLIST = ['https://www.planet-lab.eu:12345/']

    def __init__(self):
        self.busy     = {} # network -> Bool
        self.deferred = {} # network -> deferred corresponding to waiting queries

    def get_token(self, interface):
        #Log.debug("SFATokenMgr::get_token(interface=%r)" % interface)
        # We police queries only on blacklisted interfaces
        if not interface or interface not in self.BLACKLIST:
            return True

        # If the interface is not busy, the request can be done immediately
        if not (interface in self.busy and self.busy[interface]):
            return True

        # Otherwise we queue the request and return a Deferred that will get
        # activated when the queries terminates and triggers a put
        d = defer.Deferred()
        if not interface in self.deferred:
            #print "SFATokenMgr::get_token() - Deferring query to %s" % interface
            self.deferred[interface] = deque()
        self.deferred[interface].append(d)
        return d

    def put_token(self, interface):
        #Log.debug("SFATokenMgr::put_token(interface=%r)" % interface)
        # are there items waiting on queue for the same interface, if so, there are deferred that can be called
        # remember that the interface is being used for the query == available
        if not interface:
            return
        self.busy[interface] = False
        if interface in self.deferred and self.deferred[interface]:
            #print "SFATokenMgr::put_token() - Activating deferred query to %s" % interface
            d = self.deferred[interface].popleft()
            d.callback(True)
        pass
    

class SFAProxy(object):
    # Twisted HTTPS/XMLRPC inspired from
    # http://twistedmatrix.com/pipermail/twisted-python/2007-May/015357.html

#DEPRECATED#    def makeSSLContext(self, client_pem, trusted_ca_pem_list):
#DEPRECATED#        '''Returns an ssl Context Object
#DEPRECATED#       @param myKey a pem formated key and certifcate with for my current host
#DEPRECATED#              the other end of this connection must have the cert from the CA
#DEPRECATED#              that signed this key
#DEPRECATED#       @param trustedCA a pem formated certificat from a CA you trust
#DEPRECATED#              you will only allow connections from clients signed by this CA
#DEPRECATED#              and you will only allow connections to a server signed by this CA
#DEPRECATED#        '''
#DEPRECATED#
#DEPRECATED#        from twisted.internet import ssl
#DEPRECATED#
#DEPRECATED#        # our goal in here is to make a SSLContext object to pass to connectSSL
#DEPRECATED#        # or listenSSL
#DEPRECATED#
#DEPRECATED#        client_cert =  ssl.PrivateCertificate.loadPEM(client_pem)
#DEPRECATED#        # Why these functioins... Not sure...
#DEPRECATED#        if trusted_ca_pem_list:
#DEPRECATED#            ca = map(lambda x: ssl.PrivateCertificate.loadPEM(x), trusted_ca_pem_list)
#DEPRECATED#            ctx = client_cert.options(*ca)
#DEPRECATED#
#DEPRECATED#        else:
#DEPRECATED#            ctx = client_cert.options()
#DEPRECATED#
#DEPRECATED#        # Now the options you can set look like Standard OpenSSL Library options
#DEPRECATED#
#DEPRECATED#        # The SSL protocol to use, one of SSLv23_METHOD, SSLv2_METHOD,
#DEPRECATED#        # SSLv3_METHOD, TLSv1_METHOD. Defaults to TLSv1_METHOD.
#DEPRECATED#        ctx.method = ssl.SSL.TLSv1_METHOD
#DEPRECATED#
#DEPRECATED#        # If True, verify certificates received from the peer and fail
#DEPRECATED#        # the handshake if verification fails. Otherwise, allow anonymous
#DEPRECATED#        # sessions and sessions with certificates which fail validation.
#DEPRECATED#        ctx.verify = False #True
#DEPRECATED#
#DEPRECATED#        # Depth in certificate chain down to which to verify.
#DEPRECATED#        ctx.verifyDepth = 1
#DEPRECATED#
#DEPRECATED#        # If True, do not allow anonymous sessions.
#DEPRECATED#        ctx.requireCertification = True
#DEPRECATED#
#DEPRECATED#        # If True, do not re-verify the certificate on session resumption.
#DEPRECATED#        ctx.verifyOnce = True
#DEPRECATED#
#DEPRECATED#        # If True, generate a new key whenever ephemeral DH parameters are used
#DEPRECATED#        # to prevent small subgroup attacks.
#DEPRECATED#        ctx.enableSingleUseKeys = True
#DEPRECATED#
#DEPRECATED#        # If True, set a session ID on each context. This allows a shortened
#DEPRECATED#        # handshake to be used when a known client reconnects.
#DEPRECATED#        ctx.enableSessions = True
#DEPRECATED#
#DEPRECATED#        # If True, enable various non-spec protocol fixes for broken
#DEPRECATED#        # SSL implementations.
#DEPRECATED#        ctx.fixBrokenPeers = False
#DEPRECATED#
#DEPRECATED#        return ctx

    def __init__(self, interface, pkey, cert, timeout=DEFAULT_TIMEOUT):
        from twisted.web      import xmlrpc

        if not interface.startswith('http://') and not interface.startswith('https://'):
            interface = 'http://' + interface
        interface = interface.encode('latin1')

        class Proxy(xmlrpc.Proxy):
            ''' See: http://twistedmatrix.com/projects/web/documentation/howto/xmlrpc.html
                this is eacly like the xmlrpc.Proxy included in twisted but you can
                give it a SSLContext object insted of just accepting the defaults..
            '''
            def setSSLClientContext(self,SSLClientContext):
                self.SSLClientContext = SSLClientContext
            def callRemote(self, method, *args):
                def cancel(d):
                    factory.deferred = None
                    connector.disconnect()
                factory = self.queryFactory(
                    self.path, self.host, method, self.user,
                    self.password, self.allowNone, args, cancel, self.useDateTime)
                #factory = xmlrpc._QueryFactory(
                #    self.path, self.host, method, self.user,
                #    self.password, self.allowNone, args)

                if self.secure:
                    try:
                        self.SSLClientContext
                    except NameError:
                        print "Must Set a SSL Context"
                        print "use self.setSSLClientContext() first"
                        # Its very bad to connect to ssl without some kind of
                        # verfication of who your talking to
                        # Using the default sslcontext without verification
                        # Can lead to man in the middle attacks
                    ReactorThread().connectSSL(self.host, self.port or 443,
                                       factory, self.SSLClientContext,
                                       timeout=self.connectTimeout)

                else:
                   ReactorThread().connectTCP(self.host, self.port or 80, factory, timeout=self.connectTimeout)
                return factory.deferred

        # client_pem expects the concatenation of private key and certificate
        # We do not verify server certificates for now
        #client_pem = "%s\n%s" % (pkey, cert)
        #ctx = self.makeSSLContext(client_pem, None)
        ctx = CtxFactory(pkey, cert)

        self.proxy = Proxy(interface, allowNone=True, useDateTime=False)
        self.proxy.setSSLClientContext(ctx)
        self.network_hrn = None
        self.interface   = interface
        self.timeout     = timeout

    def get_interface(self):
        return self.interface

    def set_network_hrn(self, network_hrn):
        self.network_hrn = network_hrn

    def __getattr__(self, name):
        # We transfer missing methods to the remote server
        def _missing(*args):
            from twisted.internet import defer
            d = defer.Deferred()

            def proxy_success_cb(result):
                #SFATokenMgr().put_token(self.interface)
                diff = time.time() - self.started
                Log.tmp('SFA CALL SUCCESS %s(%s) - interface = %s - execution time = %s sec.' % (self.arg0, self.arg1, self.interface, round(diff,2)))
                d.callback(result)
            def proxy_error_cb(error):
                #SFATokenMgr().put_token(self.interface)
                diff = time.time() - self.started
                Log.tmp('SFA CALL ERROR %s(%s) - interface = %s - execution time = %s sec.' % (self.arg0, self.arg1, self.interface, round(diff,2)))
                d.errback(ValueError("Error in SFA Proxy %s" % error))

            #success_cb = lambda result: d.callback(result)
            #error_cb   = lambda error : d.errback(ValueError("Error in SFA Proxy %s" % error))

            def is_credential(cred):
                is_v2 = isinstance(cred, StringTypes) and cred.startswith(ARG_SNIFF_CRED)
                is_v3 = isinstance(cred, dict) and 'geni_type' in cred
                return is_v2 or is_v3

            def is_rspec(cred):
                return isinstance(cred, StringTypes) and cred.startswith(ARG_SNIFF_RSPEC)

            def is_credential_list(cred):
                return isinstance(cred, list) and reduce(lambda x, y: x and is_credential(y), cred, True)

            def is_user_list(arg):
                return isinstance(arg, dict) and 'geni_users' in arg

            #@defer.inlineCallbacks
            def wrap(source, args):
                #token = yield SFATokenMgr().get_token(self.interface)
                args = (name,) + args
                
                printable_args = []
                for arg in args:
                    if is_rspec(arg):
                        printable_args.append('<rspec>')
                    elif is_credential(arg):
                        printable_args.append('<credential>')
                    elif is_credential_list(arg):
                        printable_args.append('<credentials>')
                    elif is_user_list(arg):
                        printable_args.append('<user list>')
                    else:
                        printable_args.append(str(arg))

                self.started = time.time()
                self.arg0 = printable_args[0]
                self.arg1 = printable_args[1:]
                #Log.debug("SFA CALL %s(%s) - interface = %s" % (printable_args[0], printable_args[1:], self.interface))
                print "SFA CALL %s(%r) - interface = %s" % (printable_args[0], printable_args[1:], self.interface)
                self.proxy.callRemote(*args).addCallbacks(proxy_success_cb, proxy_error_cb)
                
            ReactorThread().callInReactor(wrap, self, args)
            return d
        return _missing

    def __str__(self):
        return "<SfaProxy %s>"% self.interface       

    def __repr__(self):
        return "<SfaProxy %s>"% self.interface
        
if __name__ == '__main__':
    from twisted.internet       import defer #, reactor
    from argparse               import ArgumentParser
    from manifold.core.query    import Query
    from manifold.util.storage  import DBStorage as Storage
    from manifold.gateways      import register_gateways
    import os, json, pprint

    DEFAULT_INTERFACE = 'https://www.planet-lab.eu:12346'
    DEFAULT_PLATFORM  = None
    DEFAULT_PKEY      = '/var/myslice/ple.upmc.slicebrowser.pkey'
    DEFAULT_CERT      = '/var/myslice/ple.upmc.slicebrowser.user.gid'
    DEFAULT_USER      = 'admin'
    DEFAULT_OPTIONS   = '{}'

    def execute(proxy, command, parameters, sfa_options, account_config):

        # Find user credentials
        creds = []

        sfa_parameters = list()

        if command != 'GetVersion':
            if parameters:
                sfa_parameters.append(parameters)

            if not 'user_credential' in account_config:
                raise Exception, "Missing user credential in account config for user '%s' on platform '%s'" % (args.user, args.platform)
            user_credential = account_config['user_credential']

            creds.append(user_credential)
            sfa_parameters.append(creds)
            sfa_parameters.append(sfa_options)

        return getattr(proxy, command)(*sfa_parameters)

    # XXX incompatible with something in Options()
    def init_options():
        usage="""%prog [options] [METHOD] [PARAMETERS]
  Issue an SFA call, using credentials from the manifold database."""

        parser = ArgumentParser()

        group = parser.add_mutually_exclusive_group()

        # We set default to None since we have no marker to detect that the
        # user has made a choice in case his choice is a default value
        # The default value is attributed later in the code
        group.add_argument("-i", "--interface", dest='interface',
                default = DEFAULT_INTERFACE,
                help = "Specify SFA interface. Default is %s" % DEFAULT_INTERFACE)
        group.add_argument("-p", "--platform", dest='platform',
                default = DEFAULT_PLATFORM,
                help = "Specify Manifold SFA platform. Default is %s" % DEFAULT_PLATFORM)

        parser.add_argument("-k", "--private_key", dest='private_key',
                default = DEFAULT_PKEY,
                help = "Specify SFA private key. Default is %s" % DEFAULT_PKEY)
        parser.add_argument("-c", "--certificate", dest='certificate',
                default = DEFAULT_CERT,
                help = "Specify SFA interface. Default is %s" % DEFAULT_CERT)
        parser.add_argument("-u", "--user", dest='user',
                default = DEFAULT_USER,
                help = "Specify the name of the user in the Manifold database. Default is %s" % DEFAULT_USER)
        parser.add_argument("-o", "--sfa-options", dest='sfa_options',
                default = DEFAULT_OPTIONS,
                help = "Specify options for the SFA call in JSON format. Default is %s" % DEFAULT_OPTIONS)

        # Positional arguments
        parser.add_argument('options', nargs='*')
        return parser

    def terminate():
        ReactorThread().stop_reactor()

    def main():

        parser = init_options()
        args = parser.parse_args()

        print "ARGS=" ,args

        # XXX Cannot both specify platform and interface
        # XXX THIS IS WRONG SINCE WE HAVE DEFAULT VALUES
        interface_specified = not not args.interface
        platform_specified  = not not args.platform
        # We are guaranteed either None or only one will be true

        if len(args.options) == 0:
            command    = 'GetVersion'
            parameters = []
        else:
            command    = args.options[0]
            parameters = args.options[1:]

        # Intercept SFA requests for adding credentials and options
        if not command in AGGREGATE_CALLS and not command in REGISTRY_CALLS:
            raise Exception, "Command not (yet) supported '%s'" % command

        # urn, user_creds, options
        if command != 'GetVersion' and not len(parameters) >= 1:
            parser.print_help()
            sys.exit(1)

        register_gateways()

        try:
            if platform_specified:
                
                platforms = Storage.execute(Query().get('platform').filter_by('platform', '==', args.platform).select('platform_id', 'config'))
                if not platforms:
                    raise Exception, "Platform '%s' not found" % args.platform
                platform = platforms[0]
                platform_id, platform_config = platform['platform_id'], platform['config']
                platform_config = json.loads(platforms[0]['config'])
                if command in AGGREGATE_CALLS:
                    if not 'sm' in platform_config:
                        raise Exception, "AM interface not found into platform '%s' configuration" % args.platform
                    interface = platform_config['sm']
                elif command in REGISTRY_CALLS:
                    if not 'registry' in platform_config:
                        raise Exception, "Registry interface not found into platform '%s' configuration" % args.platform
                    interface = platform_config['registry']
                else:
                    raise Exception, "Unknown interface"
            #elif interface_specified:
            #    print "interface specified"
            #    interface = args.interface
            else:
                interface = args.interface
                platform_id = None

            # Default = interface, only GetVersion() is allowed

            # XXX THIS IS CRAP !
            if command == 'GetVersion':
                account_config = {} # XXX TEMP HACK
            else:
                # User & account management
                users = Storage.execute(Query().get('user').filter_by('email', '==', args.user).select('user_id'))
                if not users:
                    raise Exception, "User %s not found in Manifold database"
                user = users[0]
                user_id = user['user_id']
                #
                accounts = Storage.execute(Query().get('account').filter_by('user_id', '==', user_id).filter_by('platform_id', '==', platform_id).select('config'))
                if not accounts:
                    raise Exception, "Account not found for user '%s' on platform '%s'" % (args.user, args.platform)
                account = accounts[0]
                account_config = json.loads(account['config'])

            # SFA options
            sfa_options_json =  args.sfa_options
            sfa_options = json.loads(sfa_options_json)

            # XXX interface or platform
            proxy = SFAProxy(interface, open(args.private_key).read(), open(args.certificate).read())
            print "Issueing SFA call: %s(%r)" % (command, parameters)
            # NOTE same error would occur with two proxies
            
            count = 0

#            import time
            class Callback(object):
                def __init__(self, callback=None):
                    self._count = 0
                    self._callback = callback
                def inc(self):
                    self._count += 1
                def dec(self):
                    self._count -= 1
                    if count ==0:
                        self._callback()
                def __call__(self, result):
                    #print len(result), "results"
                    print result
                    self.dec()

            cb = Callback(terminate)
#
#            cb.inc()
            cb.inc()

            d1 = execute(proxy, command, parameters, sfa_options, account_config)
            d1.callback = cb

#            # XXX It seems different errors are triggered depending on the timing
#            # time.sleep(0.1)
#            d2 = execute(proxy, command, parameters, sfa_options, account_config)
#            d2.callback = cb

        except Exception, e:
            print "Exception:", e
            import traceback
            traceback.print_exc()
        finally:
            pass


    ReactorThread().start_reactor()
    main()
