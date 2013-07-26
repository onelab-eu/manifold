#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, tempfile
from manifold.util.reactor_thread import ReactorThread
from manifold.util.log            import Log
from manifold.util.singleton      import Singleton
from twisted.internet             import ssl
from OpenSSL.crypto               import TYPE_RSA, FILETYPE_PEM
from OpenSSL.crypto               import load_certificate, load_privatekey
from twisted.internet             import defer

DEFAULT_TIMEOUT = 20

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

    BLACKLIST = ['ple', 'omf']

    def __init__(self):
        self.busy     = {} # network -> Bool
        self.deferred = {} # network -> deferred corresponding to waiting queries

    def get_token(self, network):
        #print "SFATokenMgr::get_token(network=%r)" % network
        # We police queries only on blacklisted networks
        if not network or network not in self.BLACKLIST:
            return True

        # If the network is not busy, the request can be done immediately
        if not (network in self.busy and self.busy[network]):
            return True

        # Otherwise we queue the request and return a Deferred that will get
        # activated when the queries terminates and triggers a put
        d = defer.Deferred()
        if not network in self.deferred:
            #print "SFATokenMgr::get_token() - Deferring query to %s" % network
            self.deferred[network] = deque()
        self.deferred[network].append(d)
        return d

    def put_token(self, network):
        #print "SFATokenMgr::put_token(network=%r)" % network
        # are there items waiting on queue for the same network, if so, there are deferred that can be called
        # remember that the network is being used for the query == available
        if not network:
            return
        self.busy[network] = False
        if network in self.deferred and self.deferred[network]:
            #print "SFATokenMgr::put_token() - Activating deferred query to %s" % network
            d = self.deferred[network].popleft()
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
        #from twisted.internet import reactor
        class Proxy(xmlrpc.Proxy):
            ''' See: http://twistedmatrix.com/projects/web/documentation/howto/xmlrpc.html
                this is eacly like the xmlrpc.Proxy included in twisted but you can
                give it a SSLContext object insted of just accepting the defaults..
            '''
            def setSSLClientContext(self,SSLClientContext):
                self.SSLClientContext = SSLClientContext
            def callRemote(self, method, *args):
                factory = xmlrpc._QueryFactory(
                    self.path, self.host, method, self.user,
                    self.password, self.allowNone, args)
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
                                       factory,self.SSLClientContext)
                else:
                   ReactorThread().connectTCP(self.host, self.port or 80, factory)
                return factory.deferred

        # client_pem expects the concatenation of private key and certificate
        # We do not verify server certificates for now
        #client_pem = "%s\n%s" % (pkey, cert)
        #ctx = self.makeSSLContext(client_pem, None)
        ctx = CtxFactory(pkey, cert)

        self.proxy = Proxy(interface, allowNone=True)
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
                SFATokenMgr().put_token(self.network_hrn)
                d.callback(result)
            def proxy_error_cb(error):
                SFATokenMgr().put_token(self.network_hrn)
                d.errback(ValueError("Error in SFA Proxy %s" % error))

            #success_cb = lambda result: d.callback(result)
            #error_cb   = lambda error : d.errback(ValueError("Error in SFA Proxy %s" % error))
            
            @defer.inlineCallbacks
            def wrap(source, args):
                token = yield SFATokenMgr().get_token(self.network_hrn)
                args = (name,) + args
                self.proxy.callRemote(*args).addCallbacks(proxy_success_cb, proxy_error_cb)
            
            ReactorThread().callInReactor(wrap, self, args)
            return d
        return _missing

    def __str__(self):
        return "<SfaProxy %s>"% self.interface       

    def __repr__(self):
        return "<SfaProxy %s>"% self.interface
        
if __name__ == '__main__':
    from twisted.internet import defer #, reactor
    import os, pprint

    DEFAULT_INTERFACE = 'https://www.planet-lab.eu:12346'
    DEFAULT_PKEY      = '/var/myslice/ple.upmc.slicebrowser.pkey'
    DEFAULT_CERT      = '/var/myslice/ple.upmc.slicebrowser.user.gid'

    @defer.inlineCallbacks
    def main():
        l = len(sys.argv)
        if l not in range(1, 5):
            print "%s : Issues a GetVersion asynchronously towards a SFA interface" % sys.argv[0]
            print
            print "Usage: %s [INTERFACE PRIVATE_KEY CERTIFICATE]" % sys.argv[0]
            print "Default values:"
            print "    INTERFACE  : %s" % DEFAULT_INTERFACE
            print "    PRIVATE_KEY: %s" % DEFAULT_PKEY
            print "    CERTIFICATE: %s" % DEFAULT_CERT
            os._exit(1)

        interface = DEFAULT_INTERFACE if l <= 1 else sys.argv[1]
        pkey      = DEFAULT_PKEY      if l <= 2 else sys.argv[2]
        cert      = DEFAULT_CERT      if l <= 3 else sys.argv[3]

        proxy = SFAProxy(interface, open(pkey).read(), open(cert).read())
        version = yield proxy.GetVersion()

        pprint.pprint(version)
        ReactorThread().stop_reactor()


    ReactorThread().start_reactor()
    main()
