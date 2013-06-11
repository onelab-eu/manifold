#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from manifold.util.reactor_thread import ReactorThread
from manifold.util.log            import Log
from manifold.util.misc           import make_list

DEFAULT_TIMEOUT = 20

class SFAProxy(object):
    # Twisted HTTPS/XMLRPC inspired from
    # http://twistedmatrix.com/pipermail/twisted-python/2007-May/015357.html

    def makeSSLContext(self, client_pem, trusted_ca_pem_list):
        '''Returns an ssl Context Object
       @param myKey a pem formated key and certifcate with for my current host
              the other end of this connection must have the cert from the CA
              that signed this key
       @param trustedCA a pem formated certificat from a CA you trust
              you will only allow connections from clients signed by this CA
              and you will only allow connections to a server signed by this CA
        '''

        from twisted.internet import ssl

        # our goal in here is to make a SSLContext object to pass to connectSSL
        # or listenSSL

        client_cert =  ssl.PrivateCertificate.loadPEM(client_pem)
        # Why these functioins... Not sure...
        if trusted_ca_pem_list:
            ca = map(lambda x: ssl.PrivateCertificate.loadPEM(x), trusted_ca_pem_list)
            ctx = client_cert.options(*ca)

        else:
            ctx = client_cert.options()

        # Now the options you can set look like Standard OpenSSL Library options

        # The SSL protocol to use, one of SSLv23_METHOD, SSLv2_METHOD,
        # SSLv3_METHOD, TLSv1_METHOD. Defaults to TLSv1_METHOD.
        ctx.method = ssl.SSL.TLSv1_METHOD

        # If True, verify certificates received from the peer and fail
        # the handshake if verification fails. Otherwise, allow anonymous
        # sessions and sessions with certificates which fail validation.
        ctx.verify = False #True

        # Depth in certificate chain down to which to verify.
        ctx.verifyDepth = 1

        # If True, do not allow anonymous sessions.
        ctx.requireCertification = True

        # If True, do not re-verify the certificate on session resumption.
        ctx.verifyOnce = True

        # If True, generate a new key whenever ephemeral DH parameters are used
        # to prevent small subgroup attacks.
        ctx.enableSingleUseKeys = True

        # If True, set a session ID on each context. This allows a shortened
        # handshake to be used when a known client reconnects.
        ctx.enableSessions = True

        # If True, enable various non-spec protocol fixes for broken
        # SSL implementations.
        ctx.fixBrokenPeers = False

        return ctx

    def __init__(self, interface, pkey, cert, timeout=DEFAULT_TIMEOUT):
        from twisted.web      import xmlrpc
        from twisted.internet import reactor
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
                    from twisted.internet import ssl
                    try:
                        self.SSLClientContext
                    except NameError:
                        print "Must Set a SSL Context"
                        print "use self.setSSLClientContext() first"
                        # Its very bad to connect to ssl without some kind of
                        # verfication of who your talking to
                        # Using the default sslcontext without verification
                        # Can lead to man in the middle attacks
                    reactor.connectSSL(self.host, self.port or 443,
                                       factory,self.SSLClientContext)
                else:
                    reactor.connectTCP(self.host, self.port or 80, factory)
                return factory.deferred

        # client_pem expects the concatenation of private key and certificate
        # We do not verify server certificates for now
        client_pem = "%s\n%s" % (pkey, cert)
        ctx = self.makeSSLContext(client_pem, None)

        self.proxy = Proxy(interface, allowNone=True)
        self.proxy.setSSLClientContext(ctx)
        self.interface = interface
        self.timeout = timeout

    def get_interface(self):
        return self.interface

    def __getattr__(self, name):
        # We transfer missing methods to the remote server
        def _missing(*args):
            from twisted.internet import defer
            d = defer.Deferred()
            success_cb = lambda result: d.callback(result)
            error_cb   = lambda error : d.errback(ValueError("Error in SFA Proxy %s" % error))
            
            def wrap(source, args):
                args = (name,) + args
                Log.tmp(self)
                Log.tmp(make_list(args))
                return self.proxy.callRemote(*args).addCallbacks(success_cb, error_cb)
            
            ReactorThread().callInReactor(wrap, self, args)
            return d
        return _missing

    def __str__(self):
        return "<SfaProxy %s>"% self.interface       

    def __repr__(self):
        return "<SfaProxy %s>"% self.interface
        
if __name__ == '__main__':
    from twisted.internet import defer, reactor
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

        reactor.stop()

    main()
    reactor.run()
