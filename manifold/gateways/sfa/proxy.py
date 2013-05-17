import sys
from manifold.util.reactor_thread import ReactorThread

class SFAProxy(object):
    # Twisted HTTPS/XMLRPC inspired from
    # http://twistedmatrix.com/pipermail/twisted-python/2007-May/015357.html

    def makeSSLContext(self, client_pem, trusted_ca_pem):
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
        if trusted_ca_pem:
            trusted_ca = ssl.PrivateCertificate.loadPEM(trusted_ca_pem)
            ctx = client_cert.options(trusted_ca)
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

    def default_error_cb(self, error):
        print 'SFAProxy ERROR:', error

    def __init__(self, interface, pkey, cert, timeout):
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
                if not self.secure: # XXX swapped
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
        print "GETATTR", name
        # We transfer missing methods to the remote server
        def _missing(*args, **kwargs):
            try:
                success_cb = kwargs['success']
            except AttributeError:
                raise Exception, "Internal error: missing success callback to SFAProxy call"
            error_cb = kwargs.get('error', self.default_error_cb)

            def wrap(source, args, success_cb, error_cb):
                tmp = list(args)
                args = [name]
                args.extend(tmp)
                print "CALLREMOTE", args
                self.proxy.callRemote(*args).addCallbacks(success_cb, error_cb)
            ReactorThread().callInReactor(wrap, self, args, success_cb, error_cb) # run wrap(self) in the event loop
        return _missing
