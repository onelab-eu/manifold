
import sys
from twisted.web import xmlrpc, server
from twisted.internet import reactor, ssl
from twisted.python import log

class SFAProxy(object):
    # Twisted HTTPS/XMLRPC inspired from
    # http://twistedmatrix.com/pipermail/twisted-python/2007-May/015357.html

    def makeSSLContext(self, myKey,trustedCA):
        '''Returns an ssl Context Object
       @param myKey a pem formated key and certifcate with for my current host
              the other end of this connection must have the cert from the CA
              that signed this key
       @param trustedCA a pem formated certificat from a CA you trust
              you will only allow connections from clients signed by this CA
              and you will only allow connections to a server signed by this CA
        '''

        # our goal in here is to make a SSLContext object to pass to connectSSL
        # or listenSSL

        # Why these functioins... Not sure...
        fd = open(myKey,'r')
        theCert = ssl.PrivateCertificate.loadPEM(fd.read())
        fd.close()
        if trustedCA:
            fd = open(trustedCA,'r')
            theCA = ssl.Certificate.loadPEM(fd.read())
            fd.close()
            ctx = theCert.options(theCA)
        else:
            ctx = theCert.options()

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

    def printValue(value):
        print "SFAProxy SUCCESS:", repr(value)
        #reactor.stop()

    def printError(error):
        print 'SFAProxy ERROR:', error
        #reactor.stop()

    def __init__(self, url, user):
        ctx = self.makeSSLContext(myKey='client.pem', trustedCA=None)#'cacert.pem')
        self.proxy = Proxy(url, allowNone=True)
        self.proxy.setSSLClientContext(ctx)

        # Reading private key as PEM
        #PKEY = '/home/augej/.sfi/ple.upmc.jordan_auge.pkey'
        #from sfa.trust.certificate import Keypair
        #myKey = Keypair(filename=PKEY).as_pem() # string=...

        #CRED = '/home/augej/.sfi/ple.upmc.jordan_auge.user.cred'
        #proxy.callRemote('ListResources', open(CRED).read(), {'rspec_version': 'SFA 1'}).addCallbacks(printValue, printError)
        #reactor.run()

    def __getattr__(self, name):
        # We transfer missing methods to the remote server
        def _missing(*args, **kwargs):
            if kwargs:
                raise Exception, "Cannot use named arguments with XMLRPC functions"
            args = [name].extend(list(args))
            self.proxy.callRemote(*args).addCallbacks(printValue, printError)
            getattr(self.server, name)(*args, **kwargs)
        return _missing
