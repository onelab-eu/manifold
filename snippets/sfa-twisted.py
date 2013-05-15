#!/usr/bin/env python
import sys
from twisted.web import xmlrpc, server
from twisted.internet import reactor, ssl
from twisted.python import log

def makeSSLContext(myKey,trustedCA):
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
    fd = open(trustedCA,'r')
    theCA = ssl.Certificate.loadPEM(fd.read())
    fd.close()
    ctx = theCert.options(theCA)

    # Now the options you can set look like Standard OpenSSL Library options

    # The SSL protocol to use, one of SSLv23_METHOD, SSLv2_METHOD,
    # SSLv3_METHOD, TLSv1_METHOD. Defaults to TLSv1_METHOD.
    ctx.method = ssl.SSL.TLSv1_METHOD

    # If True, verify certificates received from the peer and fail
    # the handshake if verification fails. Otherwise, allow anonymous
    # sessions and sessions with certificates which fail validation.
    ctx.verify = True

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


class Example(xmlrpc.XMLRPC):
    """An example object to be published.
       see: http://twistedmatrix.com/projects/web/documentation/howto/xmlrpc.html
    """

    def xmlrpc_echo(self, x):
        """Return all passed args."""
        log.msg('xmlrpc call echo, %s'%x)
        return x


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
    print repr(value)
    reactor.stop()

def printError(error):
    print 'error', error
    reactor.stop()

if __name__ == '__main__':
    # this should look pretty much like the examples given in the twisted
    # documents

    # Reading private key as PEM
    from sfa.trust.certificate import Keypair

    PKEY = '/home/augej/.sfi/ple.upmc.jordan_auge.pkey'
    myKey = Keypair(filename=PKEY).as_pem() # string=...

    print "running as", sys.argv[1]
    if sys.argv[1] == 'server':
        log.startLogging(sys.stdout)
        ctx = makeSSLContext(myKey='server.pem',trustedCA='cacert.pem')
        r = Example()
        reactor.listenSSL(7080, server.Site(r),ctx)
        reactor.run()
    elif sys.argv[1] == 'client':
        ctx = makeSSLContext(myKey=myKey, trustedCA=None)#'cacert.pem')
        proxy = Proxy('https://www.planet-lab.eu:12345/')
        proxy.setSSLClientContext(ctx)
        proxy.callRemote('GetVersion').addCallbacks(printValue, printError)
        reactor.run()
