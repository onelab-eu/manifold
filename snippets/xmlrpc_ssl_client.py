#!/usr/bin/env python
# -*- coding: utf-8 -*-

USE_CLIENT_CERT = True # False = use password

from OpenSSL import SSL
from twisted.internet import ssl, reactor
from twisted.internet.protocol import ClientFactory, Protocol
from twisted.web      import xmlrpc
import pprint
from manifold.core.query import Query

class CtxFactory(ssl.ClientContextFactory):
    def getContext(self):
        self.method = SSL.SSLv23_METHOD
        ctx = ssl.ClientContextFactory.getContext(self)

        ctx.use_certificate_chain_file('/etc/manifold/keys/client.crt')
        ctx.use_privatekey_file('/etc/manifold/keys/client.key')

        return ctx

def proxy_success_cb(result):
    pprint.pprint(result)
    reactor.stop()

def proxy_error_cb(error):
    print "E: %s" % error
    reactor.stop()

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
            reactor.connectSSL(self.host, self.port or 443,
                               factory, self.SSLClientContext,
                               timeout=self.connectTimeout)

        else:
           reactor.connectTCP(self.host, self.port or 80, factory, timeout=self.connectTimeout)
        return factory.deferred

    def __getattr__(self, name):
        def _missing(*args):
            from twisted.internet import defer
            d = defer.Deferred()

            success_cb = lambda result: d.callback(result)
            error_cb   = lambda error : d.errback(ValueError("Proxy %s" % error))
            
            @defer.inlineCallbacks
            def wrap(source, args):
                token = yield SFATokenMgr().get_token(self.interface)
                args = (name,) + args
                
                self.callRemote(*args).addCallbacks(proxy_success_cb, proxy_error_cb)
            
            ReactorThread().callInReactor(wrap, self, args)
            return d
        return _missing

if __name__ == '__main__':

    query = Query.get('local:platform').select('platform')

    proxy = Proxy('https://localhost:7080/', allowNone=True, useDateTime=False)

    if USE_CLIENT_CERT:
        annotations = {}
        # We need this to define the private key and certificate (GID) to use
        proxy.setSSLClientContext(CtxFactory()) 
    else:
        annotations = {
            'authentication': {'AuthMethod': 'password', 'Username': 'demo', 'AuthString': 'demo'}
        }
        proxy.setSSLClientContext(ssl.ClientContextFactory())

    proxy.callRemote('forward', query.to_dict(), annotations).addCallbacks(proxy_success_cb, proxy_error_cb)

    reactor.run()
