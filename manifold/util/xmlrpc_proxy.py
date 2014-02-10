#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Implements a XMLRPC proxy
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                          import StringTypes
from twisted.web                    import xmlrpc

from manifold.util.reactor_thread   import ReactorThread
from manifold.util.type             import accepts, returns

class XMLRPCProxy(xmlrpc.Proxy):
    def __init__(self, url, user=None, password=None, allowNone=False, useDateTime=False, connectTimeout=30.0, reactor=None): # XXX
        """
        Constructor.
        Args:
            See http://twistedmatrix.com/documents/12.2.0/api/twisted.web.xmlrpc.Proxy.html
        """
        import threading
        xmlrpc.Proxy.__init__(self, url, user, password, allowNone, useDateTime, connectTimeout, reactor)
        self.url = url
        self.event = threading.Event() 
        self.result = None
        self.error = None

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Proxy.
        """
        return "<XMLRPC client to %s>" % self.url

    def setSSLClientContext(self, SSLClientContext):
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

    def __getattr__(self, name):
        # We transfer missing methods to the remote server
        # XXX Let's not use twisted if we write synchronous code
        def _missing(*args):
            from twisted.internet import defer
            d = defer.Deferred()
            
            def proxy_success_cb(result):
                print "success", result
                self.result = result
                self.event.set()

            def proxy_error_cb(failure):
                print "error", failure
                self.error = failure
                self.event.set()
            
            #@defer.inlineCallbacks
            def wrap(source, args):
                args = (name,) + args
                self.callRemote(*args).addCallbacks(proxy_success_cb, proxy_error_cb)
            
            ReactorThread().callInReactor(wrap, self, args)
            self.event.wait()
            self.event.clear()
            if self.error:
                failure = self.error
                self.error = None
                raise Exception, "Error in proxy: %s" % failure # .trap(Exception)

            result = self.result
            self.result = None
            return result
        return _missing


