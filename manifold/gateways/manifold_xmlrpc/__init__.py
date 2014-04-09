#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Gateway speak to another Manifold Router by
# using XMLRPC calls.
# Inspired from:
#   http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

from types                              import StringTypes
#from twisted.internet                  import reactor

from manifold.gateways                  import Gateway
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns 

class ManifoldGateway(Gateway):
    __gateway_name__ = 'manifold'

    def __init__(self, interface, platform, platform_config):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        super(ManifoldGateway, self).__init__(interface, platform, platform_config)
        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this ManifoldGateway.
        """
        return "<ManifoldGateway %s>" % (self._platform_config['url'])

    def callback_records(self, rows, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            rows: The corresponding list of dict or Record instances.
        """
        self.records(rows, packet)

    def callback_error(self, error, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            error: The corresponding error message. 
        """
        self.error(packet, "Error during Manifold call: %r" % error)

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet (processing).
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        annotation = packet.get_annotation()
        receiver = packet.get_receiver()

        from twisted.web import xmlrpc 
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

        #try:
        #    def wrap(source):
        proxy = Proxy(self._platform_config['url'].encode('latin-1'), allowNone = True)
        #query = source.query
        auth = {'AuthMethod': 'guest'}

        d = proxy.callRemote(
            'forward',
            query.to_dict(),
            {'authentication': auth}
        )
        d.addCallback(self.callback_records, packet)
        d.addErrback(self.callback_error, packet)

            #reactor.callFromThread(wrap, self) # run wrap(self) in the event loop
        #    wrap(self)
        #    print "done wrap"
        #    
        #except Exception, e:
        #    print "Exception in Manifold::start", e

    def get_metadata(self):
        pass
        
