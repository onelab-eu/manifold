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

import threading
from types                              import StringTypes
#from twisted.internet                  import reactor

from manifold.core.announce             import Announces
from manifold.core.fields               import Fields
from manifold.core.query                import Query
from manifold.gateways                  import Gateway
from manifold.util.callback             import Callback
from manifold.util.log                  import Log
from manifold.util.reactor_thread       import ReactorThread, ReactorException
from manifold.util.type                 import accepts, returns

GUEST_AUTH = {'AuthMethod': 'anonymous'}

TIMEOUT = 3 # in seconds

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

        from twisted.web import xmlrpc
        from twisted.internet import ssl

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

                self.connectTimeout = TIMEOUT
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

        try:
            self._proxy = Proxy(self._platform_config['url'].encode('latin-1'), allowNone = True)
        except KeyError, e:
            Log.error("While loading %s (%s): %s" % (self.get_platform_name(), self.get_config(), e))
            raise e
        ctx = ssl.ClientContextFactory()
        self._proxy.setSSLClientContext(ctx)

        ReactorThread().start_reactor()

    def terminate(self):
        try:
            ReactorThread().stop_reactor()
        except ReactorException:
            Log.info("Nothing to do, the reactor was already stopped")
            pass

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this ManifoldGateway.
        """
        return "<ManifoldGateway %s>" % (self._platform_config['url'])

    def callback_records(self, result_value, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            rows: The corresponding list of dict or Record instances.
        """
        if not 'code' in result_value:
            raise Exception, "Invalid result value"
        if result_value['code'] != 0:
            raise Exception, "Error while repatriating metadata"
        self.records(result_value['value'], packet)

    def callback_error(self, error, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            error: A Failure instance.
        """
        error = ("%s" % error).replace("\n", "")
        self.error(packet, "while dialing [%(platform_name)s] using %(platform_config)s: %(error)s" % {
            "platform_name"   : self.get_platform_name(),
            "platform_config" : self.get_config(),
            "error"           : error
        })

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet (processing).
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        annotation = packet.get_annotation()
        receiver = packet.get_receiver()

        deferred = self._proxy.callRemote(
            'forward',
            query.to_dict(),
            {'authentication': GUEST_AUTH}
        )
        deferred.addCallback(self.callback_records, packet)
        deferred.addErrback(self.callback_error, packet)

    @returns(Announces)
    def make_announces(self):
        """
        Returns:
            The Announce related to this object.
        """
        cb = Callback()

        # qualifier name type description is_array # XXX missing origin
        query_metadata = Query.get('local:object').select('table', 'columns', 'key', 'capabilities').to_dict()

        def errb(failure):
            print "failure:", failure
            cb([])

        deferred = self._proxy.callRemote('forward', query_metadata, {'authentication': GUEST_AUTH})
        deferred.addCallback(cb)
        deferred.addErrback(errb)

        result_value = cb.get_results()
        if not 'code' in result_value:
            raise Exception, "Invalid result value"
        if result_value['code'] != 0:
            raise Exception, "Error while repatriating metadata"
        return Announces.from_dict_list(result_value['value'], self.get_platform_name())
