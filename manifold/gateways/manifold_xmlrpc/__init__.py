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
from manifold.core.query                import Query
from manifold.core.query_factory        import QueryFactory
from manifold.core.result_value         import ResultValue
from manifold.gateways                  import Gateway
from manifold.util.callback             import Callback
from manifold.util.log                  import Log
from manifold.util.reactor_thread       import ReactorThread, ReactorException
from manifold.util.type                 import accepts, returns

GUEST_AUTH = {"AuthMethod": "anonymous"}

TIMEOUT = 3 # in seconds

class ManifoldGateway(Gateway):
    __gateway_name__ = "manifold"

    def __init__(self, router, platform, platform_config):
        """
        Constructor
        Args:
            router: The Router on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        super(ManifoldGateway, self).__init__(router, platform, platform_config)

        from twisted.web        import xmlrpc
        from twisted.internet   import ssl

        class Proxy(xmlrpc.Proxy):
            """
            See: http://twistedmatrix.com/projects/web/documentation/howto/xmlrpc.html
            this is eacly like the xmlrpc.Proxy included in twisted but you can
            give it a SSLContext object insted of just accepting the defaults..
            """
            def setSSLClientContext(self,SSLClientContext):
                self.SSLClientContext = SSLClientContext

            def callRemote(self, method, *args):

                def cancel(d):
                    factory.deferred = None
                    connector.disconnect()

                factory = self.queryFactory(
                    self.path, self.host, method, self.user,
                    self.password, self.allowNone, args, cancel, self.useDateTime
                )
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
                        # verification of who your talking to
                        # Using the default sslcontext without verification
                        # Can lead to man in the middle attacks
                    ReactorThread().connectSSL(
                        self.host,
                        self.port or 443,
                        factory,
                        self.SSLClientContext,
                        timeout = self.connectTimeout
                    )
                else:
                    ReactorThread().connectTCP(
                        self.host,
                        self.port or 80,
                        factory,
                        timeout = self.connectTimeout
                    )
                return factory.deferred

        try:
            self._proxy = Proxy(self._platform_config["url"].encode("latin-1"), allowNone = True)
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
        try:
            peer_url = self.get_config()["url"]
        except KeyError:
            peer_url = "???"
        return "<ManifoldGateway %s>" % peer_url

    def callback_records(self, result_value, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            result_value: A ResultValue instance.
            packet: A QueryPacket instance.
        """
        assert isinstance(result_value, ResultValue)
        if not result_value.is_success():
            raise RuntimeError("Error while repatriating records")
        # NOTE Any error in the chain will trigger error callback, and we will
        # have the last record send twice !!!
        self.records(result_value.get_all().to_dict_list(), packet)


    def callback_error(self, error, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            error: A Failure instance.
            packet: A QueryPacket instance.
        """
        error = ("%s" % error).replace("\n", "")
        self.error(packet, "While dialing [%(platform_name)s] using %(platform_config)s: %(error)s" % {
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
        query = QueryFactory.from_packet(packet)
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
        callback = Callback()

        # qualifier name type description is_array # XXX missing origin
        query_metadata = Query.get("local:object").select("object_name", "columns", "key", "capabilities").to_dict()

        def errback(failure):
            message = "Cannot get announces from %s: %s" % (self.get_platform_name(), failure)
            Log.error(message)
            callback(ResultValue.error(message))

        deferred = self._proxy.callRemote("forward", query_metadata, {"authentication": GUEST_AUTH})
        deferred.addCallback(callback)
        deferred.addErrback(errback)

        result_value = callback.get_results()
        # This should be a ResultValue !!
        Log.warning("result_value is not always a ResultValue !")
        # << crappy hook
        if isinstance(result_value, list):
            records = result_value
            return Announces.from_dict_list(records, self.get_platform_name())
        # >>
        elif not isinstance(result_value, ResultValue):
            raise TypeError("Invalid result value: %s (%s)" % (result_value, type(ResultValue)))
        elif not result_value.is_success():
            raise RuntimeError("Error while repatriating metadata")
        else:
            # The crappy hook should not occur and lead to this part of the code
            return Announces.from_dict_list(result_value.get_all().to_dict_list(), self.get_platform_name())
