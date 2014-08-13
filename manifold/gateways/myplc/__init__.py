#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway managing MyPLC 
#   http://www.planet-lab.eu
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC

# Example:
# manifold-add-platform ple-myplc ple-myplc myplc none '{"username": "XXX", "password": "XXX"}' 0

from types                              import StringTypes

from manifold.core.record               import Record, LastRecord
from manifold.gateways                  import Gateway
from manifold.util.log                  import Log
from manifold.util.predicate            import eq, included
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns

API_URL = "https://www.planet-lab.eu:443/PLCAPI/"

MAP_METHOD = {
    "node" : "GetNodes",
    "site" : "GetSites",
    "myplcuser" : "GetPersons",
}

# XXX This could inherit from XMLRPC, like manifold_xmlrpc


class MyPLCGateway(Gateway):

    __gateway_name__ = "myplc"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router, platform, query, config, user_config, user):
        super(MyPLCGateway, self).__init__(router, platform, query, config, user_config, user)

        # The default xmlrpc.Proxy does not work, we need to use ReactorThread()...
        # XXX Share this code among manifold
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

        self._proxy = Proxy(API_URL.encode('latin-1'), allowNone = True)

        # XXX We should provide a default SSL Context...
        from twisted.internet import ssl
        self._proxy.setSSLClientContext(ssl.ClientContextFactory())

        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this gateway.
        """
        return "<MyPLCGateway %s>" % (API_URL,) #(self._platform_config['url'])

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    # XXX        nodes = srv.GetNodes(AUTH, {}, ['hostname', 'site_id'])
    def callback_records(self, rows):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            rows: The corresponding list of dict or Record instances.
        """
        for row in rows:
            self.send(Record(row))
        self.send(LastRecord())

    def callback_error(self, error):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            error: The corresponding error message.
        """
        Log.error("Error during Manifold call: %r" % error)
        self.send(LastRecord())

    @staticmethod
    def manifold_to_myplc_filter(filter):
        myplc_filter = dict()
        for predicate in filter:
            key, op, value = predicate.get_tuple()
            if op in [eq, included]:
                op_str = ''
            else:
                op_str = predicate.get_str_op()

            if isinstance(key, tuple):
                if len(key) == 1:
                    key = key[0]
                    print "VALUE=", value
                    value = value[0]
                else:
                    raise NotImplemented
            # tuples ?

            myplc_filter["%(op_str)s%(key)s" % locals()] = value
        if not myplc_filter:
            return None
        return myplc_filter

    def _get_auth(self):
        return {
            'AuthMethod': 'password',
            'Username'  : self.config['username'],
            'AuthString': self.config['password'],
        }

    def start(self):
        query = self.query
        table_name = query.get_from()

        # Need to transform manifold fields into query fields, and then back in
        # callback_records

        ## Insert a RENAME Node above this FROM Node if necessary.
        #instance = self.get_object(query.get_from())
        #aliases  = instance.get_aliases()
        #if aliases:
        #    Log.warning("I don't think this properly recables everything")
        #    try:
        #        Rename(self, aliases)
        #    except Exception, e:
        #        print "EEE:", e

        method = MAP_METHOD[query.get_from()]

        filters = MyPLCGateway.manifold_to_myplc_filter(query.get_where())
        fields = query.get_select()
        if fields:
            fields = list(fields)

        d = self._proxy.callRemote(method, self._get_auth(), filters, fields)
        d.addCallback(self.callback_records)
        d.addErrback(self.callback_error)
