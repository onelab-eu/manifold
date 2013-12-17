# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

from manifold.core.record           import LastRecord
from manifold.gateways              import Gateway
from manifold.util.reactor_thread   import ReactorThread
#from twisted.internet import reactor

# DEBUG
import sys

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

    def __str__(self):
        return "<ManifoldGateway %s %s>" % (self._platform_config['url'], self.query)

    def success_cb(self, table, query):
        # I hope this can be simplified
        socket = self.get_pit().get_socket(query)
        self.records(socket, table)

    def exception_cb(self, error, query):
        #print 'Error during Manifold call: ', error
        socket = self.get_pit().get_socket(query)
        self.error(socket, 'Error during Manifold call: %r' % error)

    def receive(self, packet):

        # The packet is a query packet
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

        # DEBUG
        if self._platform_config['url'] == "https://api2.top-hat.info/API/":
            print "W: Hardcoding XML RPC call"

            # Where conversion
            filters = {}
            for predicate in query.filters:
                field = "%s%s" % ('' if predicate.get_str_op() == "=" else predicate.op, predicate.key)
                if field not in filters:
                    filters[field] = []
                filters[field].append(predicate.value)
            for field, value in filters.iteritems():
                if len(value) == 1:
                    filters[field] = value[0]
            query.filters = filters

        #print "I: Issuing xmlrpc call to %r: %r" % (self._platform_config['url'], query)
        #print "=" * 100
        #print "auth    =", auth
        #print "method  =", query.object
        #print "filters =", query.filters
        #print "fields  =", query.fields
        #print "ts      =", query.timestamp
        #print "=" * 100


        proxy.callRemote(
            'forward',
            query.to_dict(),
            {'authentication': auth}
        ).addCallback(self.success_cb, query).addErrback(self.exception_cb, query)

            #reactor.callFromThread(wrap, self) # run wrap(self) in the event loop
        #    wrap(self)
        #    print "done wrap"
        #    
        #except Exception, e:
        #    print "Exception in Manifold::start", e

    def get_metadata(self):
        pass
        
