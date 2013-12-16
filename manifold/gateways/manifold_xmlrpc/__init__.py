# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

from manifold.core.record           import Record, LastRecord
from manifold.gateways              import Gateway
from manifold.util.reactor_thread   import ReactorThread
#from twisted.internet import reactor

# DEBUG
import sys

class ManifoldGateway(Gateway):
    __gateway_name__ = 'manifold'

    def __str__(self):
        return "<ManifoldGateway %s %s>" % (self.config['url'], self.query)

    def success_cb(self, table):
        print "Manifold SUCCESS", len(table)
        for record in table:
            self.callback(Record(record))
        self.callback(LastRecord())

    def exception_cb(self, error):
        print 'Error during Manifold call: ', error
        self.callback(LastRecord())

    def start(self):
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
                    print "not ssl", self.host, self.port
                    ReactorThread().connectTCP(self.host, self.port or 80, factory, timeout=self.connectTimeout)
                return factory.deferred

        try:
            def wrap(source):
                print "XMLRPC:",  self.config['url']
                proxy = Proxy(self.config['url'].encode('latin-1'), allowNone = True)
                query = source.query
                auth = {'AuthMethod': 'guest'}

                # DEBUG
                if self.config['url'] == "https://api2.top-hat.info/API/":
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

                print "I: Issuing xmlrpc call to %r: %r" % (self.config['url'], query)

                print "=" * 100
                print "auth    =", auth
                print "method  =", query.object
                print "filters =", query.filters
                print "fields  =", query.fields
                print "ts      =", query.timestamp
                print "=" * 100

                print "query dict", query.to_dict()

                proxy.callRemote(
                    'forward',
                    query.to_dict(),
                    {'authentication': auth}
                ).addCallbacks(source.success_cb, source.exception_cb)
                print "done call"

            #reactor.callFromThread(wrap, self) # run wrap(self) in the event loop
            wrap(self)
            print "done wrap"
            
        except Exception, e:
            print "Exception in Manifold::start", e
