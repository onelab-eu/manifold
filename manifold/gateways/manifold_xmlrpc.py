# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

from __future__ import absolute_import
#from manifold.gateways import Gateway
#from twisted.web.xmlrpc import Proxy
#from twisted.internet import reactor

# DEBUG
import sys

class ManifoldGateway(object):#Gateway):

    def __str__(self):
        return "<ManifoldGateway %s %s>" % (self.config['url'], self.query)

    def success_cb(self, table):
        print "Manifold SUCCESS", len(table)
        for record in table:
            self.callback(record)
        self.callback(None)

    def exception_cb(self, error):
        print 'Error during Manifold call: ', error

    def start(self):
        try:
            def wrap(source):
                proxy = Proxy(self.config['url'], allowNone = True)
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

                print "I: Issuing xmlrpc call to %s: %s" % (self.config['url'], query)

                print "=" * 100
                print "auth    =", auth
                print "method  =", query.fact_table
                print "filters =", query.filters
                print "fields  =", query.fields
                print "ts      =", query.ts
                print "=" * 100

                print proxy
                proxy.callRemote(
                    'Get',
                    auth,
                    query.fact_table,
                    query.ts,
                    query.filters,
                    list(query.fields)
                ).addCallbacks(source.success_cb, source.exception_cb)

            #reactor.callFromThread(wrap, self) # run wrap(self) in the event loop
            wrap(self)
            
        except Exception, e:
            print "Exception in Manifold::start", e
