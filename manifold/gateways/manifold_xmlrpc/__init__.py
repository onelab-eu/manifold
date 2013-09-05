# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

from manifold.gateways       import Gateway
from manifold.operators      import LAST_RECORD
from manifold.util.log       import Log
#from twisted.internet import reactor

class ManifoldGateway(Gateway):

    def __str__(self):
        return "<ManifoldGateway %s %s>" % (self.config['url'], self.query)

    def success_cb(self, records):
        Log.info("Manifold SUCCESS: %s records" % len(records))
        for record in records:
            self.callback(record)
        self.callback(LAST_RECORD)

    def exception_cb(self, error):
        Log.warning("Error during Manifold call: %s" % error)
        self.callback(LAST_RECORD)

    def start(self):
        from twisted.web.xmlrpc import Proxy
        try:
            def wrap(source):
                proxy = Proxy(self.config["url"], allowNone = True)
                query = source.query
                auth = {"AuthMethod": "guest"}

                # DEBUG
                if self.config["url"] == "https://api2.top-hat.info/API/":
                    Log.warning("Hardcoding XML RPC call")

                    # Where conversion
                    filters = {}
                    for predicate in query.get_where():
                        field = "%s%s" % ("" if predicate.get_str_op() == "=" else predicate.get_op(), predicate.get_key())
                        if field not in filters:
                            filters[field] = list() 
                        filters[field] += list(predicate.get_value())
                    for field, value in filters.iteritems():
                        if len(value) == 1:
                            filters[field] = value[0]
                    query.filters = filters

                Log.info("Issuing xmlrpc call to %s: %s" % (self.config["url"], query))
                proxy.callRemote(
                    "Get",
                    auth,
                    query.get_from(),
                    query.get_timestamp(),
                    query.get_where(),
                    list(query.get_select())
                ).addCallbacks(source.success_cb, source.exception_cb)

            #reactor.callFromThread(wrap, self) # run wrap(self) in the event loop
            wrap(self)
            
        except Exception, e:
            Log.warning("Exception in Manifold::start: %s" % e)
