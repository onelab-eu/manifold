# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

from twisted.web.xmlrpc import Proxy
from tophat.core.nodes import SourceNode
from twisted.internet import reactor



class XMLRPC(SourceNode):

    def __str__(self):
        return "<XMLRPCGateway %s %s>" % (self.config['url'], self.query)

    def success_cb(self, table):
        for record in table:
            self._callback(record)
        self._callback(None)

    def exception_cb(self, error):
        print 'Error during XMLRPC call: ', error

    def start(self):
        self.started = True
        try:
            def wrap(source):
                proxy = Proxy(self.config['url'], allowNone=True)
                query = source.query
                d = query.destination
                auth = {'AuthMethod': 'guest'}
                print "I: Issueing xmlrpc call to %s: %s" % (self.config['url'], query)
                proxy.callRemote('Get', auth, d.fact_table, 'now', d.filters, list(d.fields)).addCallbacks(source.success_cb, source.exception_cb)
            reactor.callFromThread(wrap, self) 
        except Exception, e:
            print "Exception in XMLRPC::start", e
