# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

from twisted.web.xmlrpc import Proxy
from tophat.core.nodes import SourceNode



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
        try:
            import xmlrpclib
            self.proxy = Proxy(self.config['url'], allowNone=True)
            q = self.query
            d = q.destination

            auth = {'AuthMethod': 'guest'}
            #print "I: Issueing xmlrpc call to %s: %s" % (self.config['url'], q)
            self.proxy.callRemote('Get', auth, d.fact_table, 'now', d.filters, list(d.fields)).addCallbacks(self.success_cb, self.exception_cb)
        except Exception, e:
            print "Exception in XMLRPC::start", e
