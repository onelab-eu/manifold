# Inspired from http://twistedmatrix.com/documents/10.1.0/web/howto/xmlrpc.html

from twisted.web.xmlrpc import Proxy
from tophat.core.ast import FromNode
from twisted.internet import reactor



class XMLRPC(FromNode):

    def __str__(self):
        return "<XMLRPCGateway %s %s>" % (self.gateway_config['url'], self.query)

    def success_cb(self, table):
        for record in table:
            self.callback(record)
        print "XMLRPC %s DONE" % self.query.fact_table
        self.callback(None)
        print "XMLRPC %s DONE POST CB" % self.query.fact_table

    def exception_cb(self, error):
        print 'Error during XMLRPC call: ', error

    def do_start(self):
        try:
            def wrap(source):
                proxy = Proxy(self.gateway_config['url'], allowNone=True)
                query = source.query
                auth = {'AuthMethod': 'guest'}
                print "I: Issueing xmlrpc call to %s: %s" % (self.gateway_config['url'], query)
                proxy.callRemote('Get', auth, query.fact_table, 'now', query.filters, list(query.fields)).addCallbacks(source.success_cb, source.exception_cb)
            reactor.callFromThread(wrap, self) 
        except Exception, e:
            print "Exception in XMLRPC::start", e
