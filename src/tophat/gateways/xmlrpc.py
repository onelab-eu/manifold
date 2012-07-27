class XMLRPC():
    def __init__(self, api, query, **kwargs):
        self.api = api
        self.config = kwargs
        self.remote = None
        self.query = query
        # Allowed types: xmlrpc, zmq, etc.
        if not 'type' in self.config:
            self.config['type'] = 'zmq'

    def __str__(self):
        if self.config['type'] == 'xmlrpc':
            return "<XMLRPCGateway[%s] %s %s>" % (self.config['type'], self.config['url'], self.query)
        else:
            return "<XMLRPCGateway[%s] %s >" % (self.config['type'], self.query)

    def connect(self):
        if self.config['type'] == 'xmlrpc':
            import xmlrpclib
            self.remote = xmlrpclib.ServerProxy(self.config['url'], allow_none=True)
        else:
            raise Exception, "Not implemented"

    def connected(self):
        return self.remote

    def get(self, query):
        return list(self._get(query))

    def _get(self):
        # iterator / supports callbacks (zmq, xmlrpc requires establishing another server)
        # can avoid local buffering of the whole table, if the remote end supports it
        # the remote end can be a gateway, for example,

        if not self.config['type'] == 'xmlrpc':
            raise Exception, "Not implemented"

        if not self.connected():
            self.connect()
        
        # Let's call the simplest query as possible to begin with
        print "I: Issueing xmlrpc call to %s: %s" % (self.config['url'], self.query)
        auth = {'AuthMethod': 'guest'}
        try:
            result = self.remote.Get(auth, self.query.get_method(), self.query.get_ts(), {}, self.query.get_fields()) # no filter/no field
        except Exception, e:
            print "Exception in Network: %s" % e
            return 
        for r in result:
            yield r
        return
