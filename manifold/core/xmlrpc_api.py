from manifold.auth              import Auth
from manifold.core.query        import Query
from manifold.util.options      import Options
from twisted.web                import xmlrpc
from manifold.core.forwarder    import Forwarder
from manifold.core.router       import Router

#-------------------------------------------------------------------------------
# Class XMLRPCAPI
#-------------------------------------------------------------------------------

class XMLRPCAPI(xmlrpc.XMLRPC, object):

    #__metaclass__ = XMLRPCAPIMetaclass
    class __metaclass__(type):
        def __init__(cls, name, bases, dic):
            type.__init__(cls, name, bases, dic)

            # Dynamically add functions corresponding to methods from the # Auth class
            for k, v in vars(Auth).items():
                if not k.startswith('_'):
                    setattr(cls, "xmlrpc_%s" % k, v)

            # We should do the same with the router/forwarder class == interface

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            assert 'platforms' not in kwargs, "Cannot specify platforms argument twice"
            self.platforms = args[0]
        elif len(args) == 0:
            assert 'platforms' in kwargs, "platforms argument mush be specified"
            self.platforms = kwargs['platforms']
        else:
            raise Exception, "Wrong arguments"
        super(XMLRPCAPI, self).__init__(**kwargs)

    def authenticate(self, auth):
        user = Auth(auth).check()
        return user

    # QUERIES
    def xmlrpc_forward(self, *args):
        """
        """
        # The first argument is eventually an authentication token
        if not Options().disable_auth:
            auth = args[0]
            user = self.authenticate(auth)
            args = args[1:]
        else:
            user = None
        # The rest defines the query
        query = Query(*args)

        cls = Forwarder if len(self.platforms) == 1 else Router
        interface = cls(self.platforms)
        interface.forward(query, user=user)

        # FORMER CODE FOR ROUTER
        # cb = Callback()
        # ast.callback = cb
        #
        # gw_or_router.set_callback(cb) # XXX should be removed from Gateway
        # gw_or_router.forward(query, deferred=False, user=user)
        # cb.wait()

        return cb.results

