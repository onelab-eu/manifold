import traceback

from manifold.auth              import Auth
from manifold.core.query        import Query
from manifold.util.options      import Options
from twisted.web                import xmlrpc
from manifold.core.result_value import ResultValue

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
            assert 'interface' not in kwargs, "Cannot specify interface argument twice"
            self.interface = args[0]
        elif len(args) == 0:
            assert 'interface' in kwargs, "interface argument mush be specified"
            self.interface = kwargs['interface']
        else:
            raise Exception, "Wrong arguments"
        super(XMLRPCAPI, self).__init__(**kwargs)

    # QUERIES
    def xmlrpc_forward(self, *args):
        """
        """
        print "-------------------"
        print "xmlrpc_api args = ",args
        print "-------------------"
        if not Options().disable_auth:
            assert len(args) == 2, "Wrong arguments for XMLRPC forward call"
            auth, query = args
            user = Auth(auth).check()
        else:
            assert len(args) == 1, "Wrong arguments for XMLRPC forward call"
            query = args[0]
            user = None

        query = Query(query)
        try:
            rv = self.interface.forward(query, user=user)
            # replace ResultValue by dict
            if 'description' in rv and isinstance(rv['description'], list):
                rv['description'] = [dict(x) for x in rv['description']]
            print "-------------------"
            print "xmlrpc_api result = ",dict(rv)
            print "-------------------"
            return dict(rv)

        except Exception, e:
            ret = dict(ResultValue(
                origin      = (ResultValue.CORE, self.__class__.__name__),
                type        = ResultValue.ERROR, 
                code        = ResultValue.ERROR,
                description = str(e),
                traceback   = traceback.format_exc()))
            return ret

    def _xmlrpc_action(self, action, *args):
        # The first argument is eventually an authentication token
        pos = 0 if Options().disable_auth else 1
        print "args=", args
        print "pos=", pos
        print "action", action
        print "========"
        args[pos]['action'] = action
        return self.xmlrpc_forward(*args)
            
    def xmlrpc_Get   (self, *args): return self._xmlrpc_action('get',    *args)
    def xmlrpc_Update(self, *args): return self._xmlrpc_action('update', *args)
    def xmlrpc_Create(self, *args): return self._xmlrpc_action('create', *args)
    def xmlrpc_Delete(self, *args): return self._xmlrpc_action('delete', *args)

        # FORMER CODE FOR ROUTER
        # cb = Callback()
        # ast.callback = cb
        #
        # gw_or_router.set_callback(cb) # XXX should be removed from Gateway
        # gw_or_router.forward(query, deferred=False, user=user)
        # cb.wait()

