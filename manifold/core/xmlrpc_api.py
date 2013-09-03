import traceback,copy
from twisted.web                import xmlrpc
from manifold.auth              import Auth
from manifold.core.query        import Query
from manifold.core.result_value import ResultValue
from manifold.util.options      import Options
from manifold.util.log          import Log
from manifold.util.misc         import make_list

#-------------------------------------------------------------------------------
# Class XMLRPCAPI
#-------------------------------------------------------------------------------

class XMLRPCAPI(xmlrpc.XMLRPC, object):

    #__metaclass__ = XMLRPCAPIMetaclass
    class __metaclass__(type):
        def __init__(cls, name, bases, dic):
            type.__init__(cls, name, bases, dic)

            # Dynamically add functions corresponding to methods from the # Auth class
            # XXX Shall we handle exceptions here ?
            for k, v in vars(Auth).items():
                if not k.startswith('_'):
                    def v_exc_handler(*args, **kwargs):
                        try:
                            v(*args, **kwargs)
                        except Exception, e:
                            ret = dict(ResultValue(
                               origin      = (ResultValue.CORE, cls.__class__.__name__),
                               type        = ResultValue.ERROR,
                               code        = ResultValue.ERROR,
                               description = str(e),
                               traceback   = traceback.format_exc()))
                            return ret
                    setattr(cls, "xmlrpc_%s" % k, v_exc_handler)

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

    def display_query(self, *args):
        # Don't show password in Server Logs
        display_args = make_list(copy.deepcopy(args))
        if 'AuthString' in display_args[0].keys():
            display_args[0]['AuthString'] = "XXXXX"
        return display_args

    # QUERIES
    # xmlrpc_forward function is called by the Query of the user using xmlrpc
    def xmlrpc_forward(self, *args):
        """
        """
        Log.info("Incoming XMLRPC request, args = %r" % self.display_query(*args))
        if not Options().disable_auth:
            assert len(args) == 2, "Wrong arguments for XMLRPC forward call"
            auth, query = args
            try:
                user = Auth(auth).check()
            except Exception, e:
                Log.warning("XMLRPCAPI::xmlrpc_forward: Authentication failed: %s" % traceback.format_exc())
                ret = dict(ResultValue(
                   origin      = (ResultValue.CORE, self.__class__.__name__),
                   type        = ResultValue.ERROR,
                   code        = ResultValue.ERROR,
                   description = str(e),
                   traceback   = traceback.format_exc()))
                return ret
                
        else:
            assert len(args) == 1, "Wrong arguments for XMLRPC forward call"
            query,  = args
            user = None

        query = Query(query)
        # self.interface is either a Router or a Forwarder
        # forward function is called with is_deferred = True in args
        deferred = self.interface.forward(query, user=user, is_deferred=True)
        def process_results(rv):
            print "XMLRPC PROCESS RESULTS"
            if 'description' in rv and isinstance(rv['description'], list):
                rv['description'] = [dict(x) for x in rv['description']]
            # Print Results
            return dict(rv)
        def handle_exceptions(failure):
            print "XMLRPC HANDLE EXCEPTIONS"
            e = failure.trap(Exception)
            ret = dict(ResultValue(
               origin      = (ResultValue.CORE, self.__class__.__name__),
               type        = ResultValue.ERROR,
               code        = ResultValue.ERROR,
               description = str(e),
               traceback   = traceback.format_exc()))
            return ret
        # deferred receives results asynchronously
        # Callbacks are triggered process_results if success and handle_exceptions if errors
        deferred.addCallbacks(process_results, handle_exceptions)
        return deferred

    def _xmlrpc_action(self, action, *args):
        # The first argument is eventually an authentication token
        if Options().disable_auth:
            query, = args
        else:
            auth, query = args

        query['action'] = action

        if Options().disable_auth:
            return self.xmlrpc_forward(query)
        else:
            return self.xmlrpc_forward(auth, query)
            
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

