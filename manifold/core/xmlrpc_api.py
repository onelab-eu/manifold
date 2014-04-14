#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# XMLRPCAPI class
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import copy, traceback
from twisted.web                        import xmlrpc
#FEDORABUGS|try:
#FEDORABUGS|    from twisted.web.xmlrpc                 import withRequest
#FEDORABUGS|except:
#FEDORABUGS|    def withRequest(f):
#FEDORABUGS|        f.withRequest = True
#FEDORABUGS|        return f

from manifold.auth                      import Auth
from manifold.core.annotation           import Annotation
from manifold.core.code                 import CORE, ERROR, FORBIDDEN
from manifold.core.deferred_receiver    import DeferredReceiver
from manifold.core.packet               import QueryPacket
from manifold.core.query                import Query
from manifold.core.result_value         import ResultValue
from manifold.util.options              import Options
from manifold.util.log                  import Log
from manifold.util.misc                 import make_list

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
                               origin      = (CORE, cls.__class__.__name__),
                               type        = ERROR,
                               code        = ERROR,
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
            assert 'interface' in kwargs, "interface argument must be specified"
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


#    @withRequest
    def xmlrpc_AuthCheck(self, request, annotation = None):
    def xmlrpc_AuthCheck(self, annotation = None):
        # We expect to find an authentication token in the annotation
        if annotation:
            auth = annotation.get('authentication', None)
        else:
            auth = {}

        auth['request'] = request

        return Auth(auth, self.interface).check()

    # QUERIES
    # xmlrpc_forward function is called by the Query of the user using xmlrpc
#    @withRequest
#    def xmlrpc_forward(self, request, query, annotation = None):
    def xmlrpc_forward(self, query, annotation = None):
        """
        """
        Log.info("xmlrpc_forward")
        Log.info("Incoming XMLRPC request, query = %r, annotation = %r" % (self.display_query(query), annotation))
        if Options().disable_auth:
            Log.info("Authentication disabled by configuration")
        else:
            if not annotation or not "authentication" in annotation:
                msg = "You need to specify an authentication token in annotation"
                return dict(ResultValue.error(msg, FORBIDDEN))

            # We expect to find an authentication token in the annotation
            if annotation:
                auth = annotation.get('authentication', None)
            else:
                auth = {}

            auth['request'] = request

            # Check login password
            try:
                # We get the router to make synchronous queries
                user = Auth(auth, self.interface.get_router()).check()
            except Exception, e:
                import traceback
                traceback.print_exc()
                Log.warning("XMLRPCAPI::xmlrpc_forward: Authentication failed...: %s" % str(e))
                msg = "Authentication failed: %s" % e
                return dict(ResultValue.error(msg, FORBIDDEN))

        # self.interface is a ManifoldDeferredRouterClient, it returns a deferred
        annotation = Annotation(annotation) if annotation else Annotation()
        annotation['user'] = user
        return self.interface.forward(Query(query), annotation)

#DEPRECATED|        query = Query(query)
#DEPRECATED|        annotation = Annotation(annotation) if annotation else Annotation()
#DEPRECATED|        annotation['user'] = user
#DEPRECATED|        receiver = DeferredReceiver()
#DEPRECATED|
#DEPRECATED|        packet = QueryPacket(query, annotation, receiver = receiver)
#DEPRECATED|        self.interface.receive(packet)
#DEPRECATED|
#DEPRECATED|        return receiver.get_deferred()

    def _xmlrpc_action(self, action, *args):
        Log.info("_xmlrpc_action")
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

