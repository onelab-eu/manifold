import os, sys, json, copy, time, traceback #, threading
from types                          import StringTypes
from twisted.internet               import defer
from manifold.core.filter           import Predicate
from manifold.core.ast              import AST
from manifold.core.key              import Key, Keys
from manifold.core.query            import Query, AnalyzedQuery
from manifold.core.table            import Table
from manifold.gateways              import Gateway
from manifold.models                import *
from manifold.core.dbnorm           import to_3nf 
from manifold.core.dbgraph          import DBGraph
from manifold.core.query_plan       import QueryPlan
from manifold.util.type             import returns, accepts
from manifold.gateways.sfa          import ADMIN_USER
from manifold.util.callback         import Callback
from manifold.core.interface        import Interface
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.storage          import DBStorage as Storage
from manifold.core.result_value     import ResultValue

from manifold.core.announce         import Announces
# XXX cannot use the wrapper with sample script
# XXX cannot use the thread with xmlrpc -n
#from manifold.util.reactor_wrapper  import ReactorWrapper as ReactorThread

CACHE_LIFETIME     = 1800

#------------------------------------------------------------------
# Class Router
# Router configured only with static/local routes, and which
# does not handle routing messages
# Router class is an Interface: 
# builds the query plan, instanciate the gateways and execute query plan using deferred if required
#------------------------------------------------------------------

class Router(Interface):
    """
    Implements a TopHat router.

    Specialized to handle Announces/Routes, ...
    """

    def boot(self):
        #print "I: Booting router"
        # Install static routes in the RIB and FIB (TODO)
        #print "D: Reading static routes in: '%s'" % self.conf.STATIC_ROUTES_FILE
        #static_routes = self.fetch_static_routes(STATIC_ROUTES_FILE)
        #self.rib[dest] = route
        # initialize dummy list of credentials to be uploaded during the
        # current session
        self.cache = {}
        
        super(Router, self).boot()
        
        self.g_3nf = to_3nf(self.metadata)

    def __enter__(self):
        ReactorThread().start_reactor()
        return self

    def __exit__(self, type=None, value=None, traceback=None):
        ReactorThread().stop_reactor()

    # This function is directly called for a Router
    # Decoupling occurs before for queries received through sockets
    def forward(self, query, is_deferred=False, execute=True, user=None):
        """
        A query is forwarded. Eventually it affects the forwarding plane, and expects an answer.
        NOTE : a query is like a flow
        """
        ret = super(Router, self).forward(query, is_deferred, execute, user)
        if ret: return ret

        # We suppose we have no namespace from here
        if not execute: 
            qp = QueryPlan()
            qp.build(query, self.g_3nf, self.allowed_capabilities, user)

            print ""
            print "QUERY PLAN:"
            print "-----------"
            qp.dump()
            print ""
            print ""

            return ResultValue.get_success(None)

        # The query plan will be the same whatever the action: it represents
        # the easier way to reach the destination = routing
        # We do not need the full query for the query plan, in fact just the
        # destination, which is a subpart of the query = (fact, filters, fields)
        # action = what to do on this QP
        # ts = how it behaves

        # Caching ?
        try:
            h = hash((user,query))
            #print "ID", h, ": looking into cache..."
        except:
            h = 0

        if query.get_action() == "get":
            if h != 0 and h in self.cache:
                res, ts = self.cache[h]
                print "Cache hit!"
                if ts > time.time():
                    return res
                else:
                    print "Expired entry!"
                    del self.cache[h]

        # Building query plan
        qp = QueryPlan()
        qp.build(query, self.g_3nf, self.allowed_capabilities, user)

        print ""
        print "QUERY PLAN:"
        print "-----------"
        qp.dump()
        print ""
        print ""

        # XXX Timestamp has been added here since it is not propagated by the query plan
        self.instanciate_gateways(qp, user, query.get_timestamp())

        if query.get_action() == "update":
            # At the moment we can only update if the primary key is present
            keys = self.metadata_get_keys(query.get_from())
            if not keys:
                raise Exception, "Missing metadata for table %s" % query.get_from()
            key = list(keys).pop()
            
            if not query.filters.has_eq(key):
                raise Exception, "The key field '%s' must be present in update request" % key

        # Execute query plan
        d = defer.Deferred() if is_deferred else None
        # the deferred object is sent to execute function of the query_plan
        return qp.execute(d)
        #return ResultValue.get_result_value(results, qp.get_result_value_array())
