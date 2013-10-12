#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Manifold router handles Query, compute the corresponding QueryPlan,
# and deduce which Queries must be sent the appropriate Gateways.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import os, sys, json, copy, time, traceback #, threading
from twisted.internet               import defer

from manifold.core.dbnorm           import to_3nf 
from manifold.core.interface        import Interface
from manifold.core.query_plan       import QueryPlan
from manifold.core.result_value     import ResultValue
from manifold.util.log              import Log
from manifold.util.type             import returns, accepts
from manifold.util.reactor_thread   import ReactorThread
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
    Implements a Manifold Router.
    Specialized to handle Announces/Routes, ...
    """

    def boot(self):
        """
        Boot the Interface (prepare metadata, etc.).
        """
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
        """
        Function called back while entering a "with" statement.
        See http://effbot.org/zone/python-with-statement.htm
        """
        ReactorThread().start_reactor()
        return self

    def __exit__(self, type=None, value=None, traceback=None):
        """
        Function called back while leaving a "with" statement.
        See http://effbot.org/zone/python-with-statement.htm
        """
        ReactorThread().stop_reactor()

    # This function is directly called for a Router
    # Decoupling occurs before for queries received through sockets
#    @returns(ResultValue)
    def forward(self, query, is_deferred=False, execute=True, user=None):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Args:
            query: The user's Query.
            is_deferred: (bool)
            execute: Set to true if the QueryPlan must be executed.
            user: The user issuing the Query.
        Returns:
            A ResultValue in case of success.
            None in case of failure.
        """
        Log.info("Router::forward: %s" % query)
        ret = super(Router, self).forward(query, is_deferred, execute, user)
        if ret: return ret

        # Code duplication with Interface() class
        if ':' in query.get_from():
            namespace, table = query.get_from().rsplit(':', 2)
            query.object = table
            allowed_platforms = [p.platform for p in self.platforms if p.platform == namespace]
        else:
            allowed_platforms = [p.platform for p in self.platforms]

        # We suppose we have no namespace from here
        if not execute: 
            qp = QueryPlan()
            qp.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)

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
            h = hash((user, query))
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
        qp.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)

        print ""
        print "QUERY PLAN:"
        print "-----------"
        qp.dump()
        print ""
        print ""

        self.instanciate_gateways(qp, user)

        if query.get_action() == "update":
            # At the moment we can only update if the primary key is present
            keys = self.metadata_get_keys(query.get_from())
            if not keys:
                raise Exception, "Missing metadata for table %s" % query.get_from()
            key_fields = keys.one().get_minimal_names()
            
            # XXX THIS SHOULD BE ABLE TO ACCEPT TUPLES
            #if not query.filters.has_eq(key):
            #    raise Exception, "The key field(s) '%r' must be present in update request" % key

        # Execute query plan
        d = defer.Deferred() if is_deferred else None
        # the deferred object is sent to execute function of the query_plan
        return qp.execute(d)
        #return ResultValue.get_result_value(results, qp.get_result_value_array())
