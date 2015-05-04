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
#   Loïc Baron        <loic.baron@lip6.fr>

import os, sys, json, copy, time, traceback #, threading
from twisted.internet                   import defer

from manifold.core.cache                import Cache
from manifold.core.dbnorm               import to_3nf 
from manifold.core.interface            import Interface
from manifold.core.query_plan           import QueryPlan
from manifold.core.record               import LastRecord
from manifold.core.result_value         import ResultValue
from manifold.util.log                  import Log
from manifold.util.type                 import returns, accepts
from manifold.util.reactor_thread       import ReactorThread
from manifold.policy                    import Policy
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

        # TODO: ROUTERV2
        # Cache per user
        self._cache = Cache()
        self._cache_user = dict()

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

    # TODO: ROUTERV2 
    # Cache per user
    # this function creates a cache per user if user_id is in annotations
    # else it provides a global cache for non logged in Queries
    def get_cache(self, annotations=None):
        user = annotations.get('user')
        user_id = user.get('user_id') if user else None

        if not user_id:
            # Use global cache
            Log.warning("Use of global cache for query, annotations=%r" % (annotations,))
            return self._cache    

        # Use per-user cache
        if user_id not in self._cache_user:
            self._cache_user[user_id] = Cache()
        return self._cache_user[user_id]

    # TODO: ROUTERV2 
    # Invalidate Cache per user
    # XXX this deletes the totally the cache for a user, not only the object in the Latice
    # this function invalidates the cache of a user if user_id is in annotations
    # else it provides invalidates the global cache for non logged in Queries
    def delete_cache(self, annotations=None):
         try:
            Log.tmp("----------> DELETE CACHE PER USER <------------")
            Log.tmp(annotations)
            if annotations is not None:
                user_id = annotations['user']['user_id']
                if user_id in self._cache_user:
                    del self._cache_user[user_id]
         except:
            import traceback
            traceback.print_exc()

    # This function is directly called for a Router
    # Decoupling occurs before for queries received through sockets
#    @returns(ResultValue)
    def forward(self, query, annotations = None, is_deferred=False, execute=True):
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

        # TMP CACHE DEBUG
        #import pdb
        #pdb.set_trace()
        
        # INVALIDATE CACHE - should be per object?
        if query.get_action() != 'get':
            self.delete_cache(annotations)

        user = annotations['user'] if annotations and 'user' in annotations else None
        if annotations is None:
            annotations = {}

        ret = super(Router, self).forward(query, annotations, is_deferred, execute)
        if ret: 
            # Note: we do not run hooks at the moment for local queries
            return ret


        # Previously, cache etc had nothing to do. We now enforce policy, and
        # eventually this will give us a new query plan
        query_plan = None
        

        # Enforcing policy
        # XXX This issues after a check (in interface) that we are not
        # considering local or object tables... and we need to do the proper
        # check when records are received, otherwise the query is absent from
        # the cache. This is sources of errors, and should be fixed.
        #
        # Possible results and related actions:
        # - ACCEPT : the query passes and will trigger a new query plan
        # - CACHED : ill named, the cache is taking care of everything, no new query plan to do
        #    This will handle multiple scenarios such as buffered (a mixed of
        #    cached and real time records), and multicast (real time records).
        # - DENIED
        # - ERROR

        (decision, data) = self.policy.filter(query, None, annotations)

        # TMP CACHE DEBUG
        #pdb.set_trace()

        if decision == Policy.ACCEPT:
            pass
        elif decision == Policy.REWRITE:
            _query, _annotations = data
            if _query:
                query = _query
            if _annotations:
                annotations = _annotations
    
        elif decision == Policy.CACHE_HIT:
            query_plan = data
            #return self.send(query, data, annotations, is_deferred)
    
        elif decision in [Policy.DENIED, Policy.ERROR]:
            if decision == Policy.DENIED:
                data = ResultValue.get_error(ResultValue.FORBIDDEN)
            return self.send_result_value(query, data, annotations, is_deferred)
    
        else:
            raise Exception, "Unknown QUERY decision from policy engine: %s" % Policy.map_decision[decision]
        
        # TMP CACHE DEBUG
        #pdb.set_trace()

        # We suppose we have no namespace from here
        if not execute: 
            if not query_plan:
                query_plan = QueryPlan()
                query_plan.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)

            Log.info(query_plan.dump())

            # Note: no hook either for queries that are not executed
            return ResultValue.get_success(None)

        # The query plan will be the same whatever the action: it represents
        # the easier way to reach the destination = routing
        # We do not need the full query for the query plan, in fact just the
        # destination, which is a subpart of the query = (fact, filters, fields)
        # action = what to do on this QP
        # ts = how it behaves

        # XXX disabled
        #if query.get_action() == "update":
        #    # At the moment we can only update if the primary key is present
        #    keys = self.metadata_get_keys(query.get_from())
        #    if not keys:
        #        raise Exception, "Missing metadata for table %s" % query.get_from()
        #    key_fields = keys.one().get_minimal_names()
        #    
        #    # XXX THIS SHOULD BE ABLE TO ACCEPT TUPLES
        #    #if not query.filters.has_eq(key):
        #    #    raise Exception, "The key field(s) '%r' must be present in update request" % key

        # Execute query plan
        # the deferred object is sent to execute function of the query_plan
        # This might be a deferred, we cannot put any hook here...

        try:
            # Namespace and table are splited if prefix is used in the query
            # namespace is passed as a parameter 
            # Then namespace and table are back together in process_qp_results
            # in order to send the results in the right Cache entry 
            if ":" in query.get_from():
                namespace, table_name = query.get_from().rsplit(":", 2)
                query.object = table_name
            else:
                namespace = None

            if query_plan:
                return self.execute_query_plan(namespace, query, annotations, query_plan, is_deferred, policy = False)
            else:
                return self.execute_query(namespace, query, annotations, is_deferred)
        except Exception, e:
            return ResultValue.get_error(e, traceback.format_exc())

    def process_qp_results(self, namespace, query, records, annotations, query_plan, policy = True):
        # namespace and table are back together in process_qp_results
        # in order to send the results in the right Cache entry 
        if namespace is not None:
            query.object = namespace + ':' + query.object

        # Handling internal queries
        is_local = (namespace == self.LOCAL_NAMESPACE)
        is_metadata = (namespace and query.object == "object")
        if policy and not is_local and not is_metadata:
            # XXX What to do in case of errors, and records is []
            for record in records:
                # We process records one by one...
                (decision, data) = self.policy.filter(query, record, annotations, is_query = False)
                if decision == Policy.ACCEPT:
                    pass
                elif decision == Policy.REWRITE:
                    print "WEIRD CASE: maybe it should be changed since here we are not replacing a query but a record"
                    _query, _annotations = data
                    if _query:
                        query = _query
                    if _annotations:
                        annotations = _annotations

                elif decision in [Policy.DENIED, Policy.ERROR]:
                    print "WEIRD CASE also for records"
                    if decision == Policy.DENIED:
                        data = ResultValue.get_error(ResultValue.FORBIDDEN)
                    return self.send_result_value(query, data, annotations, is_deferred)

                else:
                    raise Exception, "Unknown RECORD decision from policy engine: %s" % Policy.map_decision[decision]

        description = query_plan.get_result_value_array()

        return ResultValue.get_result_value(records, description)

    def execute_query_plan(self, namespace, query, annotations, query_plan, is_deferred = False, policy = True):
        records = query_plan.execute(is_deferred)
        if is_deferred:
            # results is a deferred
            records.addCallback(lambda records: self.process_qp_results(namespace, query, records, annotations, query_plan, policy))
            return records # will be a result_value after the callback
        else:
            return self.process_qp_results(namespace, query, records, annotations, query_plan)

    def execute_query(self, namespace, query, annotations, is_deferred=False):
        if annotations:
            user = annotations.get('user', None)
        else:
            user = None

        # Code duplication with Interface() class
        if namespace is not None:
            allowed_platforms = [p['platform'] for p in self.platforms if p['platform'] == namespace]
        else:
            allowed_platforms = [p['platform'] for p in self.platforms]

        qp = QueryPlan()
        qp.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)
        Log.tmp("QUERY PLAN")
        qp.dump()

        self.instanciate_gateways(qp, user)
        Log.info("QUERY PLAN:\n%s" % (qp.dump()))

        return self.execute_query_plan(namespace, query, annotations, qp, is_deferred)
