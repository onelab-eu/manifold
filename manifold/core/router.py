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
from twisted.internet.defer         import Deferred

from manifold.core.dbnorm           import to_3nf 
from manifold.core.interface        import Interface
from manifold.core.key              import Keys
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
# builds the query plan, and execute query plan using deferred if required
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
        self.cache = dict() 
        super(Router, self).boot()
        self.g_3nf = to_3nf(self.get_announces())

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

    def query_cache(self, query, user):
        """
        Try to server a Query issued by a User by querying the cache
        of this Router.
        Args:
            query: A Query instance.
            user: A User instance.
        Returns:
            The list of corresponding Records if found.
            None if not found.
        """
        # Caching ?
        try:
            h = hash((user, query))
            #print "ID", h, ": looking into cache..."
        except:
            h = 0

        if query.get_action() == "get":
            if h != 0 and h in self.cache:
                res, ts = self.cache[h]
                Log.debug("Cache hit! (query: %s)" % query)
                if ts > time.time():
                    return res
                else:
                    Log.debug("Expired entry! (query: %s)" % query)
                    del self.cache[h]
        return None

    @returns(Keys)
    def metadata_get_keys(self, table_name):
        """
        Retrieve the keys related to a given Table.
        Params:
            table_name: A String containing the name of the Table.
        Returns:
            Keys instance related to this 3nf Table.
        """
        return self.g_3nf.find_node(table_name).get_keys()

    #@returns(Deferred)
    def forward(self, query, is_deferred = False, execute = True, user = None, receiver = None):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Args:
            query: The user's Query.
            is_deferred: A boolean set to True if this Query is async
            execute: A boolean set to True if the QueryPlan must be executed.
            user: The user issuing the Query.
            receiver: An instance supporting the method set_result_value or None.
                receiver.set_result_value() will be called once the Query has terminated.
        Returns:
            A Deferred instance if the Query is async, None otherwise
        """
        # Try to forward the Query according to the parent class.
        # In practice, Interface::forwards() succeeds iif this is a local Query,
        # otherwise, an Exception is raised.
        deferred = super(Router, self).forward(query, is_deferred, execute, user, receiver)
        if receiver.get_result_value():
            return deferred

        # Code duplication with Interface() class
        if ':' in query.get_from():
            namespace, table_name = query.get_from().rsplit(':', 2)
            query.object = table_name
            allowed_platforms = [p.platform for p in self.get_platforms() if p.platform == namespace]
        else:
            allowed_platforms = [p.platform for p in self.get_platforms()]
        # We suppose we have no namespace from here

        # Search whether the result corresponding to this Query is already stored
        # in the Router's cache
        if execute:
            res = self.query_cache(query, user)
            if res != None: return res

        # Building QueryPlan 
        query_plan = QueryPlan()
        try:
            query_plan.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)
        except Exception, e:
            Router.error(receiver, query, e)
            return None
        query_plan.dump()

        # If this Query must not be executed, we can leave right now 
        if not execute: 
            Router.success(receiver, query)
            return None

        self.init_from_nodes(query_plan, user)

        if query.get_action() == "update":
            # At the moment we can only update if the primary key is present
            keys = self.metadata_get_keys(query.get_from())
            if not keys:
                Router.error(receiver, query, "Missing metadata for table %s" % query.get_from())
            key_fields = keys.one().get_minimal_names()
            
            # XXX THIS SHOULD BE ABLE TO ACCEPT TUPLES
            #if not query.filters.has_eq(key):
            #    self.error(receiver, query, "The key field(s) '%r' must be present in update request" % key)

        # Execute query plan
        # The deferred object is sent to execute function of the query_plan
        deferred = Deferred() if is_deferred else None
        return query_plan.execute(deferred, receiver)
