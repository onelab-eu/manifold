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
from manifold.policy                import Policy
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


    # This function is directly called for a Router
    # Decoupling occurs before for queries received through sockets
    #@returns(ResultValue)
    #@returns(Deferred)
    def forward(self, query, annotations = None, is_deferred = False, receiver = None, execute = True):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Args:
            query: The user's Query.
            annotations: Query annotations
            is_deferred: A boolean set to True if this Query is async
            receiver: An instance supporting the method set_result_value or None.
                receiver.set_result_value() will be called once the Query has terminated.
            execute: A boolean set to True if the QueryPlan must be executed.
        """
        assert receiver, "Invalid receiver"

        # Try to forward the Query according to the parent class.
        # In practice, Interface::forwards() succeeds iif this is a local Query,
        # otherwise, an Exception is raised.
        ret = super(Router, self).forward(query, annotations, is_deferred, receiver, execute)
        if ret: 
            # Note: we do not run hooks at the moment for local queries
            return ret

        #XXX#deferred = super(Router, self).forward(query, is_deferred, execute, user, receiver)
        #XXX#if receiver.get_result_value():
        #XXX#    return deferred

        user = annotations['user'] if annotations and 'user' in annotations else None

        # We suppose we have no namespace from here
        if not execute: 
            query_plan = QueryPlan()
            # Duplicated code
            if ':' in query.get_from():
                namespace, table = query.get_from().rsplit(':', 2)
                query.object = table
                allowed_platforms = [p['platform'] for p in self.platforms if p['platform'] == namespace]
            else:
                allowed_platforms = [p['platform'] for p in self.platforms]
            try:
                query_plan.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)
            except Exception, e:
                Router.error(receiver, query, e)
                return None
            #query_plan.dump()

            Router.success(receiver, query)
            return None

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
        # the deferred object is sent to execute function of the query_plan
        # This might be a deferred, we cannot put any hook here...
        return self.execute_query(query, annotations, is_deferred, receiver)

    def process_qp_results(self, query, records, annotations, query_plan):

        # Enforcing policy
        (decision, data) = self.policy.filter(query, records, annotations)
        if decision != Policy.ACCEPT:
            raise Exception, "Unknown decision from policy engine"

        description = query_plan.get_result_value_array()
        return ResultValue.get_result_value(records, description)

    def execute_query_plan(self, query, annotations, query_plan, is_deferred = False):
        records = query_plan.execute(is_deferred)
        if is_deferred:
            # results is a deferred
            records.addCallback(lambda records: self.process_qp_results(query, records, annotations, query_plan))
            return records # will be a result_value after the callback
        else:
            return self.process_qp_results(query, records, annotations, query_plan)

    def execute_query(self, query, annotations, is_deferred=False):
        if annotations:
            user = annotations.get('user', None)
        else:
            user = None

        # Code duplication with Interface() class
        if ':' in query.get_from():
            namespace, table = query.get_from().rsplit(':', 2)
            query.object = table
            allowed_platforms = [p['platform'] for p in self.platforms if p['platform'] == namespace]
        else:
            allowed_platforms = [p['platform'] for p in self.platforms]

        query_plan = QueryPlan()
        query_plan.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)

        self.init_from_nodes(query_plan, user)
        #XXX#self.instanciate_gateways(query_plan, user) # removed by marco ????

        #query_plan.dump()

        return self.execute_query_plan(query, annotations, query_plan, is_deferred)
