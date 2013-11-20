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

from manifold.core.capabilities     import Capabilities
from manifold.core.dbnorm           import to_3nf 
from manifold.core.dbgraph          import DBGraph
from manifold.core.interface        import Interface
from manifold.core.key              import Keys
from manifold.core.method           import Method
from manifold.core.operator_graph   import OperatorGraph
from manifold.core.query_plan       import QueryPlan
from manifold.core.result_value     import ResultValue
from manifold.core.socket           import Socket
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

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, user = None, allowed_capabilities = None):
        # NOTE: We should avoid having code in the Interface class
        # Interface should be a parent class for Router and Gateway, so that for example we can plug an XMLRPC interface on top of it
        Interface.__init__(self, user, allowed_capabilities)

        self._sockets = list()
        # We already have gateways defined in Interface
        self._operator_graph = OperatorGraph(router = self)

        # XXX metadata/g_3nf

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_metadata(self):
        return self.g_3nf

    def get_local_metadata(self):
        # We do not need normalization here, can directly query the gateway
        map_method_capabilities = {
            Method('local', 'platform'): Capabilities('retrieve', 'join', 'selection', 'projection'),
            Method('local', 'object'): Capabilities('retrieve', 'join', 'selection', 'projection'),
            Method('local', 'column'): Capabilities('retrieve', 'join', 'selection', 'projection')
        }
        local_metadata = self._storage.get_metadata()
        local_tables = [a.table for a in local_metadata]
        return DBGraph(local_tables, map_method_capabilities)

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

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

#DEPRECATED|    def forward(self, query, annotation = None, receiver = None):
#DEPRECATED|        """
#DEPRECATED|        Forwards an incoming Query to the appropriate Gateways managed by this Router.
#DEPRECATED|        Args:
#DEPRECATED|            query: The user's Query.
#DEPRECATED|            annotation: Query annotation.
#DEPRECATED|            receiver: An instance supporting the method set_result_value or None.
#DEPRECATED|                receiver.set_result_value() will be called once the Query has terminated.
#DEPRECATED|        """
#DEPRECATED|        assert receiver, "Invalid receiver"
#DEPRECATED|
#DEPRECATED|        # Try to forward the Query according to the parent class.
#DEPRECATED|        # In practice, Interface::forwards() succeeds iif this is a local Query,
#DEPRECATED|        # otherwise, an Exception is raised.
#DEPRECATED|        ret = super(Router, self).forward(query, annotation, receiver)
#DEPRECATED|        if ret: 
#DEPRECATED|            # Note: we do not run hooks at the moment for local queries
#DEPRECATED|            return ret
#DEPRECATED|
#DEPRECATED|        #XXX#deferred = super(Router, self).forward(query, is_deferred, execute, user, receiver)
#DEPRECATED|        #XXX#if receiver.get_result_value():
#DEPRECATED|        #XXX#    return deferred
#DEPRECATED|
#DEPRECATED|        user = annotation['user'] if annotation and 'user' in annotation else None
#DEPRECATED|
#DEPRECATED|        Log.warning("router::forward: hardcoded execute value")
#DEPRECATED|        execute = True
#DEPRECATED|        Log.warning("router::forward: hardcoded is_deferred value")
#DEPRECATED|        is_deferred = True
#DEPRECATED|
#DEPRECATED|        # We suppose we have no namespace from here
#DEPRECATED|        if not execute: 
#DEPRECATED|            query_plan = QueryPlan(interface = self)
#DEPRECATED|            # Duplicated code
#DEPRECATED|            if ':' in query.get_from():
#DEPRECATED|                namespace, table = query.get_from().rsplit(':', 2)
#DEPRECATED|                query.object = table
#DEPRECATED|                allowed_platforms = [p['platform'] for p in self.platforms if p['platform'] == namespace]
#DEPRECATED|            else:
#DEPRECATED|                allowed_platforms = [p['platform'] for p in self.platforms]
#DEPRECATED|            try:
#DEPRECATED|                query_plan.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)
#DEPRECATED|            except Exception, e:
#DEPRECATED|                Router.error(receiver, query, e)
#DEPRECATED|                return None
#DEPRECATED|            #query_plan.dump()
#DEPRECATED|
#DEPRECATED|            Router.success(receiver, query)
#DEPRECATED|            return None
#DEPRECATED|
#DEPRECATED|        if query.get_action() == "update":
#DEPRECATED|            # At the moment we can only update if the primary key is present
#DEPRECATED|            keys = self.metadata_get_keys(query.get_from())
#DEPRECATED|            if not keys:
#DEPRECATED|                Router.error(receiver, query, "Missing metadata for table %s" % query.get_from())
#DEPRECATED|            key_fields = keys.one().get_minimal_names()
#DEPRECATED|            
#DEPRECATED|            # XXX THIS SHOULD BE ABLE TO ACCEPT TUPLES
#DEPRECATED|            #if not query.filters.has_eq(key):
#DEPRECATED|            #    self.error(receiver, query, "The key field(s) '%r' must be present in update request" % key)
#DEPRECATED|
#DEPRECATED|        # Execute query plan
#DEPRECATED|        # the deferred object is sent to execute function of the query_plan
#DEPRECATED|        # This might be a deferred, we cannot put any hook here...
#DEPRECATED|        return self.execute_query(query, annotation, is_deferred, receiver)

#DEPRECATED|    @returns(ResultValue)
#DEPRECATED|    def execute_query(self, query, annotation, is_deferred, receiver):
#DEPRECATED|        """
#DEPRECATED|        Execute a Query on this Router.
#DEPRECATED|        Args:
#DEPRECATED|            query: A Query instance.
#DEPRECATED|            annotation:
#DEPRECATED|        Returns:
#DEPRECATED|            The ResultValue instance corresponding to this Query.
#DEPRECATED|        """
#DEPRECATED|        Log.warning("execute_query: manage is_deferred properly")
#DEPRECATED|        if annotation:
#DEPRECATED|            user = annotation.get("user", None)
#DEPRECATED|        else:
#DEPRECATED|            user = None
#DEPRECATED|
#DEPRECATED|        # Code duplication with Interface() class
#DEPRECATED|        if ":" in query.get_from():
#DEPRECATED|            namespace, table = query.get_from().rsplit(":", 2)
#DEPRECATED|            query.object = table
#DEPRECATED|            allowed_platforms = [p["platform"] for p in self.get_platforms() if p["platform"] == namespace]
#DEPRECATED|        else:
#DEPRECATED|            allowed_platforms = [p["platform"] for p in self.get_platforms()]
#DEPRECATED|
#DEPRECATED|        query_plan = QueryPlan(interface = self)
#DEPRECATED|        try:
#DEPRECATED|            query_plan.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)
#DEPRECATED|            self.init_from_nodes(query_plan, user)
#DEPRECATED|            #query_plan.dump()
#DEPRECATED|            records = self.execute_query_plan(query, annotation, query_plan, is_deferred)
#DEPRECATED|<<<<<<< HEAD
#DEPRECATED|            return ResultValue.get_success(records)
#DEPRECATED|=======
#DEPRECATED|            receiver.set_result_value(ResultValue.get_success(records))
#DEPRECATED|            Log.tmp("receiver = %s records = %s" % (receiver, receiver.get_result_value()))
#DEPRECATED|>>>>>>> routerv2
#DEPRECATED|        except Exception, e:
#DEPRECATED|            Log.error("execute_query: Error while executing %s: %s %s" % (query, traceback.format_exc(), e))
#DEPRECATED|            receiver.set_result_value(ResultValue.get_error(ResultValue.ERROR, e))
            

    # NEW ROUTER PACKET INTERFACE

    def receive(self, packet):
        """
        This method replaces forward() at the packet level
        """

        Log.tmp("received packet")

        # Create a Socket holding the connection information
        socket = Socket(packet, router = self)

        packet.set_receiver(socket)

        Log.tmp("socket ok: %r" % socket)
        query = packet.get_query()
        Log.tmp("query ok: %r" % query)
        annotation = packet.get_annotation()
        Log.tmp("annotation: %r" % annotation)

        # We need to route the query (aka connect is to the OperatorGraph)
        self._operator_graph.build_query_plan(packet)

        # Execute the operators related to the socket, if needed
        socket.receive(packet)

        
        
#    # Is it useful at all ?
#    def send(self, packet):
#        pass
