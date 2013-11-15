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

    def __init__(self, user = None, allowed_capabilities = None):
        # NOTE: We should avoid having code in the Interface class
        # Interface should be a parent class for Router and Gateway, so that for example we can plug an XMLRPC interface on top of it
        Interface.__init__(self, user, allowed_capabilities)

        self._sockets = list()
        # We already have gateways defined in Interface
        self._operator_graph = OperatorGraph(router = self)

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

    def forward(self, query, annotation = None, receiver = None):
        """
        Forwards an incoming Query to the appropriate Gateways managed by this Router.
        Args:
            query: The user's Query.
            annotation: Query annotation.
            receiver: An instance supporting the method set_result_value or None.
                receiver.set_result_value() will be called once the Query has terminated.
        """
        assert receiver, "Invalid receiver"

        # Try to forward the Query according to the parent class.
        # In practice, Interface::forwards() succeeds iif this is a local Query,
        # otherwise, an Exception is raised.
        ret = super(Router, self).forward(query, annotation, receiver)
        if ret: 
            # Note: we do not run hooks at the moment for local queries
            return ret

        #XXX#deferred = super(Router, self).forward(query, is_deferred, execute, user, receiver)
        #XXX#if receiver.get_result_value():
        #XXX#    return deferred

        user = annotation['user'] if annotation and 'user' in annotation else None

        Log.warning("router::forward: hardcoded execute value")
        execute = True
        Log.warning("router::forward: hardcoded is_deferred value")
        is_deferred = True

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
        return self.execute_query(query, annotation, is_deferred, receiver)

    @returns(ResultValue)
    def execute_query(self, query, annotation, is_deferred, receiver):
        """
        Execute a Query.
        Args:
            query: A Query instance.
            annotation:
        Returns:
            The ResultValue instance corresponding to this Query.
        """
        Log.warning("execute_query: manage is_deferred properly")
        if annotation:
            user = annotation.get("user", None)
        else:
            user = None

        # Code duplication with Interface() class
        if ":" in query.get_from():
            namespace, table = query.get_from().rsplit(":", 2)
            query.object = table
            allowed_platforms = [p["platform"] for p in self.get_platforms() if p["platform"] == namespace]
        else:
            allowed_platforms = [p["platform"] for p in self.get_platforms()]

        query_plan = QueryPlan()
        try:
            query_plan.build(query, self.g_3nf, allowed_platforms, self.allowed_capabilities, user)
            self.init_from_nodes(query_plan, user)
            #query_plan.dump()
            records = self.execute_query_plan(query, annotation, query_plan, is_deferred)
            return ResultValue.get_success(records)
        except Exception, e:
            Log.error("execute_query: Error while executing %s: %s %s" % (query, traceback.format_exc(), e))
            return ResultValue.get_error(ResultValue.ERROR, e)  
            

    # NEW ROUTER PACKET INTERFACE

    def receive(self, packet):
        """
        This method replaces forward() at the packet level
        """

        Log.tmp("received packet")

        # Create a Socket holding the connection information
        socket = Socket(packet, router = self)

        Log.tmp("socket ok: %r" % socket)

        query = packet.get_query()

        Log.tmp("query ok: %r" % query)

        # Handling local queries separately, could be improved
        namespace = None

        # Handling internal queries
        if ":" in query.get_from():
            namespace, table_name = query.get_from().rsplit(":", 2)

        if namespace == self.LOCAL_NAMESPACE:
            Log.tmp("local namespace")
            if table_name in ['object', 'gateway']:
                if table_name == 'object':
                    Log.tmp("object")
                    records = self.get_metadata_objects()
                    Log.tmp("records=%r" % records)
                elif table_name == "gateway":
                    records = [{'name': name} for name in Gateway.list().keys()]
                qp = QueryPlan()
                qp.ast.from_table(query, records, key = None).selection(query.get_where()).projection(query.get_select())
                Interface.success(receiver, query, result_value)
                return self.execute_query_plan(query, annotation, qp, is_deferred)
                
            else:
                query_storage = query.copy()
                query_storage.object = table_name
                records = self.storage.execute(query_storage, annotation)

                if query_storage.get_from() == "platform" and query_storage.get_action() != "get":
                    self.make_gateways()

                Interface.success(receiver, query, result_value)
                return self.send(query, records, annotation, is_deferred)

        elif namespace:
            platform_names = [platform['platform'] for platform in self.get_platforms()]
            if namespace not in platform_names:
                self.send(ErrorPacket()) # XXX
                #Interface.error(
                #    receiver,
                #    query,
                #    "Unsupported namespace '%s': valid namespaces are platform names ('%s') and 'local'." % (
                #        namespace,
                #        "', '".join(platform_names)
                #    )
                #)
                return

#            if table_name == "object":
#                # Prepare 'output' which will contains announces transposed as a list
#                # of dictionnaries.
#                output = list()
#                announces = self.announces[namespace]
#                for announce in announces:
#                    output.append(announce.get_table().to_dict())
#
#                qp = QueryPlan()
#                qp.ast.from_table(query, output, key = None).selection(query.get_where()).projection(query.get_select())
#                Interface.success(query, receiver, result_value)
#                return self.execute_query_plan(query, annotation, qp, is_deferred)

        # We need to route the query (aka connect is to the OperatorGraph)
        self._operator_graph.build_query_plan(query, packet.get_annotation(), receiver = socket)

        # Execute the operators related to the socket, if needed
        socket.receive(query)

        
        
    # Is it useful at all ?
    def send(self, packet):
        pass
