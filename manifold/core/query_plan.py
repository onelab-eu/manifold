#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convert a 3nf-tree into an AST (e.g. a query plan)
# \sa manifold.core.pruned_tree.py
# \sa manifold.core.ast.py
# 
# QueryPlan class builds, process and executes Queries
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loïc Baron        <loic.baron@lip6.fr>

import copy, random
from types                         import StringTypes, GeneratorType
from twisted.internet.defer        import Deferred, DeferredList

from manifold.core.stack           import Stack
from manifold.core.explore_task    import ExploreTask

from manifold.core.ast             import AST
from manifold.core.filter          import Filter
from manifold.core.result_value    import ResultValue
from manifold.core.table           import Table 
from manifold.operators.From       import From
from manifold.util.callback        import Callback
from manifold.util.log             import Log
from manifold.util.type            import returns, accepts

class QueryPlan(object):
    """
    Building a query plan consists in setting the AST and the list of Froms.
    """

    def __init__(self):
        """
        Constructor.
        """
        # TODO metadata, user should be a property of the query plan
        self.ast   = AST()
        self.froms = list() 

    def add_from(self, from_node):
        """
        Register a From Node in this QueryPlan.
        FromTable Nodes are not registered and are ignored.
        Args:
            from_node: A From instance. 
        """
        if isinstance(from_node, From):
            self.froms.append(from_node)

    @returns(GeneratorType)
    def get_froms(self):
        """
        Returns:
            A Generator allowing to iterate on From nodes involved
            in this QueryPlan.
        """
        for from_node in self.froms:
            yield from_node

    @returns(list)
    def get_result_value_array(self):
        """
        Returns:
            A list of ResultValue instance corresponding to each From Node
            involved in this QueryPlan.
        """
        # Iterate over gateways to get their ResultValue 
        result_values = list() 
        for from_node in self.get_froms():
            assert from_node.gateway, "Invalid FROM node: %s" % from_node
            result_value = from_node.get_result_value()
            if not result_value:
                Log.debug("%s didn't returned a ResultValue, may be it is related to a pruned child" % from_node)
                continue
            if result_value["code"] != ResultValue.SUCCESS:
                result_values.append(result_value)
        return result_values

    def inject_at(self, query):
        """
        Update From Nodes of the QueryPlan in order to take into account AT
        clause involved in a user Query.
        Args:
            query: The Query issued by the user.
        """
        # OPTIMIZATION: We should not built From Node involving a Table
        # unable to serve the Query due to its timestamp
        # (see query.get_timestamp())
        # Or their corresponding Gateway should return an empty result
        # by only sending LAST_RECORD.
        for from_node in self.get_froms():
            from_node.query.timestamp = query.get_timestamp()

    def set_ast(self, ast, query):
        """
        Complete an AST in order to take into account SELECT and WHERE clauses
        involved in a user Query.
        Args:
            ast: An AST instance made of Union, LeftJoin, SubQuery and From Nodes.
            query: The Query issued by the user.
        """
        #print "QUERY PLAN (before optimization):"
        #ast.dump()
        ast.optimize(query)
        self.inject_at(query)
        self.ast = ast
    
        # Update the main query to add applicative information such as action and params
        # NOTE: I suppose params cannot have '.' inside
        for from_node in self.get_froms():
            q = from_node.get_query()
            if q.get_from() == query.get_from():
                q.action = query.get_action()
                q.params = query.get_params()

    def build(self, query, metadata, allowed_platforms, allowed_capabilities, user = None):
        """
        Build the QueryPlan involving several Gateways according to a 3nf
        graph and a user Query. If only one Gateway is involved, you should
        use QueryPlan::build_simple.
        Raises:
            ValueError if the query is not coherent (invalid table name...).
            Exception if the QueryPlan cannot be built.
        Args:
            query: The Query issued by the user.
            metadata: The 3nf graph (DBGraph instance).
            allowed_platforms: A list of platform names (list of String).
                Which platforms the router is allowed to query.
                Could be used to restrict the Query to a limited set of platforms
                either because it is specified by the user Query or either due
                to the Router configuration.
            allowed_capabilities: A Capabilities instance or None.
                Specify which capabilities the Router can perform if it is
                involved as an intermediate Router between two other Routers.
            TODO: metadata, allowed_platforms and allowed_platforms capabilities should be
                deduced from the query, the router, and the user.
                    router.get_metadata()
                    router.get_allowed_platforms(query, user)
                    router.get_allowed_capabilities(query, user)
            user: A User instance or None.
        """
        root = metadata.find_node(query.get_from())
        if not root:
            raise ValueError("Cannot find %s in metadata, known tables are {%s}" % (
                query.get_from(),
                ', '.join(metadata.get_table_names()))
            )
        
        root_task = ExploreTask(root, relation=None, path=[], parent=self, depth=1)
        if not root_task:
            raise Exception("Unable to build a suitable QueryPlan")
        root_task.addCallback(self.set_ast, query)

        stack = Stack(root_task)
        seen = {} # path -> set()

        missing_fields  = set()
        missing_fields |= query.get_select()
        missing_fields |= query.get_where().get_field_names()

        while missing_fields:
            # Explore the next prior ExploreTask
            task = stack.pop()

            # The Stack is empty, so we have explored the DBGraph
            # without finding the every queried fields.
            if not task:
                Log.warning("Exploration terminated without finding fields: %r" % missing_fields)
                break

            pathstr = '.'.join(task.path)
            if not pathstr in seen:
                seen[pathstr] = set()
            task.explore(stack, missing_fields, metadata, allowed_platforms, allowed_capabilities, user, seen[pathstr], query_plan = self)

        # Cancel every remaining ExploreTasks, we cannot found anymore
        # queried fields.
        while not stack.is_empty():
            task = stack.pop()
            task.cancel()
        print "BUILD DONE"
    
        # Do we need to wait for self.ast here ?

    # XXX Note for later: what about holes in the subquery chain. Is there a notion
    # of inject ? How do we collect subquery results two or more levels up to match
    # the structure (with shortcuts) as requested by the user.

#DEPRECATED|    def build_simple(self, query, metadata, allowed_capabilities):
#DEPRECATED|        """
#DEPRECATED|        Builds a QueryPlan (self) related to a single Gateway.
#DEPRECATED|        This is used only by a Forwarder. This function will probably soon
#DEPRECATED|        become DEPRECATED.
#DEPRECATED|        If several Gateways are involved, you must use QueryPlan::build.
#DEPRECATED|        Args:
#DEPRECATED|            query: The Query issued by the user.
#DEPRECATED|            metadata:
#DEPRECATED|            allowed_capabilities: The Capabilities related to this Gateway.
#DEPRECATED|        """
#DEPRECATED|        # XXX allowed_capabilities should be a property of the query plan !
#DEPRECATED|
#DEPRECATED|        # XXX Check whether we can answer query.object
#DEPRECATED|
#DEPRECATED|        # Here we assume we have a single platform
#DEPRECATED|        platform = metadata.keys()[0]
#DEPRECATED|        announce = metadata[platform][query.get_from()] # eg. table test
#DEPRECATED|        
#DEPRECATED|        # Set up an AST for missing capabilities (need configuration)
#DEPRECATED|
#DEPRECATED|        # Selection ?
#DEPRECATED|        if query.filters and not announce.capabilities.selection:
#DEPRECATED|            if not allowed_capabilities.selection:
#DEPRECATED|                raise Exception, 'Cannot answer query: SELECTION'
#DEPRECATED|            add_selection = query.filters
#DEPRECATED|            query.filters = Filter()
#DEPRECATED|        else:
#DEPRECATED|            add_selection = None
#DEPRECATED|
#DEPRECATED|        # Projection ?
#DEPRECATED|        announce_fields = announce.get_table().get_fields()
#DEPRECATED|        if query.fields < announce_fields and not announce.capabilities.projection:
#DEPRECATED|            if not allowed_capabilities.projection:
#DEPRECATED|                raise Exception, 'Cannot answer query: PROJECTION'
#DEPRECATED|            add_projection = query.fields
#DEPRECATED|            query.fields = set()
#DEPRECATED|        else:
#DEPRECATED|            add_projection = None
#DEPRECATED|
#DEPRECATED|        table = Table({platform:''}, {}, query.get_from(), set(), set())
#DEPRECATED|        key = metadata.get_key(query.get_from())
#DEPRECATED|        capabilities = metadata.get_capabilities(platform, query.get_from())
#DEPRECATED|        self.ast = self.ast.From(table, query, capabilities, key)
#DEPRECATED|
#DEPRECATED|        # XXX associate the From node to the Gateway
#DEPRECATED|        from_node = self.ast.get_root()
#DEPRECATED|        self.add_from(from_node)
#DEPRECATED|        #from_node.set_gateway(gw_or_router)
#DEPRECATED|        #gw_or_router.query = query
#DEPRECATED|
#DEPRECATED|        if not self.root:
#DEPRECATED|            return
#DEPRECATED|        if add_selection:
#DEPRECATED|            self.ast.optimize_selection(add_selection)
#DEPRECATED|        if add_projection:
#DEPRECATED|            self.ast.optimize_projection(add_projection)
#DEPRECATED|
#DEPRECATED|        self.inject_at(query)
#DEPRECATED|
#DEPRECATED|    #@returns(ResultValue)
#DEPRECATED|    def execute(self, is_deferred = False, receiver = None):
#DEPRECATED|        """
#DEPRECATED|        Execute the QueryPlan in order to query the appropriate
#DEPRECATED|        sources of data, collect, combine and returns the records
#DEPRECATED|        requested by the user.
#DEPRECATED|        Args:
#DEPRECATED|            deferred: A twisted.internet.defer.Deferred instance (async query)
#DEPRECATED|                or None (sync query)
#DEPRECATED|            receiver: An instance supporting the method set_result_value or None.
#DEPRECATED|                receiver.set_result_value() will be called once the Query has terminated.
#DEPRECATED|        Returns:
#DEPRECATED|            The updated Deferred instance (if any) 
#DEPRECATED|        """
#DEPRECATED|        # Check whether the AST (Abstract Syntax Tree), which describes the QueryPlan.
#DEPRECATED|        assert self.ast, "Uninitialized AST"
#DEPRECATED|
#DEPRECATED|        # create a Callback object with deferred object as arg
#DEPRECATED|        # manifold/util/callback.py 
#DEPRECATED|        deferred = Deferred() if is_deferred else None
#DEPRECATED|        callback = Callback(deferred)
#DEPRECATED|
#DEPRECATED|        # Run the QueryPlan
#DEPRECATED|        self.ast.set_callback(callback)
#DEPRECATED|        self.ast.start()
#DEPRECATED|
#DEPRECATED|        return deferred if is_deferred else callback.get_results()

    def dump(self):
        """
        Dump this QueryPlan to the standard output.
        """
        print ""
        print "QUERY PLAN:"
        print "-----------"
        self.ast.dump()
        print ""
        print ""

