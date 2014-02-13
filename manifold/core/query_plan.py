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

# NOTE: The fastest way to traverse all edges of a graph is via
# adjacency_iter(), but the edges() method is often more convenient.

import copy, random
from types                         import StringTypes
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

        self.foreign_key_fields = dict()

    def add_from(self, from_node):
        """
        Add a From node to the query plan. FromTable Node are not stored
        in self.froms.
        """
        if isinstance(from_node, From):
            self.froms.append(from_node)

    def get_result_value_array(self):
        # Iterate over gateways to get their result values
        # XXX We might need tasks
        result = []
        for from_node in self.froms:
            # If no Gateway 
            if not from_node.gateway: continue
            result.extend(from_node.gateway.get_result_value())
        return result

    def inject_at(self, query):
        """
        Update From Nodes of the QueryPlan in order to take into account AT
        clause involved in a user Query.
        Args:
            query: The Query issued by the user.
        """
        Log.warning("HARDCODED: AT injection in FROM Nodes: %r" % self.froms)
        for from_node in self.froms:
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
        new_query = query.copy()

        removed_fields    = set(self.foreign_key_fields.keys())
        additional_fields = reduce(lambda x, y: x | y, self.foreign_key_fields.values(), set())

        new_query.fields |= additional_fields
        new_query.fields -= removed_fields

        ast.optimize(new_query)
        self.inject_at(new_query)
        self.ast = ast
    
        # Update the main query to add applicative information such as action and params
        # Update the main query to add applicative information such as action and params
        # NOTE: I suppose params cannot have '.' inside
        for from_node in self.froms:
            q = from_node.get_query()
            if q.get_from() == query.get_from():
                q.action = query.get_action()
                q.params = query.get_params()



        # For example "UPDATE slice SET resource", since we have a backwards relation, we need update in the children query
        # This should be done when the query is forwarded through the query plan (routerv2)
        # In the mean time... we can assume unique names and continue looking at FROms... that will break for sure...
# BETTER BUT NOT USED BECAUSE OF update_slice|        # XXX an update in a subquery should be both an INSERT and a DELETE
# BETTER BUT NOT USED BECAUSE OF update_slice|        # XXX that's why for updates we will rely on update_slice !!!!
# BETTER BUT NOT USED BECAUSE OF update_slice|        for from_node in self.froms:
# BETTER BUT NOT USED BECAUSE OF update_slice|            q = from_node.get_query()
# BETTER BUT NOT USED BECAUSE OF update_slice|            if q.object in query.get_params():
# BETTER BUT NOT USED BECAUSE OF update_slice|                q.action = query.get_action()
# BETTER BUT NOT USED BECAUSE OF update_slice|                print "q.object", q.object, " -->", query.get_params()[q.object]
# BETTER BUT NOT USED BECAUSE OF update_slice|                q.params = {q.object : query.get_params()[q.object]}

    def build(self, query, metadata, allowed_platforms, allowed_capabilities, user = None):
        """
        Build the QueryPlan involving several Gateways according to a 3nf
        graph and a user Query. If only one Gateway is involved, you should
        use QueryPlan::build_simple.
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
            Log.error("query_plan::build(): Cannot find %s in metadata, known tables are %s" % (query.get_from(), metadata.get_table_names()))
        
        root_task = ExploreTask(root, relation=None, path=[], parent=self, depth=1)
        root_task.addCallback(self.set_ast, query)

        stack = Stack(root_task)
        seen = {} # path -> set()

        missing_fields  = set()
        missing_fields |= query.get_select()
        missing_fields |= query.get_where().get_field_names()
        missing_fields |= set(query.get_params().keys())


        while missing_fields:
            task = stack.pop()
            if not task:
                # Exploration ends here
                Log.warning("Exploration terminated without finding fields: %r" % missing_fields)
                break

            pathstr = '.'.join(task.path)
            if not pathstr in seen:
                seen[pathstr] = set()

            # ROUTERV2
            # foreign_key_fields are fields added because indirectly requested by the user.
            # For example, he asked for slice.resource, which in fact will contain slice.resource.urn
            foreign_key_fields = task.explore(stack, missing_fields, metadata, allowed_platforms, allowed_capabilities, user, seen[pathstr], query_plan = self)

            self.foreign_key_fields.update(foreign_key_fields)

        while not stack.is_empty():
            task = stack.pop()
            task.cancel()

        # Do we need to wait for self.ast here ?

    # XXX Note for later: what about holes in the subquery chain. Is there a notion
    # of inject ? How do we collect subquery results two or more levels up to match
    # the structure (with shortcuts) as requested by the user.

    def build_simple(self, query, metadata, allowed_capabilities):
        """
        Builds a QueryPlan (self) related to a single Gateway.
        This is used only by a Forwarder. This function will probably soon
        become DEPRECATED.
        If several Gateways are involved, you must use QueryPlan::build.
        Args:
            query: The Query issued by the user.
            metadata:
            allowed_capabilities: The Capabilities related to this Gateway.
        """
        # XXX allowed_capabilities should be a property of the query plan !

        # XXX Check whether we can answer query.object

        # Here we assume we have a single platform
        platform = metadata.keys()[0]
        announce = metadata[platform][query.get_from()] # eg. table test
        
        # Set up an AST for missing capabilities (need configuration)

        # Selection ?
        if query.filters and not announce.capabilities.selection:
            if not allowed_capabilities.selection:
                raise Exception, 'Cannot answer query: SELECTION'
            add_selection = query.filters
            query.filters = Filter()
        else:
            add_selection = None

        # Projection ?
        announce_fields = announce.get_table().get_fields()
        if query.fields < announce_fields and not announce.capabilities.projection:
            if not allowed_capabilities.projection:
                raise Exception, 'Cannot answer query: PROJECTION'
            add_projection = query.fields
            query.fields = set()
        else:
            add_projection = None

        table = Table({platform:''}, {}, query.get_from(), set(), set())
        key = metadata.get_key(query.get_from())
        capabilities = metadata.get_capabilities(platform, query.get_from())
        self.ast = self.ast.From(table, query, capabilities, key)

        # XXX associate the From node to the Gateway
        from_node = self.ast.get_root()
        self.add_from(from_node)
        #from_node.set_gateway(gw_or_router)
        #gw_or_router.query = query

        if not self.root:
            return
        if add_selection:
            self.ast.optimize_selection(add_selection)
        if add_projection:
            self.ast.optimize_projection(add_projection)

        self.inject_at(query)

    #@returns(ResultValue)
    def execute(self, is_deferred = False):
        """
        Execute the QueryPlan in order to query the appropriate
        sources of data, collect, combine and returns the records
        requested by the user.
        Args:
            deferred: may be set to None.
        Returns:
            The corresponding ResultValue instance.    
        """


        # create a Callback object with deferred object as arg
        # manifold/util/callback.py 
        d = Deferred() if is_deferred else None
        cb = Callback(d)

        # Start AST = Abstract Syntax Tree 
        # An AST represents a query plan
        # manifold/core/ast.py
        self.ast.set_callback(cb)
        self.ast.start()

        return d if is_deferred else cb.get_results()

    def dump(self):
        """
        Dump this AST to the standard output.
        """
        self.ast.dump()

