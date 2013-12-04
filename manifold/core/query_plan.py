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

from manifold.core.stack           import Stack
from manifold.core.explore_task    import ExploreTask

from manifold.core.ast             import AST
from manifold.core.producer        import Producer
from manifold.operators.operator   import Operator
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
        # self.ast will be set thanks to set_ast()
        self.ast = None

    def set_ast(self, ast, query):
        """
        Complete an AST in order to take into account SELECT and WHERE clauses
        involved in a user Query.
        Args:
            ast: An AST instance is made of Union, LeftJoin, SubQuery [...] Nodes.
            query: The Query issued by the user.
        """
        assert isinstance(ast, AST),\
            "Invalid ast = %s (%s)" % (ast, type(ast))

        ast.optimize(query)
        self.ast = ast
    
    @returns(Operator)
    def get_root_operator():
        """
        Returns:
            An Operator instance corresponding to the root node of
            the AST related to this QueryPlan, None otherwise.
        """
        return self.ast.get_root() if self.ast else None

    @returns(Producer)
    def build(self, query, router, db_graph, allowed_platforms, user = None):
        """
        Build the QueryPlan involving several Gateways according to a 3nf
        graph and a user Query. If only one Gateway is involved, you should
        use QueryPlan::build_simple.
        Raises:
            ValueError if the query is not coherent (invalid table name...).
            Exception if the QueryPlan cannot be built.
        Args:
            query: The Query issued by the user.
            db_graph: The 3nf graph (DBGraph instance).
            allowed_platforms: A list of platform names (list of String).
                Which platforms the router is allowed to query.
                Could be used to restrict the Query to a limited set of platforms
                either because it is specified by the user Query or either due
                to the Router configuration.
            user: A User instance or None.
        Returns:
            The corresponding Producer, None in case of failure
        """
        Log.tmp("query = %s" % query)
        allowed_capabilities = router.get_capabilities()

        root_table = db_graph.find_node(query.get_from())
        if not root_table:
            raise ValueError("Cannot find %s in db_graph, known tables are {%s}" % (
                query.get_from(),
                ", ".join(db_graph.get_table_names()))
            )
        
        root_task = ExploreTask(router, root_table, relation = None, path = list(), parent = self, depth = 1)
        if not root_task:
            raise Exception("Unable to build a suitable QueryPlan")
        root_task.addCallback(self.set_ast, query)

        stack = Stack(root_task)
        seen = dict() # path -> set()

        missing_fields = set()
        if query.get_select() == frozenset(): # SELECT * FROM root_table
            missing_fields |= root_table.get_field_names()
        else:
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
            task.explore(stack, missing_fields, db_graph, allowed_platforms, allowed_capabilities, user, seen[pathstr], query_plan = self)

        # Cancel every remaining ExploreTasks, we cannot found anymore
        # queried fields.
        while not stack.is_empty():
            task = stack.pop()
            task.cancel()
    
        # Do we need to wait for self.ast here ?
        return self.ast.get_root()

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

