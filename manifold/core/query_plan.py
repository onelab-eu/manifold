#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convert a 3nf-tree into an AST (e.g. a query plan)
# See:
#   manifold/core/ast.py
# 
# QueryPlan class builds, process and executes Queries
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loïc Baron        <loic.baron@lip6.fr>


# XXX Note for later: what about holes in the subquery chain. Is there a notion
# of inject ? How do we collect subquery results two or more levels up to match
# the structure (with shortcuts) as requested by the user.

from types                          import StringTypes

from manifold.core.ast              import AST
from manifold.core.explore_task     import ExploreTask
from manifold.core.node             import Node
from manifold.core.query            import ACTION_CREATE, ACTION_UPDATE
from manifold.core.stack            import Stack
from manifold.operators.From        import From 
from manifold.operators.operator    import Operator
from manifold.util.log              import Log
from manifold.util.type             import returns, accepts

class QueryPlan(object):
    """
    Building a query plan consists in setting the AST and the list of Froms.
    """

    def __init__(self):
        """
        Constructor.
        """
        self.ast = None      # AST instance, set later thanks to set_ast()

        self.foreign_key_fields = dict()

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

        destination = query.get_destination()
        ast.get_root().format_downtree()
        if query.get_action() in [ACTION_CREATE, ACTION_UPDATE]:
            ast.reorganize_create()
        ast.optimize(destination)
        self.ast = ast

    @returns(Operator)
    def get_root_operator():
        """
        Returns:
            An Operator instance corresponding to the root node of
            the AST related to this QueryPlan, None otherwise.
        """
        return self.ast.get_root() if self.ast else None

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this QueryPlan instance.
        """
        if self.ast:
            return "QUERY PLAN:\n-----------\n%s" % self.ast
        else:
            return "(Invalid QueryPlan)"

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this QueryPlan instance.
        """
        return repr(self)

    @returns(Node)
    def build(self, query, router, db_graph, allowed_platforms, user = None):
        """
        Build the QueryPlan involving several Gateways according to a 3nf
        graph and a user Query.
        Raises:
            RuntimeError if the QueryPlan cannot be built.
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
            The corresponding Node, None in case of failure
        """
        allowed_capabilities = router.get_capabilities()

        root_table = db_graph.find_node(query.get_from())
        if not root_table:
            table_names = list(db_graph.get_table_names())
            table_names.sort()
            raise RuntimeError("Cannot find '%s' Table in db_graph. Known Tables are: {%s}" % (
                query.get_from(),
                ", ".join(table_names)
            ))

        if not root_table.get_capabilities().retrieve:
            raise RuntimeError("Table '%s' hasn't RETRIEVE capability and cannot be used in a FROM clause:\n%s" % (
                query.get_from(),
                root_table
            ))
       
        root_task = ExploreTask(router, root_table, relation = None, path = list(), parent = self, depth = 1)
        if not root_task:
            raise RuntimeError("Unable to build a suitable QueryPlan")
        root_task.addCallback(self.set_ast, query)

        stack = Stack(root_task)
        seen = dict() # path -> set()

        missing_fields = set()
        if query.get_fields().is_star():
            missing_fields |= root_table.get_field_names()
        else:
            missing_fields |= query.get_fields()
        missing_fields |= query.get_filter().get_field_names()
        missing_fields |= set(query.get_params().keys())

        while missing_fields:
            # Explore the next prior ExploreTask
            task = stack.pop()

            # The Stack is empty, so we have explored the DBGraph
            # without finding the every queried fields.
            if not task:
                raise RuntimeError("Exploration terminated without finding fields: %r" % missing_fields)

            pathstr = '.'.join(task.path)
            if not pathstr in seen:
                seen[pathstr] = set()

            # foreign_key_fields are fields added because indirectly requested by the user.
            # For example, he asked for slice.resource, which in fact will contain slice.resource.urn

            foreign_key_fields = task.explore(stack, missing_fields, db_graph, allowed_platforms, allowed_capabilities, user, seen[pathstr], query_plan = self)
            
            self.foreign_key_fields.update(foreign_key_fields)

        # Cancel every remaining ExploreTasks, we cannot found additional 
        # queried fields.
        while not stack.is_empty():
            task = stack.pop()
            task.cancel()
    
        # Do we need to wait for self.ast here ?
        #Log.debug(self)
        return self.ast.get_root() if self.ast else None

