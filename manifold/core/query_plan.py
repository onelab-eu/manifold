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
from manifold.core.field_names      import FieldNames
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

    def set_ast(self, ast_sq_rename_dict, destination):
        """
        Complete an AST in order to take into account SELECT and WHERE clauses
        involved in a user Query.
        Args:
            ast: An AST instance is made of Union, LeftJoin, SubQuery [...] Nodes.
            query: The Query issued by the user.
        """
        ast, sq_rename_dict = ast_sq_rename_dict
        assert isinstance(ast, AST),\
            "Invalid ast = %s (%s)" % (ast, type(ast))

        # Final rename
        if sq_rename_dict:
            ast.rename(sq_rename_dict)

        #print "QUERY PLAN:", ast.get_root().format_downtree()
        Log.warning('no more reorganize_create')
#DEPRECATED|        if query.get_action() in [ACTION_CREATE, ACTION_UPDATE]:
#DEPRECATED|            ast.reorganize_create()
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
    def build(self, destination, router, allowed_platforms, user = None):
        """
        Build the QueryPlan involving several Gateways according to a 3nf
        graph and a user Query.
        Raises:
            RuntimeError if the QueryPlan cannot be built.
        Args:
            query: The Query issued by the user.
            allowed_platforms: A set of platform names (list of String).
                Which platforms the router is allowed to query.
                Could be used to restrict the Query to a limited set of platforms
                either because it is specified by the user Query or either due
                to the Router configuration.
            user: A User instance or None.
        Returns:
            The corresponding Node, None in case of failure
        """
        assert isinstance(allowed_platforms, set)

        object_name = destination.get_object_name()
        namespace   = destination.get_namespace()

        allowed_capabilities = router.get_capabilities()
        root_table = router.get_fib().get_object(object_name, namespace) 
        if not root_table:
            table_names = router.get_fib().get_object_names(namespace) # db_graph.get_table_names())
            table_names.sort()
            raise RuntimeError("Cannot find '%s' object in FIB. Known objects are: {%s}" % (
                object_name,
                ", ".join(table_names)
            ))

#DEPRECATED|        if not root_table.get_capabilities().retrieve:
#DEPRECATED|            key_fields = root_table.get_key().get_field_names()
#DEPRECATED|            can_do_join = root_table.get_capabilities().join and \
#DEPRECATED|                          query.get_filter().provides_key_field(key_fields)
#DEPRECATED|            if not can_do_join:
#DEPRECATED|                raise RuntimeError("Table '%s' hasn't RETRIEVE capability and cannot be used in a FROM clause:\n%s" % (
#DEPRECATED|                    query.get_table_name(),
#DEPRECATED|                    root_table
#DEPRECATED|                ))

        root_task = ExploreTask(router, root_table, relation = None, path = [root_table.get_name()], parent = self, depth = 1)
        if not root_task:
            raise RuntimeError("Unable to build a suitable QueryPlan")
        root_task.addCallback(self.set_ast, destination)

        stack = Stack(root_task)
        seen = dict() # path -> set()

        missing_fields = FieldNames()
        if destination.get_field_names().is_star():
            missing_fields |= root_table.get_field_names()
        else:
            missing_fields |= destination.get_field_names()
        missing_fields |= destination.get_filter().get_field_names()
        Log.warning('params in destination ?')
#        missing_fields |= FieldNames(destination.get_params().keys())

        while missing_fields:
            # Explore the next prior ExploreTask
            task = stack.pop()

            # The Stack is empty, so we have explored the DBGraph
            # without finding the every queried fields.
            if not task:
                raise RuntimeError("Exploration terminated without finding fields: {'%s'}" % "', '".join(missing_fields))

            # We keep track of the paths that have been traversed
            pathstr = '.'.join(task.path)
            if not pathstr in seen:
                seen[pathstr] = set()

            # foreign_key_fields are fields added because indirectly requested by the user.
            # For example, he asked for slice.resource, which in fact will contain slice.resource.urn

            #Log.tmp("missing_fields = %s task = %s" % (missing_fields, task))
            foreign_key_fields = task.explore(
                stack, missing_fields, router.get_fib(), namespace, allowed_platforms,
                allowed_capabilities, user, seen[pathstr], query_plan = self
            )

            self.foreign_key_fields.update(foreign_key_fields)

        # Cancel every remaining ExploreTasks, we cannot found additional
        # queried fields.
        while not stack.is_empty():
            #Log.tmp("stack= %s" % stack)
            task = stack.pop()
            task.cancel()

        # Do we need to wait for self.ast here ?
        #Log.debug(self)
        return self.ast.get_root() if self.ast else None

