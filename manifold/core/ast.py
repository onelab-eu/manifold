#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Abstract Syntax Tree: 
#   An AST represents a query plan. It is made of a set
#   of interconnected Node instance which may be an SQL
#   operator (SELECT, FROM, UNION, LEFT JOIN, ...).
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys, random
from copy                             import copy, deepcopy
from types                            import StringTypes
from manifold.core.capabilities       import Capabilities
from manifold.core.field              import Field
from manifold.core.filter             import Filter
from manifold.core.key                import Key
from manifold.core.relay              import Relay
from manifold.core.query              import Query
from manifold.core.table              import Table 
from manifold.operators.From          import From
from manifold.operators.from_table    import FromTable
from manifold.operators.selection     import Selection
from manifold.operators.projection    import Projection
from manifold.operators.left_join     import LeftJoin
from manifold.operators.union         import Union
from manifold.operators.subquery      import SubQuery
from manifold.operators.cross_product import CrossProduct
from manifold.operators.demux         import Demux
from manifold.operators.dup           import Dup
from manifold.util.predicate          import Predicate, eq, contains, included
from manifold.util.type               import returns, accepts
from manifold.util.log                import Log

#------------------------------------------------------------------
# AST (Abstract Syntax Tree)
#------------------------------------------------------------------

class AST(Relay):
    """
    An AST (Abstract Syntax Tree) is used to represent a Query Plan.
    It acts as a factory (see example at the end of this file).
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, interface, user = None):
        """
        Constructor
        Args:
            user: A User instance
        """
        Relay.__init__(self)
        self._interface = interface
        self.user = user
        # The AST is initially empty
        self.root = None


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_root(self):
        """
        Returns:
            The root Node of this AST (if any), None otherwise
        """
        return self.root


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif the AST has no Node.
        """
        return self.get_root() == None


    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    #@returns(AST)
    def From(self, platform_name, query, capabilities, key):
        """
        Append a From Node to this AST.
        Args:
            platform_name: the name of the Platform related to this From Node. 
            query: The Query sent to the platform.
            capabilities: The Capabilities related to this Table of this Platform.
            key: The Key related to this Table.
        Returns:
            The updated AST.
        """
        assert self.is_empty(),          "Should be instantiated on an empty AST"
        assert isinstance(query, Query), "Invalid query = %r (%r)" % (query, type(query))

        if platform_name == 'local':
            self.root = self._interface.get_storage()
        else:
            self.root = self._interface.get_gateway(platform_name)

        return self

    #@returns(AST)
    def from_table(self, query, records, key):
        """
        Append a FromTable Node to this AST.
        Args:
            query:
            records:
            key: A Key instance which can be used to identify each Records.
        Returns:
            The resulting AST.
        """
        self.root = FromTable(query, records, key)
        self.root.set_callback(self.get_callback())
        return self

    #@returns(AST)
    def union(self, children_ast, key):
        """
        Make an AST which is the UNION of self (left operand) and children_ast (right operand)
        Args:
            children_ast: A list of AST gathered by this Union operator.
            key: A Key instance which can be used to identify each Records.
        \return The AST corresponding to the UNION
        """
        # We only need a key for UNION distinct, not supported yet
        assert not key or isinstance(key, Key), "Invalid key %r (type %r)"          % (key, type(key))
        assert isinstance(children_ast, list),  "Invalid children_ast %r (type %r)" % (children_ast, type(children_ast))
        assert len(children_ast) != 0,          "Invalid UNION (no child)"

        # If the current AST has already a root node, this node become a child
        # of this Union node ...
        old_root = None
        if not self.is_empty():
            # # old_root = self.get_root()
            children = [self.root]
            old_cb = self.root.get_callback()
        else:
            children = []
            old_cb = None

        # ... as the other children
        children.extend([ast.get_root() for ast in children_ast])

        self.root = children[0] if len(children) == 1 else Union(children, key)
        if old_cb:
            self.root.set_callback(old_cb)

        return self

    #@returns(AST)
    def left_join(self, right_child, predicate):
        """
        Make an AST which is the LEFT JOIN of self (left operand)
        and children_ast (right operand) 
            self ⋈ right_child
        Args:
            right_child: An AST instance (right operand of the LEFT JOIN).
            predicate: A Predicate instance used to perform the LEFT JOIN.
        Returns:
            The resulting AST.
        """
        assert isinstance(right_child, AST),     "Invalid right_child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate), "Invalid predicate = %r (%r)" % (predicate, type(Predicate))
        assert not self.is_empty(),              "No left table"

        self.root = LeftJoin(self.get_root(), right_child.get_root(), predicate)#, None)
        return self

    #@returns(AST)
    def demux(self):
        """
        Append a DEMUX Node above this AST.
        Returns:
            The updated AST. 
        """
        assert not self.is_empty(), "AST not initialized"

        old_root = self.get_root()
        self.root = Demux(old_root)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def dup(self, key):
        """
        Append a Dup Node above this AST
        Args:
            key: A Key instance, allowing to detecting duplicates
        Returns:
            The updated AST 
        """
        assert not self.is_empty(), "AST not initialized"

        old_root = self.get_root()
        self.root = Dup(old_root, key)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def projection(self, fields):
        """
        Append a Projection Node (SELECT) above this AST
            ast <- π_fields(ast)
        Args:
            fields: the list of fields on which to project
        Returns:
            The resulting AST.
        """
        assert not self.is_empty(),      "AST not initialized"
        #assert isinstance(fields, list), "Invalid fields = %r (%r)" % (fields, type(fields))
        if not fields:
            return self
        self.root = Projection(self.get_root(), fields)
        return self

    #@returns(AST)
    def selection(self, filters):
        """
        Append a Selection Node (WHERE) above this AST
            ast <- σ_filters(ast)
        Args:
            filters: A set of Predicate to apply
        Returns:
            The resulting AST.
        """
        assert not self.is_empty(),      "AST not initialized"
        assert isinstance(filters, set), "Invalid filters = %r (%r)" % (filters, type(filters))
        #assert filters != set(),         "Empty set of filters"
        if not filters:
            return self
        self.root = Selection(self.get_root(), filters)
        return self

    #@returns(AST)
    def subquery(self, children_ast_relation_list):
        """
        Append a SubQuery Node above the current AST, which will be used as the
        main query of this SubQuery.
        Args:
            children_ast_relation_list: A list of (AST, Relation) tuples
            corresponding to each subquery (children) involved in this
            SubQuery Node)
        Returns:
            The resulting AST.
        """
        assert not self.is_empty(), "AST not initialized"

        children = map(lambda (ast, relation): (ast.get_root(), relation), children_ast_relation_list)
        self.root = SubQuery(self.get_root(), children)
        return self

    #@returns(AST)
    def cross_product(self, children_ast_relation_list, query):
        """
        Append a CrossProduct Node above the current AST
        Args:
            children_ast_relation_list: A list of (AST, Relation) tuples.
            query: A Query instance
        Returns:
            The resulting AST.
        """
        assert self.is_empty(), "Cross-product should be done on an empty AST"

        if len(children_ast_relation_list) == 1:
            #(ast, relation) = children_ast_relation_list[0]
            try:
                (ast, relation), _ = children_ast_relation_list
            except ValueError, e:
                Log.tmp("children_ast_relation_list = %s" % children_ast_relation_list)
                raise ValueError(e)
            self.root = ast.get_root()
        else:
            children = map(lambda (ast, relation): (ast.get_root(), relation), children_ast_relation_list)
            self.root = CrossProduct(children, query)

        return self


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def dump(self, indent = 0):
        """
        Dump the current AST.
        Params:
            indent: An integer corresponding to the current indentation
                (number of space characters).
        """
        if self.is_empty():
            print "Empty AST (no root)"
        else:
            self.root.dump(indent)

#DEPRECATED|    def start(self):
#DEPRECATED|        """
#DEPRECATED|        Propagates a START message through the AST which is used to wake
#DEPRECATED|        up each Node in order to execute the Query.
#DEPRECATED|        """
#DEPRECATED|        assert not self.is_empty(), "Empty AST, cannot send START message"
#DEPRECATED|        self.get_root().start()
#DEPRECATED|
#DEPRECATED|    @property
#DEPRECATED|    def callback(self):
#DEPRECATED|        Log.info("I: callback property is deprecated")
#DEPRECATED|        return self.root.callback
#DEPRECATED|
#DEPRECATED|    @callback.setter
#DEPRECATED|    def callback(self, callback):
#DEPRECATED|        Log.info("I: callback property is deprecated")
#DEPRECATED|        self.root.callback = callback
#DEPRECATED|
#DEPRECATED|    def get_callback(self):
#DEPRECATED|        return self.root.get_callback()
#DEPRECATED|
#DEPRECATED|    def set_callback(self, callback):
#DEPRECATED|        self.root.set_callback(callback)

    def optimize(self, query):
        """
        Optimize this AST according to the Query issued by the user.
        For instance, SELECT and WHERE clause allows to fetch less
        information from the data sources involved in the query plan.
        Args:
            query: The Query issued by the user.
        """
        self.optimize_selection(query, query.get_where())
        self.optimize_projection(query, query.get_select())

    def optimize_selection(self, query, filter):
        """
        Apply a WHERE operation to an AST and spread this operation
        along the AST branches by adding appropriate Selection Nodes
        in the current tree.
        Args:
            filter: a set of String corresponding to the field names
                involved in the WHERE clause.
        """
        if not filter: return
        self.root = self.root.optimize_selection(query, filter)
        assert not self.is_empty(), "ast::optimize_selection() has failed: filter = %s" % filter
        self.root.set_consumer(self)

    def optimize_projection(self, query, fields):
        """
        Apply a SELECT operation to an AST and spread this operation
        along the AST branches by adding appropriate Projection Nodes
        in the current tree.
        Args:
            filter: a set of String corresponding to the field names
                involved in the SELECT clause.
        """
        if not fields: return
        print "self.root=", self.root
        self.root = self.root.optimize_projection(query, fields)
        assert not self.is_empty(), "ast::optimize_projection() has failed: fields = %s" % fields 
        self.root.set_consumer(self)

