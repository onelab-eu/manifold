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
from manifold.core.filter             import Filter
from manifold.core.query              import Query, AnalyzedQuery
from manifold.core.table              import Table 
from manifold.core.field              import Field
from manifold.core.key                import Key
from manifold.core.capabilities       import Capabilities
from manifold.operators.From          import From
from manifold.operators.from_cache    import FromCache
from manifold.operators.from_table    import FromTable
from manifold.operators.selection     import Selection
from manifold.operators.projection    import Projection
from manifold.operators.left_join     import LeftJoin
from manifold.operators.rename        import Rename
from manifold.operators.union         import Union
from manifold.operators.subquery      import SubQuery
from manifold.operators.cross_product import CrossProduct
#DEPRECATED|LOIC|from manifold.operators.demux         import Demux
from manifold.operators.dup           import Dup
from manifold.util.predicate          import Predicate, eq, contains, included
from manifold.util.type               import returns, accepts
from manifold.util.log                import Log

#------------------------------------------------------------------
# AST (Abstract Syntax Tree)
#------------------------------------------------------------------

class AST(object):
    """
    An AST (Abstract Syntax Tree) is used to represent a Query Plan.
    It acts as a factory (see example at the end of this file).
    """

    def __init__(self, user = None):
        """
        Constructor
        Args:
            user: A User instance
        """
        self.user = user
        # The AST is initially empty
        self.root = None

    def get_root(self):
        """
        Returns:
            The root Node of this AST (if any), None otherwise
        """
        return self.root

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif the AST has no Node.
        """
        return self.get_root() == None

    #@returns(AST)
    def From(self, platform, query, capabilities, key):
        """
        Append a From Node to this AST.
        Args:
            platform: The Platform related to this From Node. 
            query: The Query sent to the platform.
            capabilities: The Capabilities related to this Table of this Platform.
            key: The Key related to this Table.
        Returns:
            The updated AST.
        """
        assert self.is_empty(),          "Should be instantiated on an empty AST"
        assert isinstance(query, Query), "Invalid query = %r (%r)" % (query, type(query))
#        assert capabilities.virtual,     "Cannot build a From Node with a virtual Table, you should use AST::from_table()"

        self.root = From(platform, query, capabilities, key)
        self.root.set_callback(self.get_callback())

        return self

    def from_cache(self, query, cache_entry):
        """
        Append a FromCache Node to this AST.
        """
        self.root = FromCache(query, cache_entry)
        self.root.set_callback(self.get_callback())
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
        assert children_ast is not None, "Invalid children AST for UNION"

        # We treat the general case when we are provided with a list of children_ast
        if not isinstance(children_ast, list):
            children_ast = [children_ast]
        children = [ast.get_root() for ast in children_ast]

        # If the ast is empty
        if self.is_empty():
            self.root = children[0] if len(children) == 1 else Union(children, key)
            return self

        # If the root node a UNION, in this case we extend it with the set of children_ast
        if isinstance(self.get_root(), Union):
            # From the query plan construction, we are assured both UNION have the same key
            union = self.get_root()
            union.add_children([ast.get_root() for ast in children_ast])
        else:
            # We insert the current root to the list of children
            new_first_child = self.get_root()
            old_cb = self.root.get_callback()

            children.insert(0, new_first_child)
            # We have at least 2 children
            self.root = Union(children, key)
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

        # In PARENT relationships, we are JOINing two same tables
        #left_query = self.get_root().get_query()
        #right_query = right_child.get_root().get_query()
        #if left_query.object == right_query.object:
        #    # XXX Add check on primary keys
        #    left_query.fields |= right_query.fields
        #    return self

        self.root = LeftJoin(self.get_root(), right_child.get_root(), predicate)#, None)
        return self

    #@returns(AST)
#DEPRECATED|LOIC|    def demux(self):
#DEPRECATED|LOIC|        """
#DEPRECATED|LOIC|        Append a DEMUX Node above this AST.
#DEPRECATED|LOIC|        Returns:
#DEPRECATED|LOIC|            The updated AST. 
#DEPRECATED|LOIC|        """
#DEPRECATED|LOIC|        assert not self.is_empty(), "AST not initialized"
#DEPRECATED|LOIC|
#DEPRECATED|LOIC|        old_root = self.get_root()
#DEPRECATED|LOIC|        self.root = Demux(old_root)
#DEPRECATED|LOIC|        self.root.set_callback(old_root.get_callback())
#DEPRECATED|LOIC|        return self

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

    def rename(self, rename_dict):
        if not rename_dict:
            return self
        self.root = Rename(self.get_root(), rename_dict)
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
                Log.tmp("children_ast_relation_list = %s")
                raise ValueError(e)
            self.root = ast.get_root()
        else:
            children = map(lambda (ast, relation): (ast.get_root(), relation), children_ast_relation_list)
            self.root = CrossProduct(children, query)

        return self

    def dump(self, indent = 0):
        """
        Dump the current AST.
        Params:
            indent: An integer corresponding to the current indentation
                (number of space characters).
        """
        if self.is_empty():
            return "Empty AST (no root)"
        else:
            return self.root.dump(indent)

    def start(self):
        """
        Propagates a START message through the AST which is used to wake
        up each Node in order to execute the Query.
        """
        assert not self.is_empty(), "Empty AST, cannot send START message"
        self.get_root().start()

#DEPRECATED|    @property
#DEPRECATED|    def callback(self):
#DEPRECATED|        Log.info("I: callback property is deprecated")
#DEPRECATED|        return self.root.callback
#DEPRECATED|
#DEPRECATED|    @callback.setter
#DEPRECATED|    def callback(self, callback):
#DEPRECATED|        Log.info("I: callback property is deprecated")
#DEPRECATED|        self.root.callback = callback

    def get_callback(self):
        return self.root.get_callback()

    def set_callback(self, callback):
        self.root.set_callback(callback)

    def optimize(self, query):
        """
        Optimize this AST according to the Query issued by the user.
        For instance, SELECT and WHERE clause allows to fetch less
        information from the data sources involved in the query plan.
        Args:
            query: The Query issued by the user.
        """
        self.optimize_selection(query.get_where())
        self.optimize_projection(query.get_select())

    def optimize_selection(self, filter):
        """
        Apply a WHERE operation to an AST and spread this operation
        along the AST branches by adding appropriate Selection Nodes
        in the current tree.
        Args:
            filter: a set of String corresponding to the field names
                involved in the WHERE clause.
        """
        if not filter: return
        old_cb = self.get_callback()
        self.root = self.root.optimize_selection(filter)
        self.set_callback(old_cb)

    def optimize_projection(self, fields):
        """
        Apply a SELECT operation to an AST and spread this operation
        along the AST branches by adding appropriate Projection Nodes
        in the current tree.
        Args:
            filter: a set of String corresponding to the field names
                involved in the SELECT clause.
        """
        if not fields: return
        old_cb = self.get_callback()
        self.root = self.root.optimize_projection(fields)
        self.set_callback(old_cb)

#------------------------------------------------------------------
# Example
#------------------------------------------------------------------

def main():
    q = Query("get", "x", [], {}, ["x", "z"], None)

    x = Field([], "int", "x")
    y = Field([], "int", "y")
    z = Field([], "int", "z")

    a = Table(["p"], None, "A", [x, y], [Key([x])])
    b = Table(["p"], None, "B", [y, z], [Key([y])])
    
    ast = AST().From(a, q).left_join(
        AST().From(b, q),
        Predicate(a.get_field("y"), "=", b.get_field("y"))
    ).projection(["x"]).selection(set([Predicate("z", "=", 1)]))

    ast.dump()

if __name__ == "__main__":
    main()

