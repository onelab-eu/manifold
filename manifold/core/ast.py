#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Abstract Syntax Tree: 
#   An AST is used to build and represent a QueryPlan.
#   It manages a set of interconnected Operator instances 
#   inspired from the SQL operators (SELECT, FROM, UNION,
#   LEFT JOIN, ...).
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys, random, traceback
from types                                  import StringTypes

from manifold.core.key                      import Key
from manifold.core.query                    import Query
from manifold.operators.cartesian_product   import CartesianProduct
from manifold.operators.dup                 import Dup
from manifold.operators.From                import From
from manifold.operators.from_table          import FromTable
from manifold.operators.left_join           import LeftJoin
from manifold.operators.operator            import Operator
from manifold.operators.rename              import Rename
from manifold.operators.projection          import Projection
from manifold.operators.selection           import Selection
from manifold.operators.subquery            import SubQuery
from manifold.operators.union               import Union
from manifold.util.log                      import Log
from manifold.util.predicate                import Predicate, eq, contains, included
from manifold.util.type                     import returns, accepts

#------------------------------------------------------------------
# AST (Abstract Syntax Tree)
#------------------------------------------------------------------

class AST(object):
    """
    An AST (Abstract Syntax Tree) is used to represent a Query Plan.
    It acts as a factory (see example at the end of this file).
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router):
        """
        Constructor.
        Args:
            router: The Router which instanciate this AST.
        """
        self._router = router
        self.root = None # Points to the root operator

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(Operator)
    def get_root(self):
        """
        Returns:
            The root Operator of this AST.
        """
        return self.root

    def set_root(self, root):
        self.root = root

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif the AST has no Node.
        """
        return self.root == None

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
        assert isinstance(query, Query), "Invalid query = %s (%s)" % (query, type(query))
        assert isinstance(key, Key),     "Invalid key = %s (%s)" % (key, type(key))

        # Retrieve the appropriate Gateway.
        gateway = self._router.get_gateway(platform_name)

        # Build the corresponding From Operator and connect it to this AST.
        self.root = From(gateway, query, capabilities, key)

        # Eventually add a rename operator to translate between domains
        try:
            instance = gateway.get_object(query.get_table_name())
            aliases  = instance.get_aliases()
            if aliases:
                self.root = Rename(self.get_root(), aliases)
        except:
            # XXX No method get_object
            pass

        return self

    #@returns(AST)
    def from_table(self, query, records, key):
        """
        Append a FromTable Node to this AST.
        Args:
            query: A Query instance describing the Records provided by this
                FromTable Node.
            records: A list of corresponding Records.
            key: A Key instance which can be used to identify each Records.
        Returns:
            The resulting AST.
        """
        assert self.is_empty(),          "Should be instantiated on an empty AST"
        assert isinstance(query, Query), "Invalid query = %r (%r)" % (query, type(query))
        assert isinstance(key, Key),     "Invalid key = %s (%s)" % (key, type(key))

        self.root = FromTable(query, records, key)
        return self

    #@returns(AST)
    def union(self, child_asts, key):
        """
        Make an AST which is the UNION of self (left operand) and child_asts (right operand)
        Args:
            child_asts: A list of AST gathered by this Union operator.
            key: A Key instance which can be used to identify each Records.
        Returns:
            The AST corresponding to the UNION.
        """
        # We only need a key for UNION distinct, not supported yet
        assert isinstance(child_asts, list),\
            "Invalid children AST for UNION: %s (%s)" % (child_asts, type(child_asts))
        assert not key or isinstance(key, Key),\
            "Invalid key %r (type %r)" % (key, type(key))

        child_roots = [ast.get_root() for ast in child_asts]

        # If the ast is empty
        if self.is_empty():
            self.root = child_roots[0] if len(child_roots) == 1 else Union(child_roots, key)
            return self

        # If the root node a UNION, in this case we extend it with the set of child_asts
        if isinstance(self.get_root(), Union):
            # From the query plan construction, we are assured both UNION have the same key
            union = self.get_root()
            #MANDO|union.add_child_roots([ast.get_root() for ast in child_asts])
            for ast in child_asts:
                ast.get_root().add_consumer(union)
        else:
            # We insert the current root to the list of child_roots
            child_roots.insert(0, self.get_root())

            # We have at least 2 child_roots, let's do Union
            self.root = Union(child_roots, key)

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

        self.root = LeftJoin(predicate, self.get_root(), right_child.get_root())
        return self

    #@returns(AST)
    def dup(self, key):
        """
        Append a Dup Node above this AST
        Args:
            key: A Key instance, allowing to detecting duplicates
        Returns:
            The updated AST. 
        """
        assert not self.is_empty(), "AST not initialized"

        self.root = Dup(self.get_root(), key)
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
    def subquery(self, producer, relation):
        self.root = SubQuery(self.get_root(), [(producer, relation)])
        return self

#MANDO|    #@returns(AST)
#MANDO|    def subquery(self, ast, relation):
#MANDO|        """
#MANDO|        Append a SubQuery Node above the current AST, which will be used as the
#MANDO|        main query of this SubQuery.
#MANDO|
#MANDO|        Args:
#MANDO|            ast:
#MANDO|            relation:
#MANDO|
#MANDO|        Returns:
#MANDO|            The resulting AST.
#MANDO|
#MANDO|        Note:
#MANDO|            We have a single subquery child here.
#MANDO|        """
#MANDO|        assert not self.is_empty(), "AST not initialized"
#MANDO|
#MANDO|#MANDO|        self.update_root(lambda root: root.subquery(ast.get_root(), relation))
#MANDO|#MANDO|        return self
#MANDO|
#MANDO|        self.root = SubQuery(self.get_root(), [(ast.get_root(), relation)])
#MANDO|        return self

#UNUSED|    #@returns(AST)
#UNUSED|    def subqueries(self, children_ast_relation_list):
#UNUSED|        """
#UNUSED|        Append a SubQuery Node above the current AST, which will be used as the
#UNUSED|        main query of this SubQuery.
#UNUSED|        Args:
#UNUSED|            children_ast_relation_list: A list of (AST, Relation) tuples
#UNUSED|            corresponding to each subquery (children) involved in this
#UNUSED|            SubQuery Node)
#UNUSED|        Returns:
#UNUSED|            The resulting AST.
#UNUSED|        """
#UNUSED|        assert not self.is_empty(), "AST not initialized"
#UNUSED|
#UNUSED|        children = map(lambda (ast, relation): (ast.get_root(), relation), children_ast_relation_list)
#UNUSED|
#UNUSED|        # children is a list of (Producer, Relation) tuples
#UNUSED|#MANDO|        self.root = SubQuery(self.get_root(), children, self._router)
#UNUSED|#MANDO|        return self
#UNUSED|
#UNUSED|        Log.warning("mando: a verifier")
#UNUSED|        self.root = SubQuery(self.get_root(), children)
#UNUSED|        return self
#UNUSED|

    #@returns(AST)
    def cartesian_product(self, children_ast_relation_list, query):
        """
        Append a CartesianProduct Node above the current AST
        Args:
            children_ast_relation_list: A list of (AST, Relation) tuples.
            query: A Query instance
        Returns:
            The resulting AST.
        """
        Log.tmp("mando: I guess that AST::cartesian_product() should conform to AST::subqueries() prototype")
        assert self.is_empty(), "Cartesian product should be done on an empty AST"

        if len(children_ast_relation_list) == 1:
            #(ast, relation) = children_ast_relation_list[0]
            try:
                (ast, relation), = children_ast_relation_list
            except ValueError, e:
                Log.tmp("children_ast_relation_list = %s" % children_ast_relation_list)
                raise ValueError(e)
            self.root = ast.get_root()
        else:
            children = map(lambda (ast, relation): (ast.get_root(), relation), children_ast_relation_list)
            self.root = CartesianProduct(children, query)

        return self


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this AST instance.
        """
        if self.is_empty(): return "Empty AST"
        return self.get_root().format_downtree()

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this AST instance.
        """
        return repr(self)

    #---------------------------------------------------------------------------
    # AST manipulations & optimization
    #---------------------------------------------------------------------------

    def update_root(self, function):
        """
        Apply a callback function on this AST.
        Args:
            function: A callback receiving in parameter the AST self.get_root()
                and returning the resulting AST.
        """
        new_root = function(self.get_root())
        if new_root:
            self.set_root(new_root)

    def optimize(self, destination):
        """
        Optimize this AST according to the Query issued by the user.
        For instance, SELECT and WHERE clause allows to fetch less
        information from the data sources involved in the query plan.
        Args:
            destination: The Destination of the Query issued by the user.
        """
        try: # DEBUG
            self.optimize_selection(destination.get_filter())
            self.optimize_projection(destination.get_field_names())
        except Exception, e:
            Log.error(traceback.format_exc())

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
        producer = self.get_root().optimize_selection(filter)
        assert producer, "ast::optimize_selection() has failed: filter = %s" % filter
        self.root = producer

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
        producer = self.get_root().optimize_projection(fields)
        assert producer, "ast::optimize_projection() has failed: fields = %s" % fields 
        self.root = producer

#DEPRECATED|    def reorganize_create(self):
#DEPRECATED|        new_root = self.get_root().reorganize_create()
#DEPRECATED|        self.set_root(new_root)
