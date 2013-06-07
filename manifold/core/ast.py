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
from copy                          import copy, deepcopy
from types                         import StringTypes
from manifold.core.filter          import Filter
from manifold.core.query           import Query, AnalyzedQuery
from manifold.core.table           import Table 
from manifold.core.field           import Field
from manifold.core.key             import Key
from manifold.core.capabilities    import Capabilities
from manifold.operators.From       import From
from manifold.operators.selection  import Selection
from manifold.operators.projection import Projection
from manifold.operators.left_join  import LeftJoin
from manifold.operators.union      import Union
from manifold.operators.subquery   import SubQuery
from manifold.operators.demux      import Demux
from manifold.operators.dup        import Dup
from manifold.util.predicate       import Predicate, eq, contains, included
from manifold.util.type            import returns, accepts
from manifold.util.log             import Log

#------------------------------------------------------------------
# AST (Abstract Syntax Tree)
#------------------------------------------------------------------

class AST(object):
    """
    Abstract Syntax Tree used to represent a Query Plane.
    Acts as a factory.
    """

    def __init__(self, user = None):
        """
        \brief Constructor
        \param user A User instance
        """
        self.user = user
        # The AST is initially empty
        self.root = None

    def get_root(self):
        """
        \return The root Node of this AST (if any), None otherwise
        """
        return self.root

    @returns(bool)
    def is_empty(self):
        """
        \return True iif the AST has no Node.
        """
        return self.get_root() == None


    #@returns(AST)
    def From(self, platform, query, capabilities, key):
        """
        \brief Append a FROM Node to this AST
        \param table The Table wrapped by the FROM operator
        \param query The Query sent to the platform
        \return The updated AST
        """
        assert self.is_empty(),                 "Should be instantiated on an empty AST"
        #assert isinstance(table, Table),        "Invalid table = %r (%r)" % (table, type(table))
        assert isinstance(query, Query),        "Invalid query = %r (%r)" % (query, type(query))
        #assert len(table.get_platforms()) == 1, "Table = %r should be related to only one platform" % table

        # USELESS ? # self.query = query
#OBSOLETE|        platforms = table.get_platforms()
#OBSOLETE|        platform = list(platforms)[0]

        node = From(platform, query, capabilities, key)
        self.root = node
        self.root.set_callback(self.get_callback())
        return self

    #@returns(AST)
    def union(self, children_ast, key):
        """
        \brief Make an AST which is the UNION of self (left operand) and children_ast (right operand)
        \param children_ast A list of AST gathered by this UNION operator
        \param key A Key instance
            \sa manifold.core.key.py 
        \return The AST corresponding to the UNION
        """
        # We only need a key for UNION distinct, not supported yet
        assert not key or isinstance(key, Key), "Invalid key %r (type %r)"          % (key, type(key))
        assert isinstance(children_ast, list),  "Invalid children_ast %r (type %r)" % (children_ast, type(children_ast))
        assert len(children_ast) != 0,          "Invalid UNION (no child)"

        Log.debug("AST children")
        Log.debug(children_ast)
        Log.debug(key)

        # If the current AST has already a root node, this node become a child
        # of this Union node ...
        old_root = None
        if not self.is_empty():
            old_root = self.get_root()
            children = [self.get_root()]
        else:
            children = []

        # ... as the other children
        children.extend([ast.get_root() for ast in children_ast])

        if len(children) > 1:
            self.root = Union(children, key)
        else:
            self.root = children[0]
        if old_root:
            self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def left_join(self, right_child, predicate):
        """
        \brief Make an AST which is the LEFT JOIN of self (left operand) and children_ast (right operand) 
            self ⋈ right_child
        \param right_child An AST instance (right operand of the LEFT JOIN )
        \param predicate A Predicate instance used to perform the join 
        \return The resulting AST
        """
        assert isinstance(right_child, AST),     "Invalid right_child = %r (%r)" % (right_child, type(right_child))
        assert isinstance(predicate, Predicate), "Invalid predicate = %r (%r)" % (predicate, type(Predicate))
        assert not self.is_empty(),              "No left table"

        old_root = self.get_root()
        self.root = LeftJoin(old_root, right_child.get_root(), predicate)#, None)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def demux(self):
        """
        \brief Append a DEMUX Node above this AST
        \return The updated AST 
        """
        assert not self.is_empty(),      "AST not initialized"

        old_root = self.get_root()
        self.root = Demux(old_root)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def dup(self, key):
        """
        \brief Append a DUP Node above this AST
        \param key A Key instance, allowing to detecting duplicates
        \return The updated AST 
        """
        assert not self.is_empty(),      "AST not initialized"

        old_root = self.get_root()
        self.root = Dup(old_root, key)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def projection(self, fields):
        """
        \brief Append a SELECT Node (Projection) above this AST
            ast <- π_fields(ast)
        \param fields the list of fields on which to project
        \return The AST corresponding to the SELECT 
        """
        assert not self.is_empty(),      "AST not initialized"
        assert isinstance(fields, list), "Invalid fields = %r (%r)" % (fields, type(fields))

        old_root = self.get_root()
        self.root = Projection(old_root, fields)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def selection(self, filters):
        """
        \brief Append a WHERE Node (Selection) above this AST
            ast <- σ_filters(ast)
        \param filters A set of Predicate to apply
        \return The AST corresponding to the WHERE 
        """
        assert not self.is_empty(),      "AST not initialized"
        assert isinstance(filters, set), "Invalid filters = %r (%r)" % (filters, type(filters))
        assert filters != set(),         "Empty set of filters"

        old_root = self.get_root()
        self.root = Selection(old_root, filters)
        self.root.set_callback(old_root.get_callback())
        return self

    #@returns(AST)
    def subquery(self, children_ast_predicate_list, parent_key=None): # PARENT KEY DEPRECATED
        """
        \brief Append a SUBQUERY Node above the current AST
        \param children_ast the set of children AST to be added as subqueries to
            the current AST
        \return AST corresponding to the SUBQUERY
        """
        assert not self.is_empty(), "AST not initialized"
        old_root = self.get_root()

        self.root = SubQuery(old_root, children_ast_predicate_list, parent_key) # PARETN KEY DEPRECATED
        self.root.set_callback(old_root.get_callback())
        return self

    def dump(self, indent = 0):
        """
        \brief Dump the current AST
        \param indent current indentation
        """
        if self.is_empty():
            print "Empty AST (no root)"
        else:
            self.root.dump(indent)

    def start(self):
        """
        \brief Propagates a START message through the AST
        """
        assert not self.is_empty(), "Empty AST, cannot send START message"
        self.get_root().start()

    @property
    def callback(self):
        Log.info("I: callback property is deprecated")
        return self.root.callback

    @callback.setter
    def callback(self, callback):
        Log.info("I: callback property is deprecated")
        self.root.callback = callback

    def get_callback(self):
        return self.root.get_callback()

    def set_callback(self, callback):
        self.root.set_callback(callback)

    def optimize(self):
        self.root.optimize()

    def optimize_selection(self, filter):
        if not filter: return
        old_cb = self.get_callback()
        self.root = self.root.optimize_selection(filter)
        self.set_callback(old_cb)

    def optimize_projection(self, fields):
        if not fields: return
        old_cb = self.get_callback()
        self.root = self.root.optimize_projection(fields)
        self.set_callback(old_cb)

#------------------------------------------------------------------
# Example
#------------------------------------------------------------------

def main():
    q = Query("get", "x", [], {}, ["x", "z"], None)

    x = Field(None, "int", "x")
    y = Field(None, "int", "y")
    z = Field(None, "int", "z")

    a = Table(["p"], None, "A", [x, y], [Key([x])])
    b = Table(["p"], None, "B", [y, z], [Key([y])])
    
    ast = AST().From(a, q).left_join(
        AST().From(b, q),
        Predicate(a.get_field("y"), "=", b.get_field("y"))
    ).projection(["x"]).selection(set([Predicate("z", "=", 1)]))

    ast.dump()

if __name__ == "__main__":
    main()

