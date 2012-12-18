#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convert a 3nf-tree into an AST (e.g. a query plan)
# \sa tophat/util/pruned_tree.py
# \sa tophat/core/ast.py
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr> 

from tophat.core.ast            import AST, From, Union, LeftJoin
from tophat.core.table          import Table 
from tophat.core.query          import Query 
from networkx                   import DiGraph
from tophat.util.type           import returns, accepts
from tophat.models.user         import User

@accepts(DiGraph)
def find_root(tree):
    """
    \brief (Internal use) Search the root node of a tree
    \param tree A DiGraph instance representing a tree
    \return The corresponding root node
    """
    for u in tree.nodes():
        if not tree.in_edges(u):
            return u
    raise ValueError("No root node found: edges: {%r}" % [e for e in tree.edges()])

def get_from(froms, u, p):
    if u.name not in froms:
        froms[u.name] = {}
    if platform not in froms[u.name]:
        froms[u.name][platform] =  From(query, u.name, u.keys)
    return froms[u.name][platform]

def get_qp_rec(ast, tree, u, froms):
    """
    \brief (Internal use)
    \param ast The AST we are building 
    \param tree A DiGraph instance representing the 3nf-tree
    \param u The Table of the 3-nf tree we're processing
    \params froms A dictionnary which stores the From nodes
        already inserted.
        froms[table_name][platforms]
    """
#    query = Query(fact_table = u.name)
#    print "u = %s" % u
#
#    # Map a right table to its left tables
#    map_joins = dict() 
#    set_unions = set()
#
#    # For each v successor of u
#    for e_uv in tree.out_edges(u):
#        (_, v) = e_uv
#        e_uv_type = tree[u][v]["type"]
#        u_childs = []
#
#        if e_uv_type in ["-->", "~~>"]:
#            # Check consistency
#            if u.get_platforms() != v.get_platforms():
#                raise ValueError("Inconsistant platforms: (%s %r %s): %s %s", (u, e_uv_type, v, u.get_platforms(), v.get_platforms())
#
#            # For each up in u, for each vp in v, memorize "up LEFT JOIN vp"
#            plaforms_uv = u.get_platforms()
#            for p in plaforms_uv :
#                up = get_from(froms, u, p) 
#                vp = get_from(froms, v, p)
#                if up no in map_joins:
#                    map_joins = set()
#                map_joins[up].add(vp)
#
#            # Memorize that we've to build a "UNION of each up"
#            set_union.add(u.name)
#
#        elif e_uv_type == "==>":
#            # Check consistency
#            if u.get_platforms() <= v.get_platforms():
#                raise ValueError("Inconsistant platforms: (%s %r %s): %s %s", (u, e_uv_type, v, u.get_platforms(), v.get_platforms())
#
#            # platforms in u provide more information than those in v
#            # If u in the prune_tree, it means that we require some fields
#            # only in u, so only the platforms related to u can help.
#            
#        else:
#            raise ValueError("Invalid arc type: (%s %r %s)" % (u, e_uv_type, v))
#                 
#            
#
#        print "(u,v) = (%r %s %r)" % (u, tree[u][v]["type"], v)
        

@accepts(User, DiGraph)
@returns(AST)
def build_query_plane(user, pruned_tree):
    """
    \brief Compute a query plane according to a pruned tree
    \param user The User instance representing the user issuing the query
        \sa tophat/model/user.py
    \param pruned_tree A DiGraph instance representing the 3nf-tree
        such as each remaining key in and each remaining field
        (stored in the DiGraph nodes) is needed 
        - either because it is explicitly queried by the user or either because
        - either because it is needed to join tables involved in the 3nf-tree)
    \return an AST instance which describes the resulting query plane
    """
    ast = AST(user = user)
    root_table = find_root(pruned_tree)
    froms = {}
    get_qp_rec(ast, pruned_tree, root_table, froms)
    return ast
