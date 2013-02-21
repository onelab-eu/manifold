#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Provides the function build_pruned_tree() which extract from a 3-nf graph
# - a "tree" (more precisely a predecessor map, typically computed thanks to a DFS) 
# - a set of needed fields (those queried by the user)
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from networkx                   import DiGraph
from tophat.util.dfs            import dfs_color
from tophat.util.type           import returns, accepts
from copy                       import deepcopy
from types                      import StringTypes

#OBSOLETE|@accepts(DiGraph)
#OBSOLETE|def check_graph_consistency(g):
#OBSOLETE|    """
#OBSOLETE|    \brief (Debug only) Check whether a graph is compatible with the functions
#OBSOLETE|        provided in this file and raise an exception in case of problem
#OBSOLETE|    \param g The 3-nf graph we're checking
#OBSOLETE|    """
#OBSOLETE|    for e in g.edges():
#OBSOLETE|        (u,v) = e
#OBSOLETE|        e_uv = g.edge[u][v]
#OBSOLETE|        e_type = e_uv["type"]
#OBSOLETE|        e_info = e_uv["info"]
#OBSOLETE|
#OBSOLETE|        # type
#OBSOLETE|        if e_type not in ["-->", "==>", "~~>"]:
#OBSOLETE|            raise ValueError("(%r %s %r) invalid type %r" % (u, e_type, v, e_type))
#OBSOLETE|        if e_type == "==>":
#OBSOLETE|            continue
#OBSOLETE|
#OBSOLETE|        # info must be filled for ~~> and -->
#OBSOLETE|        if not isinstance(e_info, set):
#OBSOLETE|            raise ValueError("(%r %s %r) set expected: %r" % (u, e_type, v, e_info))
#OBSOLETE|
#OBSOLETE|@accepts(DiGraph, set, dict)
#OBSOLETE|@returns(dict)
#OBSOLETE|def compute_required_fields(g, needed_fields, map_vertex_pred):
#OBSOLETE|    """
#OBSOLETE|    \brief (Internal usage)
#OBSOLETE|        Compute for each table of "g" which fields are relevant
#OBSOLETE|        in the query plane
#OBSOLETE|    \param g The 3-nf tree
#OBSOLETE|    \param needed_fields The fields queried by the user
#OBSOLETE|    \param map_vertex_pred The predecessor map computed according to dfs()
#OBSOLETE|        \sa tophat/util/dfs.py
#OBSOLETE|    \return A dictionnary which maps for each table the set of required fields
#OBSOLETE|    """
#OBSOLETE|    print "map_vertex_pred = %r" % map_vertex_pred
#OBSOLETE|    map_vertex_fields = {}
#OBSOLETE|    check_graph_consistency(g)
#OBSOLETE|
#OBSOLETE|    # For each edge (u->v) referenced in map_vertex_pred retrieve
#OBSOLETE|    # - the fields we need in u
#OBSOLETE|    # - the fields we need in v (not provided by u)
#OBSOLETE|    # - the keys   we need in v
#OBSOLETE|    # Since g is 3-nf, each field (not in a key) is crossed only once (or never if unreachable)
#OBSOLETE|    # Fields involved in a key connecting (u->v) are retrieved in u
#OBSOLETE|    for v, u in map_vertex_pred.items():
#OBSOLETE|        # Skip this element, (u->v) is not in the tree
#OBSOLETE|        if not u:
#OBSOLETE|            continue
#OBSOLETE|
#OBSOLETE|        # Allocate map_vertex_fields[*] if needed
#OBSOLETE|        if u not in map_vertex_fields:
#OBSOLETE|            map_vertex_fields[u] = set()
#OBSOLETE|        if v not in map_vertex_fields:
#OBSOLETE|            map_vertex_fields[v] = set()
#OBSOLETE|
#OBSOLETE|        # Dispatch remaining needed fields to u
#OBSOLETE|        fields_u = u.get_field_names() & needed_fields
#OBSOLETE|        map_vertex_fields[u] |= fields_u
#OBSOLETE|        needed_fields -= fields_u
#OBSOLETE|
#OBSOLETE|        # Dispatch remaining needed fields to v
#OBSOLETE|        fields_v = v.get_field_names() & needed_fields
#OBSOLETE|        map_vertex_fields[v] |= fields_v
#OBSOLETE|        needed_fields -= fields_v
#OBSOLETE|
#OBSOLETE|        # We may need further fields to traverse (u, v) (e.g. to join u and v
#OBSOLETE|        # where u stands for the left table and v for the right one).
#OBSOLETE|        e_uv = g.edge[u][v]
#OBSOLETE|#OBSOLETE|        type_e_uv = e_uv["type"]
#OBSOLETE|#OBSOLETE|        if type_e_uv in ["-->", "~~>"]:
#OBSOLETE|#OBSOLETE|            (fields_u, fields_v) = (e_uv["info"], None)
#OBSOLETE|#OBSOLETE|            if fields_u:
#OBSOLETE|#OBSOLETE|                map_vertex_fields[u] |= set(fields_u)
#OBSOLETE|#OBSOLETE|            else:
#OBSOLETE|#OBSOLETE|                raise ValueError("Inconsistent arc (%r %s %r)" % (u, type_e_uv, v))
#OBSOLETE|#OBSOLETE|            if fields_v:
#OBSOLETE|#OBSOLETE|                map_vertex_fields[v] |= set(fields_v)
#OBSOLETE|#OBSOLETE|        elif type_e_uv == "==>":
#OBSOLETE|#OBSOLETE|            pass
#OBSOLETE|#OBSOLETE|        else:
#OBSOLETE|#OBSOLETE|            raise ValueError("Unknown arc type (%r %s %r)" % (u, type_e_uv, v))
#OBSOLETE|        map_vertex_fields[u] |= e_uv["info"]        
#OBSOLETE|
#OBSOLETE|    print "map_vertex_fields = %r" % map_vertex_fields
#OBSOLETE|    return map_vertex_fields
#OBSOLETE|
#OBSOLETE|
#OBSOLETE|class prune_color:
#OBSOLETE|    WHITE = 0 # this is currently not a leave
#OBSOLETE|    GRAY  = 1 # this is a leave that might be removed
#OBSOLETE|    BLACK = 2 # this leave can't be removed
#OBSOLETE|
#OBSOLETE|@accepts(DiGraph, dict, dict)
#OBSOLETE|@returns(set)
#OBSOLETE|def get_prunable_vertices(g, map_vertex_pred, map_vertex_fields):
#OBSOLETE|    """
#OBSOLETE|    \brief (Internal usage)
#OBSOLETE|        Compute which tables are useless in a 3-nf tree.
#OBSOLETE|        A "v" table is relevant iif it provides fields that are
#OBSOLETE|        not in its incident key (u->v), where "u" is its predecessor
#OBSOLETE|        in the 3-nf tree we are considering
#OBSOLETE|    \param g The 3-nf graph
#OBSOLETE|    \param map_vertex_pred The predecessor map related to the tree we are pruning
#OBSOLETE|    \param map_vertex_fields The dictionnary which maps for each table which
#OBSOLETE|        fields seems to be relevant.
#OBSOLETE|    \return The set of nodes we can safely remove from the tree
#OBSOLETE|    """
#OBSOLETE|    vertices_to_prune = set()
#OBSOLETE|
#OBSOLETE|    # Initialize color map and count the number of gray vertices
#OBSOLETE|    map_vertex_color = {}
#OBSOLETE|    num_gray_vertices = 0
#OBSOLETE|    for u in map_vertex_fields.keys():
#OBSOLETE|        map_vertex_color[u] = prune_color.WHITE
#OBSOLETE|    for v, u in map_vertex_pred.items():
#OBSOLETE|        if u:
#OBSOLETE|            map_vertex_color[v] = prune_color.GRAY
#OBSOLETE|            num_gray_vertices += 1
#OBSOLETE|
#OBSOLETE|    # Among the gray nodes, can we prune some tables?
#OBSOLETE|    # Repeat this until we can't prune no more table.
#OBSOLETE|    while num_gray_vertices > 0:
#OBSOLETE|        for v in map_vertex_fields.keys():
#OBSOLETE|            if map_vertex_color[v] == prune_color.GRAY:
#OBSOLETE|                fields_v = map_vertex_fields[v]
#OBSOLETE|                keys_v = v.get_keys()
#OBSOLETE|
#OBSOLETE|                # v can be safely pruned if it has a predecessor
#OBSOLETE|                # and if its queried fields are those used 
#OBSOLETE|                # to join u and v in this tree 
#OBSOLETE|                u = map_vertex_pred[v]
#OBSOLETE|                for key_v in keys_v:
#OBSOLETE|                    if set(fields_v) == set(key_v):
#OBSOLETE|                        print "get_prunable_vertices(): > %r can be safely pruned" % v
#OBSOLETE|                        vertices_to_prune.add(v)
#OBSOLETE|
#OBSOLETE|                        # We have to reconsider u (the predecessor of v)
#OBSOLETE|                        if u:
#OBSOLETE|                            map_vertex_color[u] = prune_color.GRAY
#OBSOLETE|                            num_gray_vertices += 1
#OBSOLETE|                            # TODO: remove from u the fields that were fk to v
#OBSOLETE|                            # and no more required
#OBSOLETE|
#OBSOLETE|                # v is now clean
#OBSOLETE|                map_vertex_color[v] = prune_color.BLACK
#OBSOLETE|                num_gray_vertices -= 1
#OBSOLETE|
#OBSOLETE|    return vertices_to_prune 

def prune_vertex_pred(g, needed_fields, map_vertex_pred):
    map_pred   = {}
    map_fields = {}

    for v, u in map_vertex_pred.items():
        if not u: # root
            map_pred[v] = None
            continue
        # If u is marked or has fields of interest
        v_fields = set(v.fields.keys())
        v_provided_fields = needed_fields & v_fields
        join_fields = g.edge[u][v]['info']
        v_provided_fields_nokey = v_provided_fields - join_fields
        if v_provided_fields_nokey:
            if not v in map_fields:
                map_fields[v] = set()
            map_fields[v] |= v_provided_fields
            while v: # u->v, the root has a NULL predecessor
                if v in map_pred:
                    break
                u = map_vertex_pred[v]
                join_fields = g.edge[u][v]['info']
                print "JOIN FIELDS %r -> %r" % (u,v), g.edge[u][v]['info']
                if not u in map_fields:
                    map_fields[u] = set()
                map_fields[u] |= join_fields
                map_fields[v] |= join_fields
                map_pred[v] = u
                v = u
        # else: we will find the fields when looking at u
    return map_pred, map_fields
    
@accepts(DiGraph, set, dict)
@returns(tuple)
def get_sub_graph(g, vertices_to_keep, map_vertex_fields):
    """
    \brief Extract a subgraph from a given graph g. Each vertex and
        arc of this subgraph is a deepcopy of those of g.
    \param g The original graph
    \param vertices_to_keep The vertices to keep in g.
    \param map_vertex_fields Store for for each vertex which fields
        are relevant
    \return A tuple made of
        a DiGraph instance (the subgraph)
        a dict 
    """
    print "Entering get_sub_graph() -----------------"
    sub_graph = DiGraph()
    map_vertex = {}
    map_vertex_fields_ret = {}

    # Copy relevant vertices from g
    for u in vertices_to_keep: 
        u_copy = deepcopy(u)
        map_vertex[u] = u_copy
        sub_graph.add_node(u_copy) # no data on nodes

    # Copy relevant arcs from g
    for u, v in g.edges():
        try:
            u_copy, v_copy = map_vertex[u], map_vertex[v]
        except:
            continue
        sub_graph.add_edge(u_copy, v_copy, deepcopy(g.edge[u][v]))

    for u, fields in map_vertex_fields.items():
        u_copy = map_vertex[u]
        map_vertex_fields_ret[u_copy] = fields

    print "map_vertex_fields:"
    for k, d in map_vertex_fields_ret.items():
        print "\t%r => %r" % (k, d)

    print "get_sub_graph() done ----------"
    return (sub_graph, map_vertex_fields_ret)

def prune_tree_fields(tree, needed_fields, map_vertex_fields):
    print "needed_fields = %r" % needed_fields
    print "tree =\n\t%s" % ("\n\t".join(["%r" % u for u in tree.nodes()]))
    missing_fields = deepcopy(needed_fields)
    for u in tree.nodes():
        print "u = %r in map_vertex_fields.keys() = %r" % (u, map_vertex_fields.keys())
        relevant_fields_u = map_vertex_fields[u]
        missing_fields -= relevant_fields_u
        for field in u.get_fields():
            if field.get_name() not in relevant_fields_u:
                u.erase_field(field.get_name())
    return missing_fields

@accepts(DiGraph, set, dict)
@returns(DiGraph)
def build_pruned_tree(g, needed_fields, map_vertex_pred):
    """
    \brief Compute the pruned 3-nf tree included in a 3nf-graph g according
        to a predecessors map modeling a 3-nf tree and a set of need fields.
    \param g The 3-nf graph
    \param needed_fields The fields queried by the user
    \param map_vertex_pred The predecessor map related to the tree we are pruning
        \sa tophat/util/dfs.py
    \return An instance of networkx.DiGraph representing the pruned 3-nf tree 
        Data related to this graph are copied from g, so it can be safely modified
        without impacting g. Such graph is typically embedded in a DBGraph instance.
        \sa tophat/util/dbgraph.py
    """
    # DEBUG
    print "Resulting Tree (predecessor map)"
    for k, d in map_vertex_pred.items():
        print "\t%r => %r" % (k,d)
    # DEBUG

    # We will select nodes of interest in map_vertex_pred before building a copy
    # of the tree rooted at the fact_table
    map_vertex_pred, map_vertex_fields = prune_vertex_pred(g, needed_fields, map_vertex_pred)
#OBSOLETE|    map_vertex_fields = compute_required_fields(g, needed_fields, map_vertex_pred)
#OBSOLETE|    vertices_to_keep  = set(map_vertex_fields.keys())
#OBSOLETE|    if vertices_to_keep == set():
#OBSOLETE|        raise Exception("Invalid query, no table kept, check predecessor map: %r" % map_vertex_pred)
#OBSOLETE|    vertices_to_keep -= get_prunable_vertices(g, map_vertex_pred, map_vertex_fields)
#OBSOLETE|    if vertices_to_keep == set():
#OBSOLETE|        raise Exception("Invalid query, every table pruned")
    vertices_to_keep = set(map_vertex_pred.keys())

    print "vertices_to_keep = %r" % vertices_to_keep
    print "map_vertex_fields ="
    for k, d in map_vertex_fields.items():
        print "\t%r => %r" % (k,d)
        
    tree, map_vertex_fields = get_sub_graph(g, vertices_to_keep, map_vertex_fields)

    missing_fields = prune_tree_fields(tree, needed_fields, map_vertex_fields)
    if missing_fields == set():
        print "build_pruned_graph(): each queried field has been successfully found"
    else: 
        print "build_pruned_graph(): the following queried fields have not been found: ", missing_fields

    #prune_tree_partitions(tree)

    print [n for n in tree.nodes()]

#    # Remove useless keys
#    for e in tree.edges():
#        (u, v) = e
#        (_, key_v) = tree[u][v]["info"]
#        if key_v:
#            #print "build_pruned_graph(): erasing key %s from %r" % (key_v, v)
#            v.erase_key(key_v)

    return tree 
