#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Provides the function build_pruned_tree() which extract from a 3-nf graph
# - a "tree" (more precisely a predecessor map, typically computed thanks to a DFS) 
# - a set of needed fields (those queried by the user)
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr> 

from networkx                   import DiGraph
from tophat.util.dfs            import dfs_color
from tophat.util.type           import returns, accepts
from tophat.core.table          import Table
from copy                       import deepcopy
from types                      import StringTypes

@accepts(DiGraph)
def check_graph_consistency(g):
    """
    \brief (Debug only) Check whether a graph is compatible with the functions
        provided in this file and raise an exception in case of problem
    \param g The 3-nf graph we're checking
    """
    for e in g.edges():
        (u,v) = e
        e_uv = g.edge[u][v]
        e_type = e_uv["type"]
        e_info = e_uv["info"]

        # type
        if e_type not in ["-->", "==>", "~~>"]:
            raise ValueError("(%r %s %r) invalid type %r" % (u, e_type, v, e_type))
        if e_type == "==>":
            continue

        # info must be filled for ~~> and -->
        if not isinstance(e_info, set):
            raise ValueError("(%r %s %r) set expected: %r" % (u, e_type, v, e_info))

@accepts(DiGraph, set, dict)
@returns(dict)
def compute_required_fields(g, needed_fields, map_vertex_pred):
    """
    \brief (Internal usage)
        Compute for each table of "g" which fields are relevant
        in the query plane
    \param g The 3-nf tree
    \param needed_fields The fields queried by the user
    \param map_vertex_pred The predecessor map computed according to dfs()
        \sa tophat/util/dfs.py
    \return A dictionnary which maps for each table the set of required fields
    """
    map_vertex_fields = {}
    check_graph_consistency(g)

    # For each edge (u->v) referenced in map_vertex_pred retrieve
    # - the fields we need in u
    # - the fields we need in v (not provided by u)
    # - the keys   we need in v
    # Since g is 3-nf, each field (not in a key) is crossed only once (or never if unreachable)
    # Fields involved in a key connecting (u->v) are retrieved in u
    for v, u in map_vertex_pred.items():
        # Skip this element, (u->v) is not in the tree
        if not u:
            continue

        # Allocate map_vertex_fields[*] if needed
        if u not in map_vertex_fields:
            map_vertex_fields[u] = set()
        if v not in map_vertex_fields:
            map_vertex_fields[v] = set()

        # Dispatch remaining needed fields to u
        fields_u = u.get_field_names() & needed_fields
        map_vertex_fields[u] |= fields_u
        needed_fields -= fields_u

        # Dispatch remaining needed fields to v
        fields_v = v.get_field_names() & needed_fields
        map_vertex_fields[v] |= fields_v
        needed_fields -= fields_v

        # We may need further fields to traverse (u, v) (e.g. to join u and v
        # where u stands for the left table and v for the right one).
        e_uv = g.edge[u][v]
        type_e_uv = e_uv["type"]
        if type_e_uv in ["-->", "~~>"]:
            (fields_u, fields_v) = (e_uv["info"], None)
            if fields_u:
                map_vertex_fields[u] |= set(fields_u)
            else:
                raise ValueError("Inconsistent arc (%r %s %r)" % (u, type_e_uv, v))
            if fields_v:
                map_vertex_fields[v] |= set(fields_v)
        elif type_e_uv == "==>":
            pass
        else:
            raise ValueError("Unknown arc type (%r %s %r)" % (u, type_e_uv, v))

    return map_vertex_fields

class prune_color:
    WHITE = 0 # this is currently not a leave
    GRAY  = 1 # this is a leave that might be removed
    BLACK = 2 # this leave can't be removed

@accepts(DiGraph, dict, dict)
@returns(set)
def get_prunable_vertices(g, map_vertex_pred, map_vertex_fields):
    """
    \brief (Internal usage)
        Compute which tables are useless in a 3-nf tree.
        A "v" table is relevant iif it provides fields that are
        not in its incident key (u->v), where "u" is its predecessor
        in the 3-nf tree we are considering
    \param g The 3-nf graph
    \param map_vertex_pred The predecessor map related to the tree we are pruning
    \param map_vertex_fields The dictionnary which maps for each table which
        fields seems to be relevant.
    \return The set of nodes we can safely remove from the tree
    """
    vertices_to_prune = set()

    # Initialize color map and count the number of gray vertices
    map_vertex_color = {}
    num_gray_vertices = 0
    for u in map_vertex_fields.keys():
        map_vertex_color[u] = prune_color.WHITE
    for v, u in map_vertex_pred.items():
        if u:
            map_vertex_color[v] = prune_color.GRAY
            num_gray_vertices += 1

    # Among the gray nodes, can we prune some tables?
    # Repeat this until we can't prune no more table.
    while num_gray_vertices > 0:
        for v in map_vertex_fields.keys():
            if map_vertex_color[v] == prune_color.GRAY:
                fields_v = map_vertex_fields[v]
                keys_v = v.get_fields_from_keys()

                # v can be safely pruned if it has a predecessor
                # and if its queried fields are those used 
                # to join u and v in this tree 
                u = map_vertex_pred[v]
                for key_v in keys_v:
                    if set(fields_v) == set(key_v):
                        print "get_prunable_vertices(): > %r can be safely pruned" % v
                        vertices_to_prune.add(v)

                        # We have to reconsider u (the predecessor of v)
                        if u:
                            map_vertex_color[u] = prune_color.GRAY
                            num_gray_vertices += 1
                            # TODO: remove from u the fields that were fk to v
                            # and no more required

                # v is now clean
                map_vertex_color[v] = prune_color.BLACK
                num_gray_vertices -= 1

    return vertices_to_prune 

@accepts(DiGraph, set)
@returns(DiGraph)
def get_sub_graph(g, vertices_to_keep):
    """
    \brief Extract a subgraph from a given graph g. Each vertex and
        arc of this subgraph is a deepcopy of those of g.
    \param g The original graph
    \param vertices_to_prune The vertices keep in g.
    \return The corresponding subgraph
    """
    sub_graph = DiGraph()

    # Copy relevant vertices from g
    # We do not yet clean vertices since we'll need "in" operator to build the relevant arcs 
    for u in vertices_to_keep: 
        sub_graph.add_node(u, deepcopy(g[u]))

    # Copy relevant arcs from g
    for e in g.edges():
        (u, v) = e
        if u in sub_graph.nodes() and v in sub_graph.nodes(): 
            sub_graph.add_edge(u, v, deepcopy(g.edge[u][v]))

    return sub_graph

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
    print "needed_fields = %r" % needed_fields
    print "map_vertex_pred = %r" % map_vertex_pred
    map_vertex_fields = compute_required_fields(g, needed_fields, map_vertex_pred)
    vertices_to_keep  = set(map_vertex_fields.keys())
    if vertices_to_keep == set():
        raise Exception("Invalid query, no table kept, check predecessor map: %r" % map_vertex_pred)
    vertices_to_keep -= get_prunable_vertices(g, map_vertex_pred, map_vertex_fields)
    if vertices_to_keep == set():
        raise Exception("Invalid query, every table pruned")
    tree = get_sub_graph(g, vertices_to_keep)

    # Remove useless fields
    missing_fields = deepcopy(needed_fields)
    for u in tree.nodes():
        relevant_fields_u = map_vertex_fields[u]
        missing_fields -= (needed_fields & relevant_fields_u)
        for field in u.get_fields():
            if field not in relevant_fields_u:
                u.erase_field(field.field_name)

#    # Remove useless keys
#    for e in tree.edges():
#        (u, v) = e
#        (_, key_v) = tree[u][v]["info"]
#        if key_v:
#            #print "build_pruned_graph(): erasing key %s from %r" % (key_v, v)
#            v.erase_key(key_v)

    if missing_fields == set():
        print "build_pruned_graph(): each queried field has been successfully found"
    else: 
        print "build_pruned_graph(): the following queried fields have not been found: ", missing_fields
    return tree 
