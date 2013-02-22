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
from copy                       import deepcopy
from types                      import StringTypes
from tophat.util.type           import returns, accepts
from tophat.core.field          import Field
from tophat.core.key            import Key, Keys

def print_map(dico):
    for k, d in dico.items():
        print "\t%r => %r" % (k,d)

#OBSOLETE|def prune_precedessor_map_old(g, needed_fields, map_vertex_pred):
#OBSOLETE|    map_pred   = {}
#OBSOLETE|    map_fields = {}
#OBSOLETE|
#OBSOLETE|    for v, u in map_vertex_pred.items():
#OBSOLETE|        if not u: # root
#OBSOLETE|            map_pred[v] = None
#OBSOLETE|            continue
#OBSOLETE|        # If u is marked or has fields of interest
#OBSOLETE|        v_fields = set(v.fields.keys())
#OBSOLETE|        v_provided_fields = needed_fields & v_fields
#OBSOLETE|        join_fields = g.edge[u][v]['info']
#OBSOLETE|        v_provided_fields_nokey = v_provided_fields - join_fields
#OBSOLETE|        if v_provided_fields_nokey:
#OBSOLETE|            if not v in map_fields:
#OBSOLETE|                map_fields[v] = set()
#OBSOLETE|            map_fields[v] |= v_provided_fields
#OBSOLETE|            while v: # u->v, the root has a NULL predecessor
#OBSOLETE|                if v in map_pred:
#OBSOLETE|                    break
#OBSOLETE|                u = map_vertex_pred[v]
#OBSOLETE|                join_fields = g.edge[u][v]['info']
#OBSOLETE|                print "JOIN FIELDS %r -> %r" % (u,v), g.edge[u][v]['info']
#OBSOLETE|                if not u in map_fields:
#OBSOLETE|                    map_fields[u] = set()
#OBSOLETE|                map_fields[u] |= join_fields
#OBSOLETE|                map_fields[v] |= join_fields
#OBSOLETE|                map_pred[v] = u
#OBSOLETE|                v = u
#OBSOLETE|        # else: we will find the fields when looking at u
#OBSOLETE|    return map_pred, map_fields
#OBSOLETE|

@returns(tuple)
@accepts(DiGraph, set, dict)
def prune_precedessor_map(g, queried_fields, map_vertex_pred):
    """
    \brief Prune from a predecessor map (representing a tree)
       the entries that are not needed (~ remove from a tree
       useless nodes).
    \param g The graph on which is based the tree
    \param queried_fields The fields that are queried by the user
        A node/table u is useful if one or both of those condition is
        satisfied:
        - u provides a field queried by the user
        - u is involved in a join required to answer to the query
    \param map_vertex_pred A dictionnary which maps a vertex and
        its predecessor in the tree we're considering
    \return A tuple made of
        - predecessors A predecessor map included in map_vertex_pred
            containing only the relevant arcs
        - relevant_keys A dictionnary which map for each vertex
            it(s) relevant key(s)
        - relevant_fields
    """
    def update_map(m, k, s):
        if k not in m.keys():
            m[k] = set()
        m[k] |= s

    # Vertices in predecessors have been already examined in a previous iteration
    predecessor     = dict()
    relevant_keys   = dict()
    relevant_fields = dict()

    for v, u in map_vertex_pred.items():
        queried_fields_v = v.get_fields_with_name(queried_fields) 

        # We store information about v iif it is the root node
        # or if provides relevant fields
        if not (u == None or queried_fields_v != set()):
            continue

        update_map(relevant_fields, v, queried_fields_v)
        predecessor[v] = u 

        # Backtrack to the root or to an already visited node
        while u: # Current arc is (u --> v)
            key_u = list(g.edge[u][v]["info"])[0] # select the first key (arbitrary!)
            if isinstance(key_u, Key):
                update_map(relevant_keys, u, key_u)

            fields_u = set(key_u)
            update_map(relevant_fields, u, fields_u) 

            # The field explicitely queried by the user have already
            # been stored in relevant_fields during a previous iteration
            if u in predecessor:
                break

            # Update infos about u 
            relevant_fields[u] |= u.get_fields_with_name(queried_fields)
            predecessor[u] = map_vertex_pred[u]

            # Move to the previous arc
            v = u
            u = predecessor[u]

    return (predecessor, relevant_keys, relevant_fields)
# 
#@accepts(DiGraph, set, dict)
#@returns(tuple)
#def get_sub_graph(g, vertices_to_keep, map_vertex_fields):
#    """
#    \brief Extract a subgraph from a given graph g. Each vertex and
#        arc of this subgraph is a deepcopy of those of g.
#    \param g The original graph
#    \param vertices_to_keep The vertices to keep in g.
#    \param map_vertex_fields Store for for each vertex which fields
#        are relevant
#    \return A tuple made of
#        a DiGraph instance (the subgraph)
#        a dict 
#    """
#    sub_graph = DiGraph()
#    map_vertex = {}
#    map_vertex_fields_ret = {}
#
#    # Copy relevant vertices from g
#    print "Keeping those tables:"
#    for u in vertices_to_keep: 
#        print "%r" % u
#        u_copy = deepcopy(u)
#        map_vertex[u] = u_copy
#        sub_graph.add_node(u_copy) # no data on nodes
#
#    # Copy relevant arcs from g
#    for u, v in g.edges():
#        try:
#            u_copy, v_copy = map_vertex[u], map_vertex[v]
#        except:
#            continue
#        sub_graph.add_edge(u_copy, v_copy, deepcopy(g.edge[u][v]))
#
#    for u, fields in map_vertex_fields.items():
#        u_copy = map_vertex[u]
#        map_vertex_fields_ret[u_copy] = fields
#
#    print "Leaving get_sub_graph: map_vertex_fields:"
#    for k, d in map_vertex_fields_ret.items():
#        print "\t%r => %r" % (k, d)
#
#    return (sub_graph, map_vertex_fields_ret)
#
#def prune_tree_fields(tree, needed_fields, map_vertex_fields):
#    print "needed_fields = %r" % needed_fields
#    print "tree =\n\t%s" % ("\n\t".join(["%r" % u for u in tree.nodes()]))
#    missing_fields = deepcopy(needed_fields)
#    for u in tree.nodes():
#        print "u = %r in map_vertex_fields.keys() = %r" % (u, map_vertex_fields.keys())
#        relevant_fields_u = map_vertex_fields[u]
#        missing_fields -= relevant_fields_u
#        for field in u.get_fields():
#            if field.get_name() not in relevant_fields_u:
#                u.erase_field(field.get_name())
#    return missing_fields


def make_sub_graph(g, relevant_keys, relevant_fields):
    """
    \brief Create a reduced graph based on g.
        We only keep vertices having a key in relevant_fields
    """
    sub_graph = DiGraph()
    copy = dict()

    vertices_to_keep = set(relevant_fields.keys())

    # Copy relevant vertices from g
    for u in vertices_to_keep: 
        print "Adding %r" % u
        copy_u = deepcopy(u)
        copy[u] = copy_u

        for field in u.get_fields():
            if field not in relevant_fields[u]:
                copy_u.erase_field(field.get_name())

        # Select an arbitrary key if no key is needed to ensure that the resulting table
        # has at least one key
        relevant_keys_u = relevant_keys[u] if u in relevant_keys.keys() else set(list(u.get_keys())[0])

#TODO: in Table: make_sub_table: the key of the subtable must refer to fields of the subtable
#TODO|        print "> relevant keys are %s" % ', '.join(["%s" % k for k in relevant_keys_u])
#TODO|        for key in u.get_keys():
#TODO|            if key not in relevant_keys_u:
#TODO|                print ">> erasing key %s" % key 
#TODO|                copy_u.erase_key(key)

        print "%s" % copy_u
        sub_graph.add_node(copy_u) # no data on nodes

    # Copy relevant arcs from g
    for u, v in g.edges():
        try:
            copy_u, copy_v = copy[u], copy[v]
        except:
            continue

        print "Adding %r --> %r" % (copy_u, copy_v)
        sub_graph.add_edge(copy_u, copy_v, deepcopy(g.edge[u][v]))

    return sub_graph


@accepts(DiGraph, set, dict)
@returns(DiGraph)
def build_pruned_tree(g, needed_fields, map_vertex_pred):
    """
    \brief Compute the pruned 3-nf tree included in a 3nf-graph g according
        to a predecessors map modeling a 3-nf tree and a set of need fields.
    \param g The 3-nf graph
    \param needed_fields A set of Field instances, queried by the user
    \param map_vertex_pred The predecessor map related to the tree we are pruning
        \sa tophat/util/dfs.py
    \return An instance of networkx.DiGraph representing the pruned 3-nf tree 
        Data related to this graph are copied from g, so it can be safely modified
        without impacting g. Such graph is typically embedded in a DBGraph instance.
        \sa tophat/util/dbgraph.py
    """
   
    # We will select nodes of interest in map_vertex_pred before building a copy
    # of the tree rooted at the fact_table

    print "-" * 100
    print "Prune useless nodes/arcs from tree"
    print "-" * 100
    
#    print "Before pruning, predecessor map is:"
#    print_map(map_vertex_pred)

    (predecessor, relevant_keys, relevant_fields) = prune_precedessor_map(g, needed_fields, map_vertex_pred)
#    print "After pruning, predecessor map is:"
#    print_map(predecessor)
#    print "Relevant keys:"
#    print_map(relevant_keys)
#    print "Relevant fields:"
#    print_map(relevant_fields)

    tree = make_sub_graph(g, relevant_keys, relevant_fields)

    # Print tree
    print "-" * 100
    print "Minimal tree:"
    print "-" * 100
    for table in tree.nodes():
        print "%s\n" % table

#    # Remove useless keys
#    for e in tree.edges():
#        (u, v) = e
#        (_, key_v) = tree[u][v]["info"]
#        if key_v:
#            #print "build_pruned_graph(): erasing key %s from %r" % (key_v, v)
#            v.erase_key(key_v)

    return tree 
