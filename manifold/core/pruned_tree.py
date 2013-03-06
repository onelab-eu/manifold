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

from copy                       import deepcopy
from networkx                   import DiGraph
from types                      import StringTypes
from manifold.util.type           import returns, accepts
from manifold.core.field          import Field
from manifold.core.key            import Key, Keys
from manifold.core.table          import Table 

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
#OBSOLETE|@accepts(DiGraph, set, dict)
#OBSOLETE|@returns(tuple)
#OBSOLETE|def get_sub_graph(g, vertices_to_keep, map_vertex_fields):
#OBSOLETE|    """
#OBSOLETE|    \brief Extract a subgraph from a given graph g. Each vertex and
#OBSOLETE|        arc of this subgraph is a deepcopy of those of g.
#OBSOLETE|    \param g The original graph
#OBSOLETE|    \param vertices_to_keep The vertices to keep in g.
#OBSOLETE|    \param map_vertex_fields Store for for each vertex which fields
#OBSOLETE|        are relevant
#OBSOLETE|    \return A tuple made of
#OBSOLETE|        a DiGraph instance (the subgraph)
#OBSOLETE|        a dict 
#OBSOLETE|    """
#OBSOLETE|    sub_graph = DiGraph()
#OBSOLETE|    map_vertex = {}
#OBSOLETE|    map_vertex_fields_ret = {}
#OBSOLETE|
#OBSOLETE|    # Copy relevant vertices from g
#OBSOLETE|    print "Keeping those tables:"
#OBSOLETE|    for u in vertices_to_keep: 
#OBSOLETE|        print "%r" % u
#OBSOLETE|        u_copy = deepcopy(u)
#OBSOLETE|        map_vertex[u] = u_copy
#OBSOLETE|        sub_graph.add_node(u_copy) # no data on nodes
#OBSOLETE|
#OBSOLETE|    # Copy relevant arcs from g
#OBSOLETE|    for u, v in g.edges():
#OBSOLETE|        try:
#OBSOLETE|            u_copy, v_copy = map_vertex[u], map_vertex[v]
#OBSOLETE|        except:
#OBSOLETE|            continue
#OBSOLETE|        sub_graph.add_edge(u_copy, v_copy, deepcopy(g.edge[u][v]))
#OBSOLETE|
#OBSOLETE|    for u, fields in map_vertex_fields.items():
#OBSOLETE|        u_copy = map_vertex[u]
#OBSOLETE|        map_vertex_fields_ret[u_copy] = fields
#OBSOLETE|
#OBSOLETE|    print "Leaving get_sub_graph: map_vertex_fields:"
#OBSOLETE|    for k, d in map_vertex_fields_ret.items():
#OBSOLETE|        print "\t%r => %r" % (k, d)
#OBSOLETE|
#OBSOLETE|    return (sub_graph, map_vertex_fields_ret)
#OBSOLETE|
#OBSOLETE|def prune_tree_fields(tree, needed_fields, map_vertex_fields):
#OBSOLETE|    print "needed_fields = %r" % needed_fields
#OBSOLETE|    print "tree =\n\t%s" % ("\n\t".join(["%r" % u for u in tree.nodes()]))
#OBSOLETE|    missing_fields = deepcopy(needed_fields)
#OBSOLETE|    for u in tree.nodes():
#OBSOLETE|        print "u = %r in map_vertex_fields.keys() = %r" % (u, map_vertex_fields.keys())
#OBSOLETE|        relevant_fields_u = map_vertex_fields[u]
#OBSOLETE|        missing_fields -= relevant_fields_u
#OBSOLETE|        for field in u.get_fields():
#OBSOLETE|            if field.get_name() not in relevant_fields_u:
#OBSOLETE|                u.erase_field(field.get_name())
#OBSOLETE|    return missing_fields

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
    \param map_vertex_pred A dictionnary {Talbe => Table} which maps a vertex and
        its predecessor in the tree we're considering
    \return A tuple made of
        - predecessors A dictionnary {Table => Table} included in map_vertex_pred
            containing only the relevant arcs
        - relevant_keys A dictionnary {Table => set(Key)} which indicates
            for each 3nf Table which are its relevant Keys
        - relevant_fields A dictionnary {Table => set(Field)} which indicates
            for each 3nf Table which are its relevant Fields
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

            # Fields use to JOIN the both tables are relevant
            # - In u: these Fields are stored in the (u --> v) arc
            # - In v: we assume the join in achieved with the first v's key 
            key_u = list(g.edge[u][v]["info"])[0] # select the first join (arbitrary) (Key or set(Field) instances depending on the arc label) 
            key_v = list(v.get_keys())[0]         # select the first key (arbitrary)

            if isinstance(key_u, Key):
                update_map(relevant_keys, u, key_u)
            update_map(relevant_keys, v, key_v)

            if isinstance(key_u, Key):
                fields_u = set(key_u)
            elif isinstance(key_u, Field):
                fields_u = set()
                fields_u.add(key_u)
            else:
                raise TypeError("Unexpected info on arc (%r, %r): %r" % (u, v, key_u))

            update_map(relevant_fields, u, fields_u) 
            update_map(relevant_fields, v, key_v) 

            # The field explicitely queried by the user have already
            # been stored in relevant_fields during a previous iteration
            if u in predecessor.keys(): 
                break

            # Update infos about u 
            relevant_fields[u] |= u.get_fields_with_name(queried_fields)
            predecessor[u] = map_vertex_pred[u]

            # Move to the previous arc
            v = u
            u = predecessor[u]

    return (predecessor, relevant_keys, relevant_fields)


@returns(DiGraph)
@accepts(DiGraph, dict)
def make_sub_graph(g, relevant_fields):
    """
    \brief Create a reduced graph based on g.
        We only keep vertices having a key in relevant_fields
    \param g A DiGraph instance (the full 3nf graph)
    \param relevant_fields A dictionnary {Table: Fields}
        indicating for each Table which Field(s) are relevant.
    \return The corresponding sub-3nf-graph
    """
    sub_graph = DiGraph()
    copy = dict()
    vertices_to_keep = set(relevant_fields.keys())

    # Copy relevant vertices from g
    for u in vertices_to_keep: 
        copy_u = Table.make_table_from_fields(u, relevant_fields[u])
        copy[u] = copy_u
        print "\nAdding %s" % copy_u
        sub_graph.add_node(copy_u) # no data on nodes

    # Copy relevant arcs from g
    for u, v in g.edges():
        try:
            copy_u, copy_v = copy[u], copy[v]
        except:
            continue

        sub_graph.add_edge(copy_u, copy_v, deepcopy(g.edge[u][v]))
        print "Adding %r %s %r via %r" % (copy_u, g.edge[u][v]["type"], copy_v, g.edge[u][v]["info"])

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
        \sa manifold.util.dfs.py
    \return 
        An instance of networkx.DiGraph representing the pruned 3-nf tree 
        Data related to this graph are copied from g, so it can be safely modified
        without impacting g. Such graph is typically embedded in a DBGraph instance.
        \sa manifold.core.dbgraph.py
    """
   
    print "-" * 100
    print "Prune useless keys/nodes/arcs from tree"
    print "-" * 100
    
    (_, relevant_keys, relevant_fields) = prune_precedessor_map(g, needed_fields, map_vertex_pred)
    tree = make_sub_graph(g, relevant_fields)

    # Print tree
    print "-" * 100
    print "Minimal tree:"
    print "-" * 100
    for table in tree.nodes():
        print "%s\n" % table

    return tree
