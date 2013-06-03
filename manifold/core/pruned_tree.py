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
from manifold.core.field        import Field
from manifold.core.key          import Key, Keys
from manifold.core.table        import Table 
from manifold.core.dbgraph      import DBGraph, Relation
from manifold.util.log          import Log
from manifold.util.type         import returns, accepts

@returns(tuple)
@accepts(DBGraph, set, dict)
def prune_precedessor_map(metadata, queried_fields, map_vertex_pred):
    """
    \brief Prune from a predecessor map (representing a tree)
       the entries that are not needed (~ remove from a tree
       useless nodes).
    \param metadata DBGraph instance
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
            for each 3nf Table which are its relevant Keys NOT USED ANYMORE
        - relevant_fields A dictionnary {Table => set(Field)} which indicates
            for each 3nf Table which are its relevant Fields
    """
    # NOTE: The pruning step could be avoided if we integrated all these conditions into the DFS procedure

    g = metadata.graph

    # Helper function to manage a dictionary of sets
    def update_map(m, k, s):
        if k not in m.keys():
            m[k] = set()
        m[k] |= s

    # Vertices in predecessors have been already examined in a previous iteration
    predecessor     = dict()
    # A map that associates each table with the set of fields that it uniquely provides
    relevant_fields = dict()

    missing_fields = queried_fields
    # XXX In debug comments, we need to explain for each table, why it has been
    # kept or discarded succintly

    # Loop in arbitrary order through the 3nf tables
    for v, u in map_vertex_pred.items():
        Log.debug("Considering %r -> [[ %r ]]" % (u,v))
        # For each table, we determine the set of fields it provides that are
        # necessary to answer the query
        queried_fields_v = v.get_fields_with_name(queried_fields, metadata) 
        # and those that are not present in the parent (foreign keys)
        queried_fields_u = u.get_fields_with_name(queried_fields, metadata) if u else set()
        queried_fields_v_unique = queried_fields_v - queried_fields_u

        # ??? missing_fields -= queried_fields_v

        # If v is not the root or does not provide relevant fields (= not found
        # in the parent), then we prune it by not including it in the
        # predecessor map we return. (We do not need a table if all fields can
        # be found in the parent.)
        if u and not queried_fields_v_unique:
            Log.debug("    [X] No interesting field")
            continue

        # Let's now consider all pairs of table (u -> v) up to the root,
        # focusing on table v
        # 
        # All tables back to the root are necessary at least to be able to
        # retrieve v through successive joins (and we will thus need the keys
        # of intermediate tables).
        while True:

            # v has already been considered
            if v in predecessor.keys():
                Log.debug("    [X] Already processed %r" % v)
                break

            # TABLE
            #
            # Don't discard table v by adding it to the predecessor map
            predecessor[v] = u

            # FIELDS
            #
            # Relevants fields for table v are those contributing to the query
            # Including fields that might be in the key is not important, since
            # they are all added later on.
            #
            # eg. queried_fields has slice_hrn, but resource has slice
            # relevant fields, hence queried_field_v should have slice
            # XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
            print "queried_fields==", queried_fields
            queried_fields_v = v.get_fields_with_name(queried_fields, metadata)
            missing_fields -= set(map(lambda x:x.get_name(), queried_fields_v))
            print "we got", queried_fields_v
            print "missing_fields becomes", missing_fields
            
            # resolve
            queried_fields_v = set(map(lambda x:x.get_name(), queried_fields_v))
            

            update_map(relevant_fields, v, queried_fields_v)

            # KEYS
            #
            # Key fields are necessary to perform JOIN in at least one table (otherwise we would not have distinct 3nf tables)
            if u: # thus, we are not considering the root (no need for keys)
                # for u, select the first join (arbitrary) (Key or set(Field) instances depending on the arc label) 
                key_u = metadata.get_relation(u,v).get_predicate().get_key()
                if isinstance(key_u, StringTypes):
                    key_u = [key_u]
                key_u = set(key_u)
                # for v, arbitrarily choose the first key assuming it is used for the join
                key_v = v.get_keys().one().get_names()

                # Adding keys...
                update_map(relevant_fields, u, key_u)
                update_map(relevant_fields, v, key_v)

                # Queries fields do not necessarily include fields from the key, so add
                # them all the time, otherwise they will get pruned
                update_map(relevant_fields, v, key_v) 

            Log.debug("    [V] Table %r, relevant_fields=%r" % (v, relevant_fields.get(v, None) ))

            # Stopping conditions:
            if not u:
                # u = None : u is the root, no need to continue
                Log.debug("<<< reached root")
                break

            # Move to the previous arc u' -> v'=u -> v
            v = u
            u = map_vertex_pred[u]


    return (predecessor, relevant_fields, missing_fields)


@returns(DiGraph)
@accepts(DBGraph, dict)
def make_sub_graph(metadata, relevant_fields):
    """
    \brief Create a reduced graph based on g.
        We only keep vertices having a key in relevant_fields
    \param g A DiGraph instance (the full 3nf graph)
    \param relevant_fields A dictionnary {Table: Fields}
        indicating for each Table which Field(s) are relevant.
    \return The corresponding sub-3nf-graph
    """
    g = metadata.graph
    sub_graph = DiGraph()
    copy = dict()
    vertices_to_keep = set(relevant_fields.keys())

    # Copy relevant vertices from g
    for u in vertices_to_keep: 
        copy_u = Table.make_table_from_fields(u, relevant_fields[u])
        copy[u] = copy_u
        sub_graph.add_node(copy_u) # no data on nodes

    # Copy relevant arcs from g
    for u, v in g.edges():
        try:
            copy_u, copy_v = copy[u], copy[v]
        except:
            continue

        sub_graph.add_edge(copy_u, copy_v, deepcopy(g.edge[u][v]))
        Log.debug("Adding copy of : %s" % metadata.print_arc(u, v))

    return sub_graph


@accepts(DBGraph, set, dict)
@returns(DiGraph)
def build_pruned_tree(metadata, needed_fields, map_vertex_pred):
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
   
    Log.debug("-" * 100)
    Log.debug("Prune useless keys/nodes/arcs from tree")
    Log.debug("-" * 100)
    
    g = metadata.graph

    (_, relevant_fields, missing_fields) = prune_precedessor_map(metadata, needed_fields, map_vertex_pred)
    # XXX we don't use predecessor graph for building subgraph, a sign we can simplify here
    tree = make_sub_graph(metadata, relevant_fields)

    # Print tree
    Log.debug("-" * 100)
    Log.debug("Minimal tree:")
    Log.debug("-" * 100)
    for table in tree.nodes():
        Log.debug("%s\n" % table)
    Log.debug("-" * 100)

    return (tree, missing_fields)
