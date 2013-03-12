#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convert a 3nf-tree into an AST (e.g. a query plan)
# \sa manifold.core.pruned_tree.py
# \sa tophat/core/ast.py
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# NOTE: The fastest way to traverse all edges of a graph is via
# adjacency_iter(), but the edges() method is often more convenient.

from networkx                                         import DiGraph
from networkx.algorithms.traversal.depth_first_search import dfs_preorder_nodes

from tophat.core.ast            import AST, From, Union, LeftJoin
from tophat.core.table          import Table 
from manifold.core.query          import Query 
from manifold.util.type           import returns, accepts
from manifold.core.dbgraph        import find_root
from manifold.models.user         import User

#OBSOLETE|@returns(From)
#OBSOLETE|@accepts(dict, Table, str)
#OBSOLETE|def get_from(froms, table, platform):
#OBSOLETE|    """
#OBSOLETE|    \brief Retrieve (and create if not found) a From node
#OBSOLETE|        thanks to a dictionnary
#OBSOLETE|    \param froms A dictionnary {String : From} mapping for
#OBSOLETE|        a given table
#OBSOLETE|    \param table A Table instance
#OBSOLETE|    \param platform A String value (the name of the platform
#OBSOLETE|        providing the table)
#OBSOLETE|    \return The corresponding From node
#OBSOLETE|    """
#OBSOLETE|    table_name = table.get_name()
#OBSOLETE|    if table_name not in froms:
#OBSOLETE|        froms[table_name] = {}
#OBSOLETE|    if platform not in froms[table_name]:
#OBSOLETE|        froms[table_name][platform] =  From(query, table_name, table.get_keys())
#OBSOLETE|    return froms[table_name][platform]
#OBSOLETE|
#OBSOLETE|def get_qp_rec(ast, tree, u, froms):
#OBSOLETE|    """
#OBSOLETE|    \brief (Internal use)
#OBSOLETE|    \param ast The AST we are building 
#OBSOLETE|    \param tree A DiGraph instance representing the 3nf-tree
#OBSOLETE|    \param u The Table of the 3-nf tree we're processing
#OBSOLETE|    \params froms A dictionnary which stores the From nodes
#OBSOLETE|        already inserted.
#OBSOLETE|        froms[table_name][platforms]
#OBSOLETE|    """
#OBSOLETE|
#OBSOLETE|    # XXX Not sure we need the _froms_ parameter
#OBSOLETE|    u_partitions = u.get_partitions()
#OBSOLETE|    u_platforms = u_partitions.keys()
#OBSOLETE|
#OBSOLETE|    # We should have a pass that reconstitute platforms along with some
#OBSOLETE|    # attributes: overlap/disjoint - maximum/full
#OBSOLETE|    #
#OBSOLETE|    # I propose that we have two properties:
#OBSOLETE|    # platforms = name + clause = partition criterion
#OBSOLETE|    # partitions = set of (set of platforms realizing a partition + attributes)
#OBSOLETE|    #
#OBSOLETE|    # After pruning, 
#OBSOLETE|    # - partitions of the full space will be replaced by partitions of the
#OBSOLETE|    # needed space, and useless platforms will have been discarded
#OBSOLETE|    # - only one partition will remain (most exact/complete, then
#OBSOLETE|    # less costly. eventually involving the least number of platforms in the
#OBSOLETE|    # cost).
#OBSOLETE|
#OBSOLETE|#    query = Query(fact_table = u.name)
#OBSOLETE|#    print "u = %s" % u
#OBSOLETE|#
#OBSOLETE|#    # Map a right table to its left tables
#OBSOLETE|#    map_joins = dict() 
#OBSOLETE|#    set_unions = set()
#OBSOLETE|#
#OBSOLETE|#    # For each sv successor of u
#OBSOLETE|#    for e_uv in tree.out_edges(u):
#OBSOLETE|#        (_, v) = e_uv
#OBSOLETE|#        e_uv_type = tree[u][v]["type"]
#OBSOLETE|#        u_childs = []
#OBSOLETE|#
#OBSOLETE|#        if e_uv_type in ["-->", "~~>"]:
#OBSOLETE|#            # Check consistency
#OBSOLETE|#            if u.get_platforms() != v.get_platforms():
#OBSOLETE|#                raise ValueError("Inconsistant platforms: (%s %r %s): %s %s", (u, e_uv_type, v, u.get_platforms(), v.get_platforms())
#OBSOLETE|#
#OBSOLETE|#            # For each up in u, for each vp in v, memorize "up LEFT JOIN vp"
#OBSOLETE|#            plaforms_uv = u.get_platforms()
#OBSOLETE|#            for p in plaforms_uv :
#OBSOLETE|#                up = get_from(froms, u, p) 
#OBSOLETE|#                vp = get_from(froms, v, p)
#OBSOLETE|#                if up no in map_joins:
#OBSOLETE|#                    map_joins = set()
#OBSOLETE|#                map_joins[up].add(vp)
#OBSOLETE|#
#OBSOLETE|#            # Memorize that we've to build a "UNION of each up"
#OBSOLETE|#            set_union.add(u.name)
#OBSOLETE|#
#OBSOLETE|#        elif e_uv_type == "==>":
#OBSOLETE|#            # Check consistency
#OBSOLETE|#            if u.get_platforms() <= v.get_platforms():
#OBSOLETE|#                raise ValueError("Inconsistant platforms: (%s %r %s): %s %s", (u, e_uv_type, v, u.get_platforms(), v.get_platforms())
#OBSOLETE|#
#OBSOLETE|#            # platforms in u provide more information than those in v
#OBSOLETE|#            # If u in the prune_tree, it means that we require some fields
#OBSOLETE|#            # only in u, so only the platforms related to u can help.
#OBSOLETE|#            
#OBSOLETE|#        else:
#OBSOLETE|#            raise ValueError("Invalid arc type: (%s %r %s)" % (u, e_uv_type, v))
#OBSOLETE|#                 
#OBSOLETE|#            
#OBSOLETE|#
#OBSOLETE|#        print "(u,v) = (%r %s %r)" % (u, tree[u][v]["type"], v)
#OBSOLETE|        
#OBSOLETE|
#OBSOLETE|@accepts(User, DiGraph)
#OBSOLETE|@returns(AST)
#OBSOLETE|def build_query_plane(user, pruned_tree):
#OBSOLETE|    """
#OBSOLETE|    \brief Compute a query plane according to a pruned tree
#OBSOLETE|    \param user The User instance representing the user issuing the query
#OBSOLETE|        \sa tophat/model/user.py
#OBSOLETE|    \param pruned_tree A DiGraph instance representing the 3nf-tree
#OBSOLETE|        such as each remaining key in and each remaining field
#OBSOLETE|        (stored in the DiGraph nodes) is needed 
#OBSOLETE|        - either because it is explicitly queried by the user or either because
#OBSOLETE|        - either because it is needed to join tables involved in the 3nf-tree)
#OBSOLETE|    \return an AST instance which describes the resulting query plane
#OBSOLETE|    """
#OBSOLETE|    ast = None # AST(user = user)
#OBSOLETE|    root_table = find_root(pruned_tree)
#OBSOLETE|
#OBSOLETE|    # The AST contains two types of information: which joins must be done before
#OBSOLETE|    # others, which fields are required as join keys
#OBSOLETE|    # Join order can be impacted by several metrics, such as pi and sigmas
#OBSOLETE|    # We are not considering such optimizations at the moment, and only extract
#OBSOLETE|    # one arbitrary legit order
#OBSOLETE|
#OBSOLETE|    from_cache = {}
#OBSOLETE|    # key = platform method
#OBSOLETE|
#OBSOLETE|    def get_joined_tables(table, platform): # pruned_tree
#OBSOLETE|        # adjacency dict keyed by neighbor to edge attributes
#OBSOLETE|        for succ, edge_data in pruned_tree[table]:
#OBSOLETE|            if not platform in succ.get_platforms():
#OBSOLETE|                continue
#OBSOLETE|            try:
#OBSOLETE|                succ.get_method(platform)
#OBSOLETE|            except:
#OBSOLETE|                continue
#OBSOLETE|            #edge_type = 
#OBSOLETE|            #is_joined = 
#OBSOLETE|            
#OBSOLETE|        
#OBSOLETE|
#OBSOLETE|    def get_from_ast(platform, table):
#OBSOLETE|        cache = []
#OBSOLETE|        if (platform, table) in cache:
#OBSOLETE|            # Return a FromList node cached
#OBSOLETE|            pass 
#OBSOLETE|        else:
#OBSOLETE|            # Determines the set of FromList operators that have to be created
#OBSOLETE|            # and add them to a list
#OBSOLETE|            pass#get_joined_tables
#OBSOLETE|
#OBSOLETE|            # Create a FROM NODE towards this platform
#OBSOLETE|
#OBSOLETE|
#OBSOLETE|    # We visit the nodes one by one
#OBSOLETE|    nodes = dfs_preorder_nodes(pruned_tree, root_table)
#OBSOLETE|    print "Nodes of the pruned tree:"
#OBSOLETE|    for node in nodes:
#OBSOLETE|        partitions = node.get_partitions()
#OBSOLETE|        subtree = AST(user=user)
#OBSOLETE|        if len(partitions) == 1:
#OBSOLETE|            print "no union"
#OBSOLETE|            # Retrieve the From operator associated to platform::method
#OBSOLETE|            #  - either as a FromList operator, for information provided
#OBSOLETE|            #  indirectly by another query
#OBSOLETE|            #  - or as a From operator, for information that needs to be
#OBSOLETE|            #  retrieved directly from platforms
#OBSOLETE|            platform = partitions.keys()[0]
#OBSOLETE|            from_ast = get_from_ast(platform, node.name)
#OBSOLETE|            subtree = from_ast 
#OBSOLETE|        else:
#OBSOLETE|            print "union"
#OBSOLETE|            # A node is composed of several partitions, which must all be
#OBSOLETE|            # UNION'ed: { platform : filter }
#OBSOLETE|            for platform, clause in partitions.items():
#OBSOLETE|                # NOTE clause is unused
#OBSOLETE|                from_ast = get_from_ast(platform, node.name)
#OBSOLETE|                subtree = ast.union(from_ast)
#OBSOLETE|        ast = ast.join(subtree)
#OBSOLETE|
#OBSOLETE|    
#OBSOLETE|    #froms = {}
#OBSOLETE|    #get_qp_rec(ast, pruned_tree, root_table, froms)
#OBSOLETE|
#OBSOLETE|    return ast


# Pour chaque table
    # m1:
    #   P1 m1 (a, b, c, d, e)
    #   P2 m1 (a, b, c, d)
    # m2:
    #   P2 m2 (d, e)
# Cache
# A chaque fois qu'on croise une clé et qu'il n'y a pas de trou : DUP + PROJ
# Parcours: feuilles d'abord join avec ce qui a été déjà join
# Si la table vient de plusieurs plateformes, construire l'union des from de chaque partition
#     Mais certains de ces FromTable sont simplement des Cache (FromList)

# TODO: les noeuds From doivent avoir plusieurs callbacks
# - alimentation des union et des fromlists
# TODO: create noeud DUP
# TODO: Cache: associe à chaque methode des FROM_LIST

# 1) UNION des froms sur toutes les partitions
# 2) JOIN (dfs)
# 3) DUP(SELECT()) qui alimente des FROM_LIST

# DFS : but ordonner dans quel ordre les champs sont query
# => "graphe des vues" ordonné et annoté comportant éventuellement des "trous"
# Join des tables ordonnées (chercher dans le cache) 
# Pour chaque "étage": UNION DE FROM chaque plateforme (si elle fournit)

@accepts(User, Query, DiGraph)
@returns(AST)
def build_query_plane(user, user_query, pruned_tree):
    """
    \brief Compute a query plane according to a pruned tree
    \param user The User instance representing the user issuing the query
        \sa tophat/model/user.py
    \param user_query A Query instance (the query issued by the user)
    \param pruned_tree A DiGraph instance representing the 3nf-tree
        such as each remaining key in and each remaining field
        (stored in the DiGraph nodes) is needed 
        - either because it is explicitly queried by the user or either because
        - either because it is needed to join tables involved in the 3nf-tree)
    \return an AST instance which describes the resulting query plane
    """
    print "-" * 80
    print "build_query_plane()"
    print "-" * 80
    print "W: dummy cache" 
    cache = None 
    ast = AST(user = user)

    # Find the root node in the pruned 3nf tree
    root_node = find_root(pruned_tree)

    # Exploring this tree according to a DFS algorithm leads to a table
    # ordering leading to feasible successive joins
    ordered_tables = dfs_preorder_nodes(pruned_tree, root_node)
    for table in ordered_tables:
        from_asts = list()
        key = list(table.get_keys())[0]

        # For each platform related to the current table, extract the
        # corresponding table and build the corresponding FROM node
        map_method_fields = table.get_annotations()
        for method, fields in map_method_fields.items(): 
            sub_table = Table.make_table_from_platform(table, fields, method.get_platform())
            print "sub_table %s" % sub_table

            query = Query(
                user_query.get_action(),                # action
                method.get_name(),                      # from
                [],                                     # where will be eventually optimized later
                user_query.get_params(),                # params
                [field.get_name() for field in fields], # select
                user_query.get_ts()                     # ts
            )

            from_ast = AST(user = user).From(sub_table, query)
            from_asts.append(from_ast)

        # Add the current table in the query plane 
        if ast.is_empty():
            # Process this table, which is the root of the 3nf tree
            ast.union(from_asts, key)
        else:
            # Retrieve in-edge (u-->v): there is always exactly 1
            # predecessor in the 3nf tree since v is not the root.
            v = table
            preds = pruned_tree.predecessors(v)
            assert len(preds) == 1, "pruned_tree is not a tree: predecessors(%r) = %r" % (table, preds)
            u = preds[0]
            predicate = pruned_tree[u][v]["predicate"]
            ast.left_join(AST(user = user).union(from_asts, key), predicate)
    return ast

#@returns(AST)
#def from_table(user, query, table, cache = None):
#    """
#    \param user The User issuing the query
#    \param query A Query instance (used to retrieve records from the platform)
#    \param table The Table queried by the user
#    \param cache
#    \return An AST instance able to query efficiently the table
#    """
#    # Split per platform
#    from_asts = list()
#    for platform, predicate in table.get_partitions().items():
#        table = Table([platform], None, table.get_name(), table.get_fields(), table.get_keys())
#        ast = AST(user = user).From(table, query)
#        from_asts.append(ast)
#        #from_nodes.append(from_partition(platform, table.get_name(), table.get_field_names(), None, cache))
#
#    # Craft an AST made of:
#    # - 1 FROM node (1 child)
#    # - 1 UNION node gathering n>1 FROM nodes
#    ast = None
#    if len(from_asts) == 1:
#        ast = from_asts[0]
#    else:
#        assert len(table.get_keys()) != 0, "This table has no key!\n%s!" % table
#        ast = AST(user = user)
#        key = list(table.get_keys())[0]
#        ast.union(from_asts, key)
#    return ast 
#
#def from_partition(platform, table_name, field_names, annotations, cache = None):
#    # annotations:  je sais que P1:X me fournit aussi P1:Y et P1:Z
#    if cache:
#        try:
#            return cache[(platform, table_name)]
#        except:
#            pass
#
##    # create From
##    create demux # alimenter pi/dups avant le from (cf left join) 
##    for each annotation
##        create pi -> dup -> fromlist(annotation)
##        if cache:
##            cache.add(fromlist)
##    callback From sur union
##    return From
#    return None
