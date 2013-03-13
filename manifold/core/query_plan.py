#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convert a 3nf-tree into an AST (e.g. a query plan)
# \sa manifold.core.pruned_tree.py
# \sa manifold.core.ast.py
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# NOTE: The fastest way to traverse all edges of a graph is via
# adjacency_iter(), but the edges() method is often more convenient.

from networkx                                         import DiGraph
from networkx.algorithms.traversal.depth_first_search import dfs_preorder_nodes

from manifold.core.ast          import AST, From, Union, LeftJoin
from manifold.core.table        import Table 
from manifold.core.query        import Query 
from manifold.util.type         import returns, accepts
from manifold.core.dbgraph      import find_root
from manifold.models.user       import User
from manifold.util.callback     import Callback
from manifold.core.filter       import Filter

class QueryPlan(object):

    def __init__(self):
        self.ast = AST()
        self.froms = []

    def build(self, query, metadata, allowed_capabilities):
        """
        \brief Builds a query plane for a router or a platform
        \param query
        \param metadata a list of metadata for all platforms
        \param allowed_capabilities the set of operators we can use to build the
        query plane (this is a parameter of the router)
        """
        # We answer a query given 
        # What if we cannot answer the query ? or answer it partially
        # What about subqueries, and onjoin ?

        return None

    def build_simple(self, query, metadata, allowed_capabilities):
        """
        \brief Builds a query plan to a single gateway
        """

        # XXX Check whether we can answer query.fact_table


        # Here we assume we have a single platform
        platform = metadata.keys()[0]
        announce = metadata[platform][query.fact_table] # eg. table test
        

        # Set up an AST for missing capabilities (need configuration)
        #
        # Selection ?
        if query.filters and not announce.capabilities.selection:
            if not allowed_capabilities.projection:
                raise Exception, 'Cannot answer query: PROJECTION'
            add_selection = query.filters
            query.filters = Filter()
        else:
            add_selection = None
        #
        # Projection ?
        #
        announce_fields = set([f.get_name() for f in announce.table.fields])
        if query.fields < announce_fields and not announce.capabilities.projection:
            if not allowed_capabilities.projection:
                raise Exception, 'Cannot answer query: PROJECTION'
            add_projection = query.fields
            query.fields = set()
        else:
            add_projection = None

        t = Table({platform:''}, {}, query.fact_table, set(), set())
        self.ast = self.ast.From(t, query)

        # XXX associate the From node to the Gateway
        fromnode = self.ast.root
        self.froms.append(fromnode)
        #fromnode.set_gateway(gw_or_router)
        #gw_or_router.query = query


        if add_selection:
            self.ast = self.ast.selection(add_selection) # set of predicates
        if add_projection:
            self.ast = self.ast.projection(add_projection) # list of fields

        self.ast.dump()


    def execute(self, callback=None):
        cb = callback if callback else Callback()
        self.ast.set_callback(cb)
        self.ast.start()
        if not callback:
            return cb.get_results()
        return
        

# Marco's code

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
def build_query_plan(user, user_query, pruned_tree):
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
    print "build_query_plan()"
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

    # Add WHERE node the tree
    if user_query.get_where() != set():
        ast.selection(user_query.get_where())

    # Add SELECT node above the tree
    ast.projection(list(user_query.get_select()))

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
