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

from manifold.core.ast            import AST, From, Union, LeftJoin
from manifold.core.table          import Table 
from manifold.core.query          import Query 
from manifold.core.key            import Key 
from manifold.util.type           import returns, accepts
from manifold.core.dbgraph        import find_root
from manifold.models.user         import User

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

        return self.build_simple(query, metadata, allowed_capabilities)

    def build_simple(self, query, metadata, allowed_capabilities):
        """
        \brief Builds a query plan to a single gateway
        """

        # XXX Check whether we can answer query.fact_table

        # Set up an AST for missing capabilities (need configuration)
        #
        # Selection ?
        if query.filters and not platform_capabilities.selection:
            add_selection = query.filters
            query.filters = []
        else:
            add_selection = None
        #
        # Projection ?
        if query.fields < platform_fields[query.fact_table] \
                and not platform_capabilities.projection:
            add_projection = query.fields
            query.fields = [] # all
        else:
            add_projection = None

        t = Table({u'dummy':''}, {}, '', set(), set())
        self.ast = self.ast.From(t, query)

        # XXX associate the From node to the Gateway
        fromnode = self.ast.root
        self.froms.append(fromnode)
        fromnode.set_gateway(gw_or_router)

        gw_or_router.query = query

        if add_selection:
            print "SUPPLEMENTING selection"
            self.ast = self.ast.selection(add_selection) # set of predicates
        if add_projection:
            print "SUPPLEMENTING projection"
            self.ast = self.ast.projection(add_projection) # list of fields

        print "GENERATED AST="
        self.ast.dump()

    def set_callback(self, callback):
        self.ast.set_callback(callback)

    def execute(self, callback=None):
        cb = callback if callback else Callback()
        self.set_callback(cb)
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
    def get_methods_supporting_key(table, key):
        # TO MOVE IN PRUNING
        assert isinstance(table, Table), "Invalid table = %r (%r)" % (table, type(table))
        assert isinstance(key,   Key),   "Invalid key = %r (%r)"   % (key,   type(key))
        assert len(key) >= 0,            "Empty key"

        key_methods = None
        for key_elt in key:
            if key_methods == None:
                key_methods = set(table.map_field_methods[key_elt])
            else:
                key_methods &= table.map_field_methods[key_elt]
        return key_methods 

    def get_methods_needing_demux(tables):
        map_method_fields = dict()
        map_tablename_method = dict()

        for table in tables:

            # Intersecting keys
            for key in table.get_keys():
                for method in get_methods_supporting_key(table, key):
                    table_name = table.get_name()
                    if table_name not in map_tablename_method.keys():
                        map_tablename_method[table.get_name()] = set()
                    map_tablename_method[table.get_name()].add(method)
                
            # Intersecting fields 
            for field, methods in table.map_field_methods.items():
                for method in methods:
                    if method not in map_method_fields.keys():
                        map_method_fields[method] = set()
                    map_method_fields[method].add(field)

                    if method.get_name() != table.get_name():
                        methods_demux.add(method)
        return methods_demux 

    print "-" * 80
    print "build_query_plane()"
    print "-" * 80
    ast = AST(user = user)

    # Find the root node in the pruned 3nf tree
    root_node = find_root(pruned_tree)

    # Exploring this tree according to a DFS algorithm leads to a table
    # ordering leading to feasible successive joins
    map_method_demux = dict()
    map_method_bestkey = dict()

    methods_needing_demux = get_methods_needing_demux(pruned_tree.nodes(False))
    print "methods_needing_demux = %r" % methods_needing_demux

    ordered_tables = dfs_preorder_nodes(pruned_tree, root_node)
    for table in ordered_tables:
        from_asts = list()
        key = list(table.get_keys())[0]

        # Rq: the best key should/could be computed during the pruning
        # Compute which methods support the current key
        # The more we are explore the tree, the best the key is
        for method in get_methods_supporting_key(table, key):
            map_method_bestkey[method] = key

        # For each platform related to the current table, extract the
        # corresponding table and build the corresponding FROM node
        map_method_fields = table.get_annotations()
        for method, fields in map_method_fields.items(): 
            if method.get_name() == table.get_name():
                # The table announced by the platform fits with the 3nf schema
                # Build the corresponding FROM 
                sub_table = Table.make_table_from_platform(table, fields, method.get_platform())

                query = Query(
                    user_query.get_action(),                # action
                    method.get_name(),                      # from
                    [],                                     # where will be eventually optimized later
                    user_query.get_params(),                # params
                    [field.get_name() for field in fields], # select
                    user_query.get_ts()                     # ts
                )

                from_ast = AST(user = user).From(sub_table, query)
                map_method_demux[method] = from_ast.get_root()
            else:
                # The table announced by the platform doesn't fit with the 3nf schema
                # Build a FROMLIST + DUP(best_key) + SELECT(best_key u {fields}) branch
                # and plug it to the above the FROM referenced in map_method_from[method] 
                # Ask this FROM node for fetching fields
                from_node = map_method_demux[method] # TO FIX
                key_dup = map_method_bestkey[method]
                select_fields = list(set(fields) | set(key_dup))
                from_node.add_fields_to_query([field.get_name() for field in fields])

                print "FROMLIST -- DUP(%r) -- SELECT(%r) -- %r" % (key_dup, select_fields, from_node) 

                # Build a new AST (the branch we'll add) above an existing FROM node
                from_ast = AST(user = user)
                from_ast.root = from_node
                #TODO from_node.add_callback(from_ast.callback)

                # Add DUP and SELECT to this AST
                #TODO from_ast.duplicate(key_dup).projection(select_fields)
                
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
