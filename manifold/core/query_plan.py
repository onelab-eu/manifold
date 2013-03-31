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

from manifold.core.ast          import AST, From, Union, LeftJoin, Demux, Dup
from manifold.core.table        import Table 
from manifold.core.query        import Query, AnalyzedQuery 
from manifold.util.type         import returns, accepts
from manifold.core.dbgraph      import find_root
from manifold.models.user       import User
from manifold.util.callback     import Callback
from manifold.core.filter       import Filter
from manifold.util.dfs          import dfs
from manifold.core.pruned_tree  import build_pruned_tree

class QueryPlan(object):

    def __init__(self):
        self.ast = AST()
        self.froms = []

    def get_result_value_array(self):
        # Iterate over gateways to get their result values
        # XXX We might need tasks
        result = []
        for from_node in self.froms:
            result.extend(from_node.gateway.get_result_value())
        return result

    # metadata == router.g_3nf
    def build(self, query, metadata, allowed_capabilities, user = None):
        """
        \brief Builds a query plane for a router or a platform, consisting
        mainly in the AST (tree of SQL operators) related to a query
        \sa manifold.core.ast.py
        \param query A Query issued by the user
        \param metadata a list of metadata for all platforms
        \param allowed_capabilities the set of operators we can use to build the
        query plane (this is a parameter of the router)
        \param user A User instance (carry user's information) 
        """
        #print "=" * 100
        #print "Entering process_subqueries %s (need fields %s) " % (query.get_from(), query.get_select())
        #print "=" * 100
        table_name = query.get_from()
        table = metadata.find_node(table_name)
        if not table:
            raise ValueError("Can't find table %r related to query %r" % (table_name, query))

        analyzed_query = AnalyzedQuery(query)

        qp = AST(user)

        children_ast = None
        for subquery in analyzed_query.subqueries():

            method = table.get_field(method).get_type()
            if not method in cur_fields:
                subquery.select(method)

            # Adding primary key in subquery to be able to merge
            keys = metadata_get_keys(method)
            if keys:
                key = list(keys).pop()
                print "W: selecting arbitrary key %s to join with '%s'" % (key, method)
                if isinstance(key, Key):
                    for field in key:
                        field_name = field.get_name()
                        if field_name not in subfields:
                            subquery.select(field_name)
                else:
                    raise TypeError("Invalid type: key = %s (type %s)" % (key, type(key)))

            child_ast = self.process_subqueries(subquery, user)
            children_ast.append(child_ast.root)

        qp = self.process_query(analyzed_query, metadata, user)
        if children_ast: qp.subquery(children_ast)

        self.ast = qp

    @returns(AST)
    def process_query(self, query, metadata, user):
        """
        \brief Compute the query plan related to a query which involves
            no sub-queries. Sub-queries should already processed thanks to
            process_subqueries().
        \param query The Query instance representing the query issued by the user.
            \sa manifold/core/query.py
        \param user The User instance reprensenting the user issuing
            the query. The query can be resolved in various way according to
            the user grants.
            \sa tophat/model/user.py
        \return The AST instance representing the query plan.
        """

        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
        needed_fields = set(query.get_select())
        if needed_fields == set():
            raise ValueError("No queried field")
        needed_fields.update(query.get_where().keys())

        # Retrieve the root node corresponding to the fact table
        #print query
        #print "QUERY FROM", query.get_from()
        #print "METADATA FOR DFS", metadata
        #for t in metadata.graph.nodes():
        #    print str(t)
        root = metadata.find_node(query.get_from())

        # Retrieve the (unique due to 3-nf) tree included in "self.g_3nf" and rooted in "root"
        # \sa manifold.util.dfs.py
        #print "Entering DFS(%r) in graph:" % root

        # Compute the corresponding pruned tree.
        # Each node of the pruned tree only gathers relevant table, and only their
        # relevant fields and their relevant key (if used).
        # \sa manifold.util.pruned_graph.py
        pruned_tree = build_pruned_tree(metadata.graph, needed_fields, dfs(metadata.graph, root))

        # Compute the skeleton resulting query plan
        # (e.g which does not take into account the query)
        # It leads to a query plan made of Union, From, and LeftJoin nodes
        return self.build_query_plan(user, query, pruned_tree)

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
        #if add_projection:
        #    self.ast = self.ast.projection(add_projection) # list of fields

        self.ast.dump()


    def execute(self, callback=None):
        cb = callback if callback else Callback()
        self.ast.set_callback(cb)
        self.ast.start()
        if not callback:
            return cb.get_results()
        return

    def dump(self):
        self.ast.dump()

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

    # @accepts(User, Query, DiGraph)
    # @returns(AST)
    def build_query_plan(self, user, user_query, pruned_tree):
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
        # <<<<<<<< DEBUG DEBUT
        #tables = pruned_tree.nodes(False)
        #
        ## annotations
        #print "-" * 80
        #print "Annotations"
        #print "-" * 80
        #for table in tables:
        #    print "---------------- Table %r ----------------" % table
        #    print "> map_key_methods"
        #    for k, d in table.map_method_keys.items():
        #        print "%r => %r" % (k, d)
        #
        #    print "> map_method_fields"
        #    for k, d in table.map_method_fields.items():
        #        print "%r => %r" % (k, d)
        # >>>>>>>> DEBUG FIN 

        #print "-" * 80
        #print "build_query_plan()"
        #print "-" * 80
        ast = AST(user = user)

        # Find the root node in the pruned 3nf tree
        root_node = find_root(pruned_tree)

        # Exploring this tree according to a DFS algorithm leads to a table
        # ordering leading to feasible successive joins
        map_method_bestkey = dict()
        map_method_demux   = dict()

        ordered_tables = dfs_preorder_nodes(pruned_tree, root_node)
        for table in ordered_tables:
            from_asts = list()
            key = list(table.get_keys())[0]

            # Update the key used by a given method
            # The more we iterate, the best the key is
            for method, keys in table.map_method_keys.items():
                if key in table.map_method_keys[method]: 
                    map_method_bestkey[method] = key 

            # For each platform related to the current table, extract the
            # corresponding table and build the corresponding FROM node
            map_method_fields = table.get_annotations()
            for method, fields in map_method_fields.items(): 
                if method.get_name() == table.get_name():
                    # The table announced by the platform fits with the 3nf schema
                    # Build the corresponding FROM 
                    sub_table = Table.make_table_from_platform(table, fields, method.get_platform())
                    field_names = [field.get_name() for field in fields]

                    query = Query(
                        user_query.get_action(),  # action
                        method.get_name(),        # from
                        [],                       # where will be eventually optimized later
                        user_query.get_params(),  # params
                        field_names,              # select
                        user_query.get_ts()       # ts
                    )

                    from_ast = AST(user = user).From(sub_table, query)

                    self.froms.append(from_ast.root)

                    if method in table.methods_demux:
                        from_ast.demux().projection(list(fields))
                        demux_node = from_ast.get_root().get_child()
                        assert isinstance(demux_node, Demux), "Bug"
                        map_method_demux[method] = demux_node 

                else:
                    # The table announced by the platform doesn't fit with the 3nf schema
                    # Build a FROMLIST + DUP(best_key) + SELECT(best_key u {fields}) branch
                    # and plug it to the above the DEMUX node referenced in map_method_demux
                    # Ask this FROM node for fetching fields
                    demux_node = map_method_demux[method]
                    from_node = demux_node.get_child()
                    key_dup = map_method_bestkey[method]
                    select_fields = list(set(fields) | set(key_dup))
                    from_node.add_fields_to_query([field.get_name() for field in fields])

                    print "FROMLIST -- DUP(%r) -- SELECT(%r) -- %r -- %r" % (key_dup, select_fields, demux_node, from_node) 

                    # Build a new AST (the branch we'll add) above an existing FROM node
                    from_ast = AST(user = user)
                    from_ast.root = demux_node
                    #TODO from_node.add_callback(from_ast.callback)

                    self.froms.append(from_ast.root)

                    # Add DUP and SELECT to this AST
                    from_ast.dup(key_dup).projection(select_fields)
                    
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
        #TODO ast.projection(list(user_query.get_select()))

        return ast

