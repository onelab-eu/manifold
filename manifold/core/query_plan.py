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
from manifold.core.key          import Key
from manifold.core.query        import Query, AnalyzedQuery 
from manifold.core.dbgraph      import find_root
from manifold.core.relation     import Relation
from manifold.core.filter       import Filter
from manifold.core.pruned_tree  import build_pruned_tree
from manifold.util.predicate    import Predicate, contains, eq
from manifold.util.type         import returns, accepts
from manifold.util.callback     import Callback
from manifold.util.dfs          import dfs
from manifold.util.log          import Log
from manifold.models.user       import User

class QueryPlan(object):

    def __init__(self):
        self.ast = AST()
        self.froms = []

    def get_result_value_array(self):
        # Iterate over gateways to get their result values
        # XXX We might need tasks
        result = []
        for from_node in self.froms:
            # If no Gateway 
            if not from_node.gateway: continue
            result.extend(from_node.gateway.get_result_value())
        return result

    # metadata == router.g_3nf
    def build(self, query, metadata, allowed_capabilities, user = None):
        analyzed_query = AnalyzedQuery(query)
        self.ast = self.process_subqueries(analyzed_query, metadata, allowed_capabilities, user)

    def process_subqueries(self, query, metadata, allowed_capabilities, user, in_subquery = False):
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
        Log.debug("=" * 100)
        Log.debug("Entering process_subqueries %s (need fields %s) " % (query.get_from(), query.get_select()))
        Log.tmp("Query=", query)
        Log.debug("=" * 100)

        table_name = query.get_from()
        table = metadata.find_node(table_name)
        if not table:
            raise ValueError("Can't find table %r related to query %r" % (table_name, query))

        # This should eventually be done once by moving it to the root function...
        qp = AST(user)

        children_ast = []
        predicates = {}
        subquery_methods = set()
        for method, subquery in query.subqueries():

            # We need to determine how to join with each subquery
            # key of subquery
            # key of parent query

            # Here we need to analyze metadata = full information about a table...
            # XXX Analysing subqueries might be a bit more complicated than that
            # XXX We might need to inspect the arcs of DBGraph

            method_table = metadata.find_node(method)

            # (1) Do we have a reachable field of type method[] that contains a
            # list of identifier for child items
            
            fields = [ f for f in metadata.get_fields(table) if f.get_name() == method]
            if fields:
                field = fields[0]
                if field.is_array(): # 1..N
                    # We add the field name to the set of retrieved fields
                    #Log.tmp("=============================== Query.Select(", method, ")", query)
                    child_key_fields = method_table.get_keys().one().get_minimal_names()
                    predicates[method] = Predicate(method, contains, child_key_fields)
                    query.select(method)
                    # TODO We need to be sure that the key is retrieved in the child
                else: # 1..1
                    raise Exception, "1..1 relationships not handled"

            # (2) Do we have pointers to the parent
            else:
                parent_fields = set(metadata.get_fields(table))
                parent_key = table.get_keys().one()
                child_fields = set(metadata.get_fields(method_table))
                # XXX why is it necessarily the key of the child, and not the fields...
                print "RELATION (2) ----------------------------"
                print "parent_fields", parent_fields
                print "child_fields", child_fields
                intersection = parent_fields & child_fields
                intersection2 = set([f.get_name() for f in child_fields if f.get_name() == table_name])
                if intersection == parent_fields:
                    # 1..1
                    raise Exception, "1..1 relationships not handled"

                elif intersection:
                    # 1..N
                    print "*** Relation of type (2) between parent '%s' and child '%s'" % (table_name, method) 
                    # Add the fields in both the query and the subquery
                    for field in intersection:
                        predicates[method] = Predicate(None, None, None)
                        query.select(field.get_name())
                        subquery.select(field.get_name())
                        Log.tmp("================================ adding to subquery (intersection)", field.get_name())

                elif intersection2:
                    # Child table references parent table name
                    Log.tmp("=================================== adding to subquery (intersection2)", table.get_name()) #parent_key.get_names())
                    print "PARENT MINIMAL KEY", parent_key.get_minimal_names()
                    print "="*80
                    print "="*80
                    print "="*80
                    print "="*80
                    print "="*80
                    print "="*80
                    print "="*80
                    print "="*80
                    print "="*80
                    predicates[method] = Predicate(parent_key.get_minimal_names(), eq, table.get_name())
                    subquery.select(table.get_name())
                    #subquery.select(parent_key.get_names())
                    

                else:
                    # Find a path
                    import networkx as nx
                    #print "PATH=", nx.shortest_path(metadata.graph, table, method_table)
                    raise Exception, "No relation between parent '%s' and child '%s'" % (table_name, method)

            # XXX Between slice and application, we have leases... how to handle ???

            # Recursive processing of subqueries
            child_ast = self.process_subqueries(subquery, metadata, allowed_capabilities, user, in_subquery=True)
            children_ast.append(child_ast.root)

        qp = self.process_query(query, metadata, user, in_subquery)
        if children_ast:
            # We are not interested in the 3nf fields, but in the set of fields that will be available when we answer the whole parent query
            # parent_fields = metadata.find_node(query.object).get_field_names() # wrong
            # XXX Note that we should request in the parent any field needed for subqueries
            parent_fields = query.fields - subquery_methods

            # XXX some fields are 1..N fields and should not be present in this list...
            qp.subquery(children_ast, predicates, table.keys.one())

        return qp

    @returns(AST)
    def process_query(self, query, metadata, user, in_subquery = False):
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

        Log.debug("-" * 100)
        Log.debug("Entering process_query %s (need fields %s) " % (query.get_from(), query.get_select()))
        Log.tmp("Query=", query)
        Log.debug("-" * 100)

        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
        needed_fields = set(query.get_select())
        if needed_fields == set():
            raise ValueError("No queried field")
        needed_fields.update(query.get_where().keys())

        # Retrieve the root node corresponding to the fact table
        #print query
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
        dfs_tree = dfs(metadata.graph, root, exclude_uv=lambda u,v: metadata.get_relation(u,v).get_type() == Relation.types.LINK_1N)
        pruned_tree = build_pruned_tree(metadata, needed_fields, dfs_tree)
        #pruned_tree = build_pruned_tree(metadata, needed_fields, dfs(metadata.graph, root))

        # Compute the skeleton resulting query plan
        # (e.g which does not take into account the query)
        # It leads to a query plan made of Union, From, and LeftJoin nodes
        return self.build_query_plan(user, query, pruned_tree, metadata, in_subquery)

    def build_simple(self, query, metadata, allowed_capabilities):
        """
        \brief Builds a query plan to a single gateway
        """
        # XXX allowed_capabilities should be a property of the query plan !

        # XXX Check whether we can answer query.object


        # Here we assume we have a single platform
        platform = metadata.keys()[0]
        announce = metadata[platform][query.object] # eg. table test
        

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

        t = Table({platform:''}, {}, query.object, set(), set())
        key = metadata.get_key(query.object)
        cap = metadata.get_capabilities(platform, query.object)
        self.ast = self.ast.From(t, query, metadata.get_capabilities(platform, query.object), key)

        # XXX associate the From node to the Gateway
        fromnode = self.ast.root
        self.froms.append(fromnode)
        #fromnode.set_gateway(gw_or_router)
        #gw_or_router.query = query

        if not self.root: return
        if add_selection:
            self.ast.optimize_selection(add_selection)
        if add_projection:
            self.ast.optimize_projection(add_projection)

        self.ast.dump()


    def execute(self, callback=None):
        try:
            cb = callback if callback else Callback()
            self.ast.set_callback(cb)
            print "before ast start"
            self.ast.start()
            print "after ast start"
            if not callback:
                print "not callback, return results"
                return cb.get_results()
            print "simple return"
            return
        except Exception, e:
            print "E in execute", e

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
    def build_query_plan(self, user, user_query, pruned_tree, metadata, in_subquery = False):
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

        # Let's remove parent tables from ordered tables
        tmp = []
        prev_table = None
        cpt = 0
        for table in ordered_tables:
            if prev_table:
                if prev_table.name == table.name:
                    cpt += 1
                else:
                    cpt = 0
                if cpt != 1: tmp.append(prev_table)
            prev_table = table
        tmp.append(prev_table)
        ordered_tables = tmp
        
        for table in ordered_tables:
            from_asts = list()
            key = list(table.get_keys())[0]

            # XXX I don't understand this -- Jordan
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
                    #sub_table = Table.make_table_from_platform(table, fields, method.get_platform())

                    # XXX We lack field pruning
                    query = Query.action(user_query.get_action(), method.get_name()) \
                                .set(user_query.get_params()).select(fields)
                    # user_query.get_timestamp() # timestamp
                    # where will be eventually optimized later

                    platform = method.get_platform()
                    capabilities = metadata.get_capabilities(platform, query.object)

                    # XXX Improve platform capabilities support
                    if not in_subquery and not capabilities.retrieve: continue
                    from_ast = AST(user = user).From(platform, query, capabilities, key)

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
                    from_node.add_fields_to_query(fields)

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
                if from_asts:
                    ast.union(from_asts, key)
            else:
                # Retrieve in-edge (u-->v): there is always exactly 1
                # predecessor in the 3nf tree since v is not the root.
                # XXX JE NE COMPRENDS PAS CA !!!
                print "AST", ast.dump()
                v = table
                print "TABLE", v
                preds = pruned_tree.predecessors(v)
                assert len(preds) == 1, "pruned_tree is not a tree: predecessors(%r) = %r" % (table, preds)
                u = preds[0]
                predicate = pruned_tree[u][v]["relation"].get_predicate()
                print "PREDICATE", predicate
                ast.left_join(AST(user = user).union(from_asts, key), predicate)

        if not ast.root: return ast

        # Add WHERE node the tree
        ast.optimize_selection(user_query.get_where())
        # Add SELECT node above the tree
        Log.tmp("OPTIMIZE PROJECTION", user_query.get_select())
        ast.optimize_projection(user_query.get_select())

        #if user_query.get_where() != set():
        #    ast.selection(user_query.get_where())
        #TODO ast.projection(list(user_query.get_select()))

        return ast

