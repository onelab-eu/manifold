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
#from networkx.algorithms.traversal.depth_first_search import dfs_preorder_nodes
from manifold.core.table           import Table 
from manifold.core.key             import Key
from manifold.core.query           import Query, AnalyzedQuery 
from manifold.core.dbgraph         import find_root
from manifold.core.relation        import Relation
from manifold.core.filter          import Filter
#from manifold.core.pruned_tree     import build_pruned_tree
from manifold.core.ast             import AST
#from manifold.operators.From       import From
#from manifold.operators.selection  import Selection
#from manifold.operators.projection import Projection
#from manifold.operators.union      import Union
#from manifold.operators.subquery   import SubQuery
#from manifold.operators.demux      import Demux
#from manifold.operators.dup        import Dup
from manifold.util.predicate       import Predicate, contains, eq
from manifold.util.type            import returns, accepts
from manifold.util.callback        import Callback
#from manifold.util.dfs             import dfs
from manifold.util.log             import Log
from manifold.models.user          import User

class QueryPlan(object):

    def __init__(self):
        # TODO metadata, user should be a property of the query plan
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

    def build_union(self, user_query, table, needed_fields, metadata, user):
        from_asts = list()
        key = list(table.get_keys())
        key = key[0] if key else None

        # TO BE REMOVED ?
        # Exploring this tree according to a DFS algorithm leads to a table
        # ordering leading to feasible successive joins
        map_method_bestkey = dict()
        map_method_demux   = dict()

        # XXX I don't understand this -- Jordan
        # Update the key used by a given method
        # The more we iterate, the best the key is
        if key:
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
                            .set(user_query.get_params()).select(fields & needed_fields)
                # user_query.get_timestamp() # timestamp
                # where will be eventually optimized later

                platform = method.get_platform()
                capabilities = metadata.get_capabilities(platform, query.object)

                # XXX Improve platform capabilities support
                if not capabilities.retrieve: continue
                from_ast = AST(user = user).From(platform, query, capabilities, key, fields)

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
        # Process this table, which is the root of the 3nf tree
        return AST().union(from_asts, key)

    # metadata == router.g_3nf
    def build(self, query, metadata, allowed_capabilities, user = None,   qp = None):
        Log.debug(query)

        # XXX In the current recursive version, we might go far in the
        # XXX recursion to find fields that in fact will be found closer in the
        # XXX next iteration. We should in fact do a BFS. We expect the schema
        # XXX to be finite and small enough so that it does not make a big
        # XXX difference
        query = AnalyzedQuery(query, metadata)
        root = metadata.find_node(query.get_from())

        # Local fields in root table
        missing_fields  = set()
        missing_fields |= query.get_select()
        missing_fields |= query.get_where().get_field_names()
        missing_fields |= query.get_subquery_names() # only if those subqueries are used XXX

        ast, missing_fields = self.process_subqueries(root, query, missing_fields, metadata, allowed_capabilities, user, 0)

        if ast:
             ast.optimize_selection(query.get_where())
             #ast.optimize_projection(query.get_select())

        Log.warning("Missing fields: %r" % missing_fields)
        self.ast = ast
        

    def process_query(self, root, query, missing_fields, metadata, allowed_capabilities, user, seen, depth):
        Log.debug(root, query, missing_fields, depth)
        # Explore throughout 1-1, and return needed ast beyond 1-N

        # We need root if it provides fields, or connection to needed table
        # Let's start looking at neighbours

        ast = None
        neighbour_ast_predicate_list = []
        if not seen: seen = ()
        if root in seen: return (None, missing_fields, ())
        seen += (root,)
        
        root_fields = set(root.get_field_names())
        unique_fields = root_fields & missing_fields
        if depth > 0:
            # key fields are available in parent, but we will need them for join
            key_fields = root.get_keys().one()
            unique_fields -= key_fields 

        missing_fields -= unique_fields
        
        relations    = ()
        relations_11 = ()
        relations_1N = ()

        for neighbour in metadata.graph.successors(root):
            for relation in metadata.get_relations(root, neighbour):
                if relation.get_type() not in [Relation.types.LINK, Relation.types.CHILD, Relation.types.PARENT]:
                    relations_1N += ((root, neighbour, relation),)
                else:
                    relations_11 += ((neighbour, relation),)

        for neighbour, relation in relations_11:
            if not missing_fields and not query.get_subquery_names():
                break
            #if neighbour in seen:
            #    continue
            #seen.add(neighbour)
            # we expect the set of missing_fields to reduce through exploration
            _ast, missing_fields, _relations_1N = self.process_query(neighbour, query, missing_fields, metadata, allowed_capabilities, user, seen, depth)
            if _ast:
                # if children_ast, we will at least have a minimal _ast to connect them
                predicate = relation.get_predicate()
                neighbour_ast_predicate_list.append((_ast, predicate))
            relations_1N += _relations_1N

        if unique_fields or neighbour_ast_predicate_list: # we need root
            if depth > 0:
                # We add the key that will be necessary for join
                unique_fields |= root.keys.one().get_names()
            for _ast, _predicate in neighbour_ast_predicate_list:
                unique_fields |= _predicate.get_field_names()
            ast = self.build_union(query, root, unique_fields, metadata, user)
            
        for _ast, _predicate in neighbour_ast_predicate_list:
            ast.left_join(_ast, _predicate)
            # XXX Make sure we have all necessary fields for LEFTJOIN

        # XXX We need to do it from subquery to keep keys for 1..N relationships
        #if ast:
        #    ast.optimize_selection(query.get_where())
        #    ast.optimize_projection(query.get_select())

        return (ast, missing_fields, relations_1N)

    def process_subqueries(self, root, query, missing_fields, metadata, allowed_capabilities, user, depth):
        Log.debug(root, query, missing_fields, depth)
        if depth >= 3:
            return (None, missing_fields) # Nothing found

        ast = None
        relations_1N = ()

        # We process current_node, as well as the chain of 1..1 nodes, we will also get 1..N nodes in _children_ast
        _ast, missing_fields, _relations_1N = self.process_query(root, query, missing_fields, metadata, allowed_capabilities, user, None, depth)
        relations_1N += _relations_1N
        
        if _ast:
            ast = _ast

        children_ast_predicate_list  = []

        seen = ()
        for root, neighbour, relation in relations_1N:
            if neighbour in seen: continue
            seen += (neighbour,)

            if not missing_fields and not query.get_subquery_names():
                break

            # missing fields should be extended if we go through a link in the query
            # here we know which subqueries are used XXX

            relation_name = relation.get_predicate().get_key()
            sq = query.get_subquery(relation_name)
            if sq:
                query2 = sq
                missing_fields2  = missing_fields
                missing_fields2 |= sq.get_select()
                missing_fields2 |= sq.get_where().get_field_names()
                missing_fields2 |= sq.get_subquery_names() # only if those subqueries are used XXX

            else:
                query2 = query
                missing_fields2 = missing_fields

            if not missing_fields2: continue

            child_ast, missing_fields = self.process_subqueries(neighbour, query2, missing_fields2, metadata, allowed_capabilities, user, depth+1)
            if child_ast:
                children_ast_predicate_list.append((child_ast, relation.get_predicate()))
            continue



        if children_ast_predicate_list:
            children_ast_predicate_list = [ (a.get_root(), p) for a, p in children_ast_predicate_list]
            ast.subquery(children_ast_predicate_list)

        return (ast, missing_fields)


###############################################################################################
#
#
#
#
#
#
#
#        # The query plan we are building
#        query_plan = None
#
#        root = metadata.find_node(user_query.get_from())
#
#        missing_fields = set()  # the set of fields being looked for
#
#        query = AnalyzedQuery(user_query, metadata)
#        stack = ()
#
#        for pred, table, relation in metadata.iter_tables(root):
#            print '-'*80
#            print "prev     =", pred.get_name() if pred else "NONE"
#            print "table    =", table.get_name()
#            print "relation = %s" % relation
#
#            if relation:
#                # We have followed a relation in the graph
#                name = relation.get_predicate().get_key()
#                print "We have followed relation name", name, "to reach", table.get_name()
#                if relation.get_type() == Relation.types.LINK_1N:
#                    # We cross a 1..N section, back up the query for later
#                    stack += (query,)
#                    # And look after fields inside subquery (other remaining fields will be in missing)
#                    q = query.get_subquery(name)
#                    if q:
#                        query = q
#    
#            # Local fields in root table
#            needed_fields  = set()
#            needed_fields |= query.get_select()
#            needed_fields |= query.get_where().get_field_names()
#            needed_fields |= missing_fields
#
#            # we keep only base such as field =  base.subfield (not now since AQ)
#            # needed_fields = set(map(lambda x: x.split('.', 2)[0], needed_fields))
#
#            provided_fields = set(table.get_field_names())
#            missing_fields  = needed_fields - provided_fields
#
#            Log.tmp("Searching for fields", needed_fields, "in", root.get_name(), "found:", provided_fields, "- missing", missing_fields)
#
#            ast = self.build_union(query, table, provided_fields, metadata, user)
#            query_plan = ast if not query_plan else query_plan.left_join(ast, relation.get_predicate())
#
#        if missing_fields:
#            Log.warning("Missing fields: %r" % missing_fields)
#
#        self.ast = query_plan
#        return
#        
#
#
#        # OLD CODE
#        analyzed_query = AnalyzedQuery(query, metadata)
#        print "AQ=", analyzed_query
#        self.ast = self.process_subqueries(analyzed_query, metadata, allowed_capabilities, user)
#
#    def process_subqueries(self, query, metadata, allowed_capabilities, user, in_subquery = False):
#        """
#        \brief Builds a query plane for a router or a platform, consisting
#        mainly in the AST (tree of SQL operators) related to a query
#        \sa manifold.core.ast.py
#        \param query A Query issued by the user
#        \param metadata a list of metadata for all platforms
#        \param allowed_capabilities the set of operators we can use to build the
#        query plane (this is a parameter of the router)
#        \param user A User instance (carry user's information) 
#        """
#        Log.debug("=" * 100)
#        Log.debug("Entering process_subqueries %s (need fields %s) " % (query.get_from(), query.get_select()))
#        Log.tmp("Query=", query)
#        Log.debug("=" * 100)
#
#        table_name = query.get_from()
#        table = metadata.find_node(table_name)
#        if not table:
#            raise ValueError("Can't find table %r related to query %r" % (table_name, query))
#
#        qp, missing_fields = self.process_query(query, metadata, user, in_subquery)
#        # We need to reinject missing_fields in subqueries that are not multiple
#        # XXX attempt, reinject in _all_ of them
#
#        children_ast = {}
#        sq_relations = {}
#        subquery_methods = set()
#        for method, subquery in query.subqueries():
#
#            # We need to determine how to join with each subquery
#            # key of subquery
#            # key of parent query
#
#            # Here we need to analyze metadata = full information about a table...
#            # XXX Analysing subqueries might be a bit more complicated than that
#            # XXX We might need to inspect the arcs of DBGraph
#
#            method_type = metadata.get_field_type(table_name, method)
#            method_table = metadata.find_node(method_type)
#
#            # Get the relation between the parent and the child tables of a subquery
#            relations = metadata.get_relations(table_name, method_table)
#            # we expect only one for the given method which corresponds to one field
#            relations = [r for r in relations if r.get_predicate().get_key() == method]
#            print "relations", relations
#            assert len(relations) == 1, "Multiple relations for subquery"
#            relation = relations[0]
#            sq_relations[method] = relation
#
#            p = relation.get_predicate()
#            t = relation.get_type()
#            if t not in [Relation.types.LINK_1N]:
#                print "injecting missing fields", missing_fields, "into sq", method
#                subquery.select(missing_fields)
#            query.select(p.get_key())
#            subquery.select(p.get_value())
#
#
## OLD #             # (1) Do we have a reachable field of type method[] that contains a
## OLD #             # list of identifier for child items
## OLD #             
## OLD #             fields = [ f for f in metadata.get_fields(table) if f.get_name() == method]
## OLD #             if fields:
## OLD #                 field = fields[0]
## OLD #                 if field.is_array(): # 1..N
## OLD #                     # We add the field name to the set of retrieved fields
## OLD #                     #Log.tmp("=============================== Query.Select(", method, ")", query)
## OLD #                     child_key_fields = method_table.get_keys().one().get_minimal_names()
## OLD #                     predicates[method] = Predicate(method, contains, child_key_fields)
## OLD #                     query.select(method)
## OLD #                     # TODO We need to be sure that the key is retrieved in the child
## OLD #                 else: # 1..1
## OLD #                     raise Exception, "1..1 relationships not handled"
## OLD # 
## OLD #             # (2) Do we have pointers to the parent
## OLD #             else:
## OLD #                 parent_fields = set(metadata.get_fields(table))
## OLD #                 parent_key = table.get_keys().one()
## OLD #                 child_fields = set(metadata.get_fields(method_table))
## OLD #                 # XXX why is it necessarily the key of the child, and not the fields...
## OLD #                 intersection = parent_fields & child_fields
## OLD #                 intersection2 = set([f.get_name() for f in child_fields if f.get_name() == table_name])
## OLD #                 if intersection == parent_fields:
## OLD #                     # 1..1
## OLD #                     raise Exception, "1..1 relationships not handled"
## OLD # 
## OLD #                 elif intersection:
## OLD #                     # 1..N
## OLD #                     # Add the fields in both the query and the subquery
## OLD #                     for field in intersection:
## OLD #                         predicates[method] = Predicate(None, None, None)
## OLD #                         query.select(field.get_name())
## OLD #                         subquery.select(field.get_name())
## OLD # 
## OLD #                 elif intersection2:
## OLD #                     # Child table references parent table name
## OLD #                     predicates[method] = Predicate(parent_key.get_minimal_names(), eq, table.get_name())
## OLD #                     subquery.select(table.get_name())
## OLD #                     #subquery.select(parent_key.get_names())
## OLD #                     
## OLD # 
## OLD #                 else:
## OLD #                     # Find a path
## OLD #                     import networkx as nx
## OLD #                     #print "PATH=", nx.shortest_path(metadata.graph, table, method_table)
## OLD #                     raise Exception, "No relation between parent '%s' and child '%s'" % (table_name, method)
#
#            # XXX Between slice and application, we have leases... how to handle ???
#
#            # Recursive processing of subqueries
#            child_ast = self.process_subqueries(subquery, metadata, allowed_capabilities, user, in_subquery=True)
#            children_ast[method] = child_ast.root
#
#        if children_ast:
#            # We are not interested in the 3nf fields, but in the set of fields that will be available when we answer the whole parent query
#            # parent_fields = metadata.find_node(query.object).get_field_names() # wrong
#            # XXX Note that we should request in the parent any field needed for subqueries
#            parent_fields = query.fields - subquery_methods
#
#            # XXX some fields are 1..N fields and should not be present in this list...
#            qp.subquery(children_ast, sq_relations, table.keys.one())
#
#        return qp
#
#    @returns(AST)
#    def process_query(self, query, metadata, user, in_subquery = False):
#        """
#        \brief Compute the query plan related to a query which involves
#            no sub-queries. Sub-queries should already processed thanks to
#            process_subqueries().
#        \param query The Query instance representing the query issued by the user.
#            \sa manifold/core/query.py
#        \param user The User instance reprensenting the user issuing
#            the query. The query can be resolved in various way according to
#            the user grants.
#            \sa tophat/model/user.py
#        \return The AST instance representing the query plan.
#        """
#
#        Log.debug("-" * 100)
#        Log.debug("Entering process_query %s (need fields %s) " % (query.get_from(), query.get_select()))
#        Log.tmp("Query=", query)
#        Log.debug("-" * 100)
#
#        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
#        needed_fields = set(query.get_select())
#
#        # We don't necessary need fields in a given table because of JOINS that will bring them
#        #if needed_fields == set():
#        #    raise ValueError("No queried field")
#        needed_fields.update(query.get_where().keys())
#
#        # Retrieve the root node corresponding to the fact table
#        #print query
#        #print "METADATA FOR DFS", metadata
#        #for t in metadata.graph.nodes():
#        #    print str(t)
#        root = metadata.find_node(query.get_from())
#
#        # Retrieve the (unique due to 3-nf) tree included in "self.g_3nf" and rooted in "root"
#        # \sa manifold.util.dfs.py
#        #print "Entering DFS(%r) in graph:" % root
#
#        # Compute the corresponding pruned tree.
#        # Each node of the pruned tree only gathers relevant table, and only their
#        # relevant fields and their relevant key (if used).
#        # \sa manifold.util.pruned_graph.py
#
#        # We will exclude from the dfs arcs for which there are only 1..N relationships
#        exclude_uv = lambda u, v: not not [r for r in metadata.get_relations(u,v) if r.get_type() != Relation.types.LINK_1N]
#
#        dfs_tree = dfs(metadata.graph, root, exclude_uv=exclude_uv)
#        pruned_tree, missing_fields = build_pruned_tree(metadata, needed_fields, dfs_tree)
#        #pruned_tree = build_pruned_tree(metadata, needed_fields, dfs(metadata.graph, root))
#
#        # Compute the skeleton resulting query plan
#        # (e.g which does not take into account the query)
#        # It leads to a query plan made of Union, From, and LeftJoin nodes
#        return (self.build_query_plan(user, query, pruned_tree, metadata, in_subquery), missing_fields)

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

#    # @accepts(User, Query, DiGraph)
#    # @returns(AST)
#    def build_query_plan(self, user, user_query, pruned_tree, metadata, in_subquery = False):
#        """
#        \brief Compute a query plane according to a pruned tree
#        \param user The User instance representing the user issuing the query
#            \sa tophat/model/user.py
#        \param user_query A Query instance (the query issued by the user)
#        \param pruned_tree A DiGraph instance representing the 3nf-tree
#            such as each remaining key in and each remaining field
#            (stored in the DiGraph nodes) is needed 
#            - either because it is explicitly queried by the user or either because
#            - either because it is needed to join tables involved in the 3nf-tree)
#        \return an AST instance which describes the resulting query plane
#        """
#
#        #print "-" * 80
#        #print "build_query_plan()"
#        #print "-" * 80
#        ast = AST(user = user)
#
#        # Find the root node in the pruned 3nf tree
#        root_node = find_root(pruned_tree)
#
#        # Exploring this tree according to a DFS algorithm leads to a table
#        # ordering leading to feasible successive joins
#        map_method_bestkey = dict()
#        map_method_demux   = dict()
#
#        ordered_tables = dfs_preorder_nodes(pruned_tree, root_node)
#
#        # Let's remove parent tables from ordered tables
#        tmp = []
#        prev_table = None
#        cpt = 0
#        for table in ordered_tables:
#            if prev_table:
#                if prev_table.name == table.name:
#                    cpt += 1
#                else:
#                    cpt = 0
#                if cpt != 1: tmp.append(prev_table)
#            prev_table = table
#        tmp.append(prev_table)
#        ordered_tables = tmp
#        
#        for table in ordered_tables:
#            from_asts = list()
#            key = list(table.get_keys())
#            key = key[0] if key else None
#
#            # XXX I don't understand this -- Jordan
#            # Update the key used by a given method
#            # The more we iterate, the best the key is
#            if key:
#                for method, keys in table.map_method_keys.items():
#                    if key in table.map_method_keys[method]: 
#                        map_method_bestkey[method] = key 
#
#            # For each platform related to the current table, extract the
#            # corresponding table and build the corresponding FROM node
#            map_method_fields = table.get_annotations()
#            for method, fields in map_method_fields.items(): 
#                if method.get_name() == table.get_name():
#                    # The table announced by the platform fits with the 3nf schema
#                    # Build the corresponding FROM 
#                    #sub_table = Table.make_table_from_platform(table, fields, method.get_platform())
#
#                    # XXX We lack field pruning
#                    query = Query.action(user_query.get_action(), method.get_name()) \
#                                .set(user_query.get_params()).select(fields)
#                    # user_query.get_timestamp() # timestamp
#                    # where will be eventually optimized later
#
#                    platform = method.get_platform()
#                    capabilities = metadata.get_capabilities(platform, query.object)
#
#                    # XXX Improve platform capabilities support
#                    if not in_subquery and not capabilities.retrieve: continue
#                    from_ast = AST(user = user).From(platform, query, capabilities, key)
#
#                    self.froms.append(from_ast.root)
#
#                    if method in table.methods_demux:
#                        from_ast.demux().projection(list(fields))
#                        demux_node = from_ast.get_root().get_child()
#                        assert isinstance(demux_node, Demux), "Bug"
#                        map_method_demux[method] = demux_node 
#
#                else:
#                    # The table announced by the platform doesn't fit with the 3nf schema
#                    # Build a FROMLIST + DUP(best_key) + SELECT(best_key u {fields}) branch
#                    # and plug it to the above the DEMUX node referenced in map_method_demux
#                    # Ask this FROM node for fetching fields
#                    demux_node = map_method_demux[method]
#                    from_node = demux_node.get_child()
#                    key_dup = map_method_bestkey[method]
#                    select_fields = list(set(fields) | set(key_dup))
#                    from_node.add_fields_to_query(fields)
#
#                    print "FROMLIST -- DUP(%r) -- SELECT(%r) -- %r -- %r" % (key_dup, select_fields, demux_node, from_node) 
#
#                    # Build a new AST (the branch we'll add) above an existing FROM node
#                    from_ast = AST(user = user)
#                    from_ast.root = demux_node
#                    #TODO from_node.add_callback(from_ast.callback)
#
#                    self.froms.append(from_ast.root)
#
#                    # Add DUP and SELECT to this AST
#                    from_ast.dup(key_dup).projection(select_fields)
#                    
#                from_asts.append(from_ast)
#
#            # Add the current table in the query plane 
#            if ast.is_empty():
#                # Process this table, which is the root of the 3nf tree
#                if from_asts:
#                    ast.union(from_asts, key)
#            else:
#                # Retrieve in-edge (u-->v): there is always exactly 1
#                # predecessor in the 3nf tree since v is not the root.
#                # XXX JE NE COMPRENDS PAS CA !!!
#                print "AST", ast.dump()
#                v = table
#                print "TABLE", v
#                preds = pruned_tree.predecessors(v)
#                assert len(preds) == 1, "pruned_tree is not a tree: predecessors(%r) = %r" % (table, preds)
#                u = preds[0]
#                predicate = pruned_tree[u][v]["relation"].get_predicate()
#                print "PREDICATE", predicate
#                ast.left_join(AST(user = user).union(from_asts, key), predicate)
#
#        if not ast.root: return ast
#
#        # Add WHERE node the tree
#        ast.optimize_selection(user_query.get_where())
#        # Add SELECT node above the tree
#        Log.tmp("OPTIMIZE PROJECTION", user_query.get_select())
#        ast.optimize_projection(user_query.get_select())
#
#        #if user_query.get_where() != set():
#        #    ast.selection(user_query.get_where())
#        #TODO ast.projection(list(user_query.get_select()))
#
#        return ast
#
