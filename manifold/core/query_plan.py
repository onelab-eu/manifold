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

import copy
from networkx                      import DiGraph
from manifold.core.table           import Table 
from manifold.core.key             import Key
from manifold.core.query           import Query, AnalyzedQuery 
from manifold.core.dbgraph         import find_root
from manifold.core.relation        import Relation
from manifold.core.filter          import Filter
from manifold.core.ast             import AST
from manifold.util.predicate       import Predicate, contains, eq
from manifold.util.type            import returns, accepts
from manifold.util.callback        import Callback
from manifold.util.log             import Log
from manifold.models.user          import User
from manifold.util.misc            import make_list
from manifold.core.result_value    import ResultValue

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
                            .set(user_query.get_params()).select(fields)
                # user_query.get_timestamp() # timestamp
                # where will be eventually optimized later

                platform = method.get_platform()
                capabilities = metadata.get_capabilities(platform, query.object)

                # XXX Improve platform capabilities support
                if not capabilities.retrieve: continue
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
        # Process this table, which is the root of the 3nf tree
        return AST().union(from_asts, key)

    # metadata == router.g_3nf
    def build(self, query, metadata, allowed_capabilities, user = None,   qp = None):

        # XXX In the current recursive version, we might go far in the
        # XXX recursion to find fields that in fact will be found closer in the
        # XXX next iteration. We should in fact do a BFS. We expect the schema
        # XXX to be finite and small enough so that it does not make a big
        # XXX difference
        analyzed_query = AnalyzedQuery(query, metadata)
        root = metadata.find_node(analyzed_query.get_from())

        # Local fields in root table
        missing_fields  = set()
        missing_fields |= analyzed_query.get_select()
        missing_fields |= analyzed_query.get_where().get_field_names()
        missing_fields |= analyzed_query.get_subquery_names() # only if those subqueries are used XXX

        ast, missing_fields = self.process_subqueries(root, None, analyzed_query, missing_fields, metadata, allowed_capabilities, user, 0)

        if ast:
            ast.optimize_selection(query.get_where())
            #ast.optimize_projection(query.get_select())

        if missing_fields:
            Log.warning("Missing fields: %r" % missing_fields)
        self.ast = ast
        

    def process_query(self, root, query, missing_fields, metadata, allowed_capabilities, user, seen, depth):
        missing_fields_begin = copy.deepcopy(missing_fields)
        # Explore throughout 1-1, and return needed ast beyond 1-N

        added_fields = set()

        # We need root if it provides fields, or connection to needed table
        # Let's start looking at neighbours

        ast = None
        neighbour_ast_predicate_list = []
        if not seen: seen = (root,)
        #if root in seen: return (None, missing_fields, ())
        #seen += (root,)
        
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
            if neighbour in seen:
                continue
            seen += (neighbour,)
            # we expect the set of missing_fields to reduce through exploration
            predicate = relation.get_predicate()
            missing_fields_query = copy.deepcopy(missing_fields)
            missing_fields_query |= predicate.get_value_names()
            _ast, _missing_fields, _relations_1N = self.process_query(neighbour, query, missing_fields_query, metadata, allowed_capabilities, user, seen, depth)
            
            # If the ast is useful, missing_fields should have reduced
            if _missing_fields < missing_fields:
                #added_fields |= predicate.get_value_names()
                missing_fields = _missing_fields
                # if children_ast, we will at least have a minimal _ast to connect them
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
        if ast:
            ast.optimize_projection((missing_fields_begin - missing_fields) | added_fields)

        return (ast, missing_fields, relations_1N)

    def process_subqueries(self, root, predicate, query, missing_fields, metadata, allowed_capabilities, user, depth):
        if depth >= 3:
            return (None, missing_fields) # Nothing found

        ast = None
        relations_1N = ()

        missing_fields_first = copy.deepcopy(missing_fields)

        # *** First pass !
        # We process current_node, as well as the chain of 1..1 nodes, we will also get 1..N nodes in _children_ast
        # At this stage, we have not explored 1N subqueries, so we do not know whether we need an ast to connect them
        # And we cannot analyzed relations 1N because we need to look for missing fields in the local reach first
        _ast, missing_fields, _relations_1N = self.process_query(root, query, missing_fields, metadata, allowed_capabilities, user, None, depth)
        relations_1N += _relations_1N
        
        # The _ast is useful and not only containing the relation predicate
        if _ast:
            ast = _ast

        children_ast_predicate_list  = []

        seen = ()
        subquery_names = set(query.get_subquery_names())
        for root, neighbour, relation in relations_1N:
            if neighbour in seen: continue
            seen += (neighbour,)

            if not missing_fields and not subquery_names: break

            # missing fields should be extended if we go through a link in the query
            # here we know which subqueries are used XXX

            neighbour_predicate = relation.get_predicate()
            relation_name = neighbour_predicate.get_key() # XXX NOT TRUE FOR BACKWARD 1N LINKS
            subquery_names -= set([relation_name])
            sq = query.get_subquery(relation_name)
            missing_fields2  = missing_fields
            if sq:
                missing_fields2 |= sq.get_select()
                missing_fields2 |= sq.get_where().get_field_names()
                #missing_fields2 |= sq.get_subquery_names() # only if those subqueries are used XXX
                query2 = sq

            else:
                query2 = query

            if not missing_fields2: continue

            missing_fields2 |= neighbour_predicate.get_value_names()

            child_ast, missing_fields = self.process_subqueries(neighbour, predicate, query2, missing_fields2, metadata, allowed_capabilities, user, depth+1)
            # XXX test if missing_fields < missing_fields2
            if child_ast:
                children_ast_predicate_list.append((child_ast, relation.get_predicate()))
        
#        if _ast or children_ast_predicate_list:
#            print "SECOND PASS"
#            # *** Second pass
#            # We can now determine which subqueries we need from children_ast_predicate_list
#            # XXX no need for second pass if we already had all connecting fields
#            if predicate:
#                missing_fields_first |= predicate.get_field_names()
#            for _ast, _predicate in children_ast_predicate_list:
#                missing_fields_first |= _predicate.get_field_names()
#            # They reside in the 11 frontier, so we can discard relations_1N here
#            ast, _, _ = self.process_query(root, query, missing_fields_first, metadata, allowed_capabilities, user, None, depth)
        if not ast:
            ast = AST()

        if children_ast_predicate_list:
            children_ast_predicate_list = [ (a.get_root(), p) for a, p in children_ast_predicate_list]
            ast.subquery(children_ast_predicate_list)

        return (ast, missing_fields)




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

    def execute(self, deferred=None):
        cb = Callback(deferred)
        self.ast.set_callback(cb)
        self.ast.start()
        if not deferred:
            results = cb.get_results()
            results = ResultValue.get_result_value(results, self.get_result_value_array())
            return results
        # Formating results in a Callback for asynchronous execution
        deferred.addCallback(lambda results:ResultValue.get_result_value(results, self.get_result_value_array()))
        return deferred

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

