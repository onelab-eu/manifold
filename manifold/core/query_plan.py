#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convert a 3nf-tree into an AST (e.g. a query plan)
# \sa manifold.core.pruned_tree.py
# \sa manifold.core.ast.py
# 
# QueryPlan class builds, process and executes Queries
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loïc Baron        <loic.baron@lip6.fr>

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
        Log.debug(user_query, table, needed_fields)
        from_asts = list()
        key = table.get_keys().one()

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
        Log.tmp(map_method_fields)
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
                Log.tmp(capabilities)
                if not capabilities.retrieve: continue
                from_ast = AST(user = user).From(platform, query, capabilities, key)

                Log.tmp("New from:", from_ast)
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
        if not from_asts:
            return None
        return AST().union(from_asts, key)

    # metadata == router.g_3nf
    def build(self, query, metadata, allowed_capabilities, user = None,   qp = None):

        # XXX In the current recursive version, we might go far in the
        # XXX recursion to find fields that in fact will be found closer in the
        # XXX next iteration. We should in fact do a BFS. We expect the schema
        # XXX to be finite and small enough so that it does not make a big
        # XXX difference
        aq = AnalyzedQuery(query, metadata)
        root = metadata.find_node(aq.get_from())

        # Local fields in root table
        missing_fields  = set() | aq.get_select() | aq.get_where().get_field_names()

        ast, missing_fields, _ = self.process_subqueries(root, None, aq, missing_fields, metadata, allowed_capabilities, user, 0)

        if ast:
            Log.debug("optimize_selection: %r" % query.get_where())
            print "optimize_selection: %r" % query.get_where()
            ast.optimize_selection(query.get_where())
            #ast.optimize_projection(query.get_select())

        if missing_fields:
            Log.warning("Missing fields: %r" % missing_fields)
        self.ast = ast
        

    def process_query(self, root, predicate, query, missing_fields, metadata, allowed_capabilities, user, seen, depth, level):
        """
        This is quite a tricky function so let's detail a bit what we do...
        
        root is interesting if:
        (a) it provides missing_fields
        (b) direct connectivity to subqueries
        (c) it should be traversed to answer the query
        
        (a) and (b) can be verified with root.get_field_names()

        If keep_root_b: we will have to dig into this subquery, but not before
        finishing exploring 1..1 to have the correct missing_fields parameter
        We add them to relations_1N_sq, that will be explored before relations_1N

        Other functions will continue with a reduced set of missing_fields and
        subqueries, otherwise we might search for the same things twice.

        If not sufficient
        1. establish neighbours, classify them as 1..1 or 1..N
        2. go through 1..1 and recursively call the function until we have all fields
        In interesting results, (c) implies we need root
        
        If not sufficient we don't look at 1..N, we first return in case
        there are others 1..1 to explore, we pass 1..N to the parent
        Note: since we still don't know whether we need to connect to any of
        the 1..N relation, the AST we will return might be incomplete, and we
        will have to make a second pass.
        
        if we don't need root: return (None, missing_fields, relations_1N)
        . Build ast with the union of tables in root (remember to ask for
        predicate values, both for parent and children)
        . Complete the ast by joining the list of AST from recursive calls if necessary
        return the AST
        . /!\ parent tables for the hierarchy when assembling query plan
        """
        Log.debug(' '*4*depth, root, query, missing_fields)
        missing_fields_begin = set() | missing_fields

        # we initialize the variable only once, otherwise just look at neighbours
        if not seen: seen = (root,) 

        # Prepare return values (Note: query and missing_fields are being edited in place)
        ast = None
        relations_11, relations_1N, relations_1N_sq = (), (), ()
        # query and missing_fields are being edited in place

        # since we can get them in the parent (unless backwards reference?)
        assert root.keys.one().get_field_names() not in missing_fields, "Requesting key fields in child table"

        root_provided_fields = root.get_field_names()
        keep_root_a = root_provided_fields & missing_fields
        missing_fields -= keep_root_a

        # The next statement is wrong since the relation might not have a field in the root table
        # This is the case for backwards relationships.
        # keep_root_b = root_provided_fields & sq_names
        # sq_names -= keep_root_b
        keep_root_b = set()

        sq_names = query.get_subquery_names() 
         
        # In all cases, we have to list neighbours for returning 1..N relationships. Let's do it now. 
        for neighbour in metadata.graph.successors(root):
            for relation in metadata.get_relations(root, neighbour):
                if relation.get_type() not in [Relation.types.LINK, Relation.types.CHILD, Relation.types.PARENT]:
                    relation_name = relation.get_relation_name()
                    if relation_name in sq_names:
                        keep_root_b.add(relation_name)
                        sq_names.remove(relation_name)
                        relations_1N_sq += ((root, neighbour, relation, query.subquery(relation_name)),)
                        query.remove_subquery(relation_name)
                    else:
                        relations_1N += ((root, neighbour, relation),)
                else:
                    # No need for root since it will be used immediately (or never)
                    relations_11 += ((neighbour, relation),)


        # We might need to continue exploring 1..1 relationships. As we can
        # have cycles in the recursive exploration, we need to maintain a
        # set of tables already explored, the seen variable passed into
        # paramters
        
        # We are building this list to eventually establish the aggregated AST later
        neighbour_ast_predicate_list = []

        # XXX The base table for table cannot be an onjoin table. This occurs when 
        #    1) onjoin,
        #    1) parent 2) onjoin
        
        for neighbour, relation in relations_11:
            if neighbour in seen:
                continue

            # Stop iterating when done
            if not missing_fields and not query.get_subquery_names():
                break

            old_sq_names = query.get_subquery_names()

            # let's recursively determine if this neighbour is useful
            _ast, _missing_fields, query, _relations_1N_sq, _relations_1N = self.process_query(neighbour, predicate, query, missing_fields, metadata, allowed_capabilities, user, seen, depth, level+1)
            
            # First, save apart newly learned 1..N relationships
            relations_1N    += _relations_1N
            relations_1N_sq += _relations_1N_sq

            if not _ast:
                continue 
    
            # The neighbour is useful
            neighbour_ast_predicate_list.append((_ast, predicate))
    
            # The list of missing fields should have reduced
            if not query.get_subquery_names() < old_sq_names:
                assert _missing_fields < missing_fields, "The set of missing fields should have reduced"
            else:
                assert _missing_fields <= missing_fields, "The set of missing fields should have reduced"
            
            missing_fields = _missing_fields

        # Either we have explored all queries, or we have explored all needed ones at this stage

        # For convenience of notations
        keep_root_c = bool(neighbour_ast_predicate_list)

        keep_root = keep_root_a or keep_root_b or keep_root_c
        if not keep_root:
            # Of course:
            #  - ast = None
            #  - missing_fields is unchanged
            #  - we have no relations_1N_sq
            return (None, missing_fields, query, (), relations_1N)

        # Let's build the AST (provided fields + PK for linking to parent ast + FK for children ast)

        queried_fields = set() | keep_root_a
        added_fields   = set()
        if predicate:
            added_fields |= predicate.get_value_names()
        queried_fields |= added_fields
        for _ast, _predicate in neighbour_ast_predicate_list:
            queried_fields |= _predicate.get_field_names()

        if metadata.is_parent(root):
            ast, _ = neighbour_ast_predicate_list.pop()
            ast.dump()
        else:
            ast = self.build_union(query, root, queried_fields, metadata, user)
            
        if not ast:
            # Could not return an AST (lack of capabilities ? priviledges ?)
            Log.warning("Could not build AST because no table for '%r' was available in current platforms" % root)
            # XXX Maybe we should mark the fields as unreachable... otherwise we might find another path that is wrong
            # Anyways, we have already reduced missing_fields...
            return (None, missing_fields, query, set(), set())
        
        # Proceed to joins with (remaining) children
        for _ast, _predicate in neighbour_ast_predicate_list:
            ast.left_join(_ast, _predicate)

        # XXX We need to do it from subquery to keep keys for 1..N relationships
        if ast:
            # Note: we need to keep track of added fields, since we don't have the optimization originating from Subquery
            # This might be done now that we have relation.get_relation_name()
            ast.optimize_projection((missing_fields_begin - missing_fields) | added_fields)

        return (ast, missing_fields, query, relations_1N_sq, relations_1N)

# DEPRECATED #        # Of course we search for missing fields
# DEPRECATED #        query_fields  = set() | missing_fields
# DEPRECATED #        # If we want to use this table, we have to connect it to the parent and thus also collect fields in the predicate
# DEPRECATED #        query_fields |= predicate.get_value_names()
# DEPRECATED #        # We might want to connect subqueries
# DEPRECATED #        query_fields |= query.get_subquery_names()
# DEPRECATED #
# DEPRECATED #        # We need root if it provided missing fields
# DEPRECATED #        keep_root   = not not provided_fields & missing_fields
# DEPRECATED #        # The set of fields we will get from root in the potentially resulting AST
# DEPRECATED #        root_fields = provided_fields & query_fields
# DEPRECATED #
# DEPRECATED #        # If we still need some fields, let's look at neighbours...
# DEPRECATED #        query_fields -= root_fields
# DEPRECATED #        if query_fields
# DEPRECATED #
# DEPRECATED #        root_fields = 
# DEPRECATED #        provided_fields = root.get_field_names()
# DEPRECATED #        
# DEPRECATED #        root_pneed_root = provided_fields & missing_fields
# DEPRECATED #
# DEPRECATED #        query_fields
# DEPRECATED #    
# DEPRECATED #
# DEPRECATED #
# DEPRECATED #
# DEPRECATED #        # OLD CODE
# DEPRECATED #
# DEPRECATED #        missing_fields_begin = copy.deepcopy(missing_fields)
# DEPRECATED #        # Explore throughout 1-1, and return needed ast beyond 1-N
# DEPRECATED #
# DEPRECATED #        added_fields = set()
# DEPRECATED #
# DEPRECATED #        # We need root if it provides fields, or connection to needed table
# DEPRECATED #        # Let's start looking at neighbours
# DEPRECATED #
# DEPRECATED #        ast = None
# DEPRECATED #        neighbour_ast_predicate_list = []
# DEPRECATED #        if not seen: seen = (root,)
# DEPRECATED #        #if root in seen: return (None, missing_fields, ())
# DEPRECATED #        #seen += (root,)
# DEPRECATED #        
# DEPRECATED #        root_fields = set(root.get_field_names())
# DEPRECATED #        unique_fields = root_fields & missing_fields
# DEPRECATED #        if depth > 0:
# DEPRECATED #            # key fields are available in parent, but we will need them for join
# DEPRECATED #            key_fields = root.get_keys().one()
# DEPRECATED #            unique_fields -= key_fields 
# DEPRECATED #
# DEPRECATED #        missing_fields -= unique_fields
# DEPRECATED #        
# DEPRECATED #        relations    = ()
# DEPRECATED #        relations_11 = ()
# DEPRECATED #        relations_1N = ()
# DEPRECATED #
# DEPRECATED #
# DEPRECATED #        for neighbour, relation in relations_11:
# DEPRECATED #            if not missing_fields and not query.get_subquery_names():
# DEPRECATED #                break
# DEPRECATED #            if neighbour in seen:
# DEPRECATED #                continue
# DEPRECATED #            seen += (neighbour,)
# DEPRECATED #            # we expect the set of missing_fields to reduce through exploration
# DEPRECATED #            predicate = relation.get_predicate()
# DEPRECATED #            missing_fields_query = copy.deepcopy(missing_fields)
# DEPRECATED #            missing_fields_query |= predicate.get_value_names()
# DEPRECATED #            _ast, _missing_fields, _relations_1N = self.process_query(neighbour, predicate, query, missing_fields_query, metadata, allowed_capabilities, user, seen, depth)
# DEPRECATED #
# DEPRECATED #            # If the ast is useful, missing_fields should have reduced
# DEPRECATED #            if _missing_fields < missing_fields:
# DEPRECATED #                #added_fields |= predicate.get_value_names()
# DEPRECATED #                missing_fields = _missing_fields
# DEPRECATED #                # if children_ast, we will at least have a minimal _ast to connect them
# DEPRECATED #                neighbour_ast_predicate_list.append((_ast, predicate))
# DEPRECATED #            relations_1N += _relations_1N
# DEPRECATED #
# DEPRECATED #        if unique_fields or neighbour_ast_predicate_list: # we need root
# DEPRECATED #            if depth > 0:
# DEPRECATED #                # We add the key that will be necessary for join
# DEPRECATED #                unique_fields |= root.keys.one().get_names()
# DEPRECATED #            for _ast, _predicate in neighbour_ast_predicate_list:
# DEPRECATED #                unique_fields |= _predicate.get_field_names()
# DEPRECATED #            ast = self.build_union(query, root, unique_fields, metadata, user)
# DEPRECATED #
# DEPRECATED #        if not ast:
# DEPRECATED #            return (None, missing_fields, relations_1N)
# DEPRECATED #            
# DEPRECATED #        for _ast, _predicate in neighbour_ast_predicate_list:
# DEPRECATED #            ast.left_join(_ast, _predicate)
# DEPRECATED #            # XXX Make sure we have all necessary fields for LEFTJOIN
# DEPRECATED #
# DEPRECATED #        # XXX We need to do it from subquery to keep keys for 1..N relationships
# DEPRECATED #        if ast:
# DEPRECATED #            ast.optimize_projection((missing_fields_begin - missing_fields) | added_fields)
# DEPRECATED #
# DEPRECATED #        return (ast, missing_fields, relations_1N)

    def process_subqueries(self, root, predicate, query, missing_fields, metadata, allowed_capabilities, user, depth):
        """
        """
        Log.debug(' '*4*depth, root, query, missing_fields)

        if depth >= 3:
            return (None, missing_fields, query) # Nothing found

        # 1. Can we answer the query only looking at the current depth level
        # We make a first pass, without considering 1..N relationships
        # Eventually, if 1..N relationships prove to be necessary, we might
        # have to make a second pass to connect them

        # The query might be modified in place, let's make a backup
        initial_query = query.copy()

        ast, _missing_fields, query, relations_1N_sq, relations_1N = self.process_query(root, predicate, query, missing_fields, metadata, allowed_capabilities, user, None, depth, 0)
        # We need to remember the fields provided by the 1..1 depth level for second pass
        pass1_fields = missing_fields -_missing_fields
        missing_fields = missing_fields
            
        # Elements of the subquery
        children_ast_relation_list = []

        # We don't expect any duplicate in the 1..N relationships, thanks to normalization


        # Let's first consider 1..N_sq, aka those explicitely specified by the user
        # = explicit subqueries
        for _root, _neighbour, _relation, _sq in relations_1N_sq:
            # We might not need _root, unless this allows more easily to connect the SQ

            # No break since we want to explore _all_ explicit shortcuts
            # Recursive call
            missing_fields_rec = missing_fields | _sq.get_select() | _sq.get_where().get_field_names()
            _ast, missing_fields, query = self.process_subqueries(_neighbour, _relation.get_predicate(), _sq, missing_fields_rec, metadata, allowed_capabilities, user, depth+1)
            if _ast:
                children_ast_relation_list.append((_ast, _relation))

        # Implicit shortcuts for subqueries
        for _root, _neighbour, _relation in relations_1N:
            if not missing_fields and not query.get_subquery_names():
                break

            _ast, missing_fields, query = self.process_subqueries(_neighbour, _relation.get_predicate(), query, missing_fields, metadata, allowed_capabilities, user, depth+1)
            if _ast:
                children_ast_relation_list.append((_ast, _relation))

        if not children_ast_relation_list:
            return (ast, missing_fields, query)

        # We might need a second pass (for sure if not ast), since we have more missing fields. They are the FK to connect subqueries
        # This won't change missing_fields, and we can safely ignore 1..N relations of any kind
        for _ast, _relation in children_ast_relation_list:
            pass2_fields = pass1_fields | _relation.get_predicate().get_field_names()
            ast, _missing_fields, _, _, _ = self.process_query(root, predicate, initial_query, pass2_fields, metadata, allowed_capabilities, user, None, depth, 0)
            assert not _missing_fields, "Missing fields are not expected in pass two (unless build fails)... How to handle ?"
            
        children_ast_relation_list = [ (a.get_root(), p) for a, p in children_ast_relation_list]

        ast.subquery(children_ast_relation_list)

        return (ast, missing_fields, query)

# XXX Note for later: what about holes in the subquery chain. Is there a notion
# of inject ? How do we collect subquery results two or more levels up to match
# the structure (with shortcuts) as requested by the user.

            
# DEPRECATED #        ast = None
# DEPRECATED #        relations_1N = ()
# DEPRECATED #
# DEPRECATED #        missing_fields_first = copy.deepcopy(missing_fields)
# DEPRECATED #
# DEPRECATED #        # *** First pass !
# DEPRECATED #        # We process current_node, as well as the chain of 1..1 nodes, we will also get 1..N nodes in _children_ast
# DEPRECATED #        # At this stage, we have not explored 1N subqueries, so we do not know whether we need an ast to connect them
# DEPRECATED #        # And we cannot analyzed relations 1N because we need to look for missing fields in the local reach first
# DEPRECATED #        _ast, missing_fields, _relations_1N_sq, _relations_1N = self.process_query(root, predicate, query, missing_fields, metadata, allowed_capabilities, user, None, depth)
# DEPRECATED #        relations_1N += _relations_1N
# DEPRECATED #        
# DEPRECATED #        # The _ast is useful and not only containing the relation predicate
# DEPRECATED #        if _ast:
# DEPRECATED #            ast = _ast
# DEPRECATED #
# DEPRECATED #        children_ast_predicate_list  = []
# DEPRECATED #
# DEPRECATED #        seen = ()
# DEPRECATED #        subquery_names = set(query.get_subquery_names())
# DEPRECATED #        for root, neighbour, relation in relations_1N:
# DEPRECATED #            if neighbour in seen: continue
# DEPRECATED #            seen += (neighbour,)
# DEPRECATED #
# DEPRECATED #            if not missing_fields and not subquery_names: break
# DEPRECATED #
# DEPRECATED #            # missing fields should be extended if we go through a link in the query
# DEPRECATED #            # here we know which subqueries are used XXX
# DEPRECATED #
# DEPRECATED #            neighbour_predicate = relation.get_predicate()
# DEPRECATED #            relation_name = neighbour_predicate.get_key() # XXX NOT TRUE FOR BACKWARD 1N LINKS
# DEPRECATED #            subquery_names -= set([relation_name])
# DEPRECATED #            sq = query.get_subquery(relation_name)
# DEPRECATED #            missing_fields2  = missing_fields
# DEPRECATED #            if sq:
# DEPRECATED #                missing_fields2 |= sq.get_select()
# DEPRECATED #                missing_fields2 |= sq.get_where().get_field_names()
# DEPRECATED #                #missing_fields2 |= sq.get_subquery_names() # only if those subqueries are used XXX
# DEPRECATED #                query2 = sq
# DEPRECATED #
# DEPRECATED #            else:
# DEPRECATED #                query2 = query
# DEPRECATED #
# DEPRECATED #            if not missing_fields2: continue
# DEPRECATED #
# DEPRECATED #            missing_fields2 |= neighbour_predicate.get_value_names()
# DEPRECATED #
# DEPRECATED #            child_ast, missing_fields = self.process_subqueries(neighbour, predicate, query2, missing_fields2, metadata, allowed_capabilities, user, depth+1)
# DEPRECATED #            # XXX test if missing_fields < missing_fields2
# DEPRECATED #            if child_ast:
# DEPRECATED #                print "***** childast"
# DEPRECATED #                child_ast.dump(indent=4)
# DEPRECATED #                children_ast_predicate_list.append((child_ast, relation.get_predicate()))
# DEPRECATED #        
# DEPRECATED ##        if _ast or children_ast_predicate_list:
# DEPRECATED ##            print "SECOND PASS"
# DEPRECATED ##            # *** Second pass
# DEPRECATED ##            # We can now determine which subqueries we need from children_ast_predicate_list
# DEPRECATED ##            # XXX no need for second pass if we already had all connecting fields
# DEPRECATED ##            if predicate:
# DEPRECATED ##                missing_fields_first |= predicate.get_field_names()
# DEPRECATED ##            for _ast, _predicate in children_ast_predicate_list:
# DEPRECATED ##                missing_fields_first |= _predicate.get_field_names()
# DEPRECATED ##            # They reside in the 11 frontier, so we can discard relations_1N here
# DEPRECATED ##            ast, _, _ = self.process_query(root, query, missing_fields_first, metadata, allowed_capabilities, user, None, depth)
# DEPRECATED #
# DEPRECATED #        if children_ast_predicate_list:
# DEPRECATED #            children_ast_predicate_list = [ (a.get_root(), p) for a, p in children_ast_predicate_list]
# DEPRECATED #            print ast
# DEPRECATED #            print children_ast_predicate_list
# DEPRECATED #            ast.subquery(children_ast_predicate_list)
# DEPRECATED #
# DEPRECATED #        return (ast, missing_fields)




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
        # create a Callback object with deferred object as arg
        # manifold/util/callback.py 
        cb = Callback(deferred)

        # Start AST = Abstract Syntax Tree 
        # An AST represents a query plan
        # manifold/core/ast.py
        self.ast.set_callback(cb)
        self.ast.start()

        # Not Async, wait for results
        if not deferred:
            results = cb.get_results()
            results = ResultValue.get_result_value(results, self.get_result_value_array())
            return results

        # Async, results sent to a deferred object 
        # Formating results triggered when deferred get results
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

