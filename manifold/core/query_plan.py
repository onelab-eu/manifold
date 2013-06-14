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

from twisted.internet.defer import Deferred

class Stack(object):
    def __init__(self, root):
        self.tasks_11   = [root]
        self.tasks_1Nsq = []
        self.tasks_1N   = []

    def push_11(self, task):   self.tasks_11.append(task)
    def push_1Nsq(self, task): self.tasks_1Nsq.append(task)
    def push_1N(self, task):   self.tasks_1N.append(task)

    def push(self, tasks_11, tasks_1Nsq, tasks_1N):
        self.push_11(tasks_11)
        self.push_1Nsq(tasks_1Nsq)
        self.push_1N(tasks_1N)

    def pop(self):
        if self.tasks_11:
            return self.tasks_11.pop()
        elif self.tasks_1Nsq:
            return self.tasks_1Nsq.pop()
        else:
            return self.tasks_1N.pop()

def build():
    stack = Stack(root)
    while missing and sq_names:
        task = stack.pop()
        missing, query, tasks_11, tasks_1Nsq, tasks_1N = task.explore(missing, query) # Not clear
        stack.push(tasks_11, tasks_1Nsq, tasks_1N)
    # We pop the stack and mark all tasks as useless !
    # XXX

# Still unused
class ExplorationStatus(object);
    def __init__(self):
        self.query = None

class ExploreTask(defer.Deferred):
    """
    A pending exploration of the metadata graph
    """
    def __init__(self):
        Deferred.__init__(self):
        # XXX parameters for exploration

class ExploreTask11(ExploreTask):
    def __init__(self, root, predicate, query, missing_fields, metadata, allowed_capabilities, user, seen, depth, level):
        assert root, "Invalid root object check whether Manifold has fetched metadata concerning '%s'" % query.get_from()

        Log.debug(" " * 4 * depth, root, predicate, query, missing_fields)
        missing_fields_begin = set() | missing_fields
        predicate_begin = copy.deepcopy(predicate)

        # we initialize the variable only once, otherwise just look at neighbours
        if not seen: seen = (root,) 

        # Prepare return values (Note: query and missing_fields are being edited in place)
        ast = None
        relations_11, relations_1N, relations_1N_sq = (), {}, {}
        # query and missing_fields are being edited in place

        # since we can get them in the parent (unless backwards reference?)
        assert root.keys.one().get_field_names() not in missing_fields, "Requesting key fields in child table"

        root_provided_fields = root.get_field_names()
        if root.get_name() == 'traceroute': # not root.capabilities.retrieve:
            Log.warning("HARDCODED TRACEROUTE AS ONJOIN")
            # Keys fields need to be provided by subqueries (and these will be executed before)
            root_key_fields = root.keys.one().get_field_names()
            print "REMOVED FROM root_provided_fields", root_key_fields
            root_provided_fields -= root_key_fields
            # XXX Do we need to prevent these fields to be found in the 1..1 related tables ?
        print "ROOT PROVIDED FIELDS ==", root_provided_fields
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
                if relation.get_type() not in [Relation.types.LINK, Relation.types.CHILD, Relation.types.PARENT]: # direct 1..1
                    relation_name = relation.get_relation_name()
                    if relation_name in sq_names:
                        keep_root_b.add(relation_name)
                        sq_names.remove(relation_name)
                        relations_1N_sq[relation_name] = (root, neighbour, relation, query.subquery(relation_name))
                        query.remove_subquery(relation_name)
                    else:
                        relations_1N[relation_name] = (root, neighbour, relation)
                else:
                    # No need for root since it will be used immediately (or never)
                    relations_11 += ((neighbour, relation),)


        # We create an exploration task for each of the 11 relationships
        # We will go through tasks and store further tasks until they give all answers or we reach a stopping condition.
        # Task will answer when it is sure it does not need further exploration
        

        # We might need to continue exploring 1..1 relationships. As we can
        # have cycles in the recursive exploration, we need to maintain a
        # set of tables already explored, the seen variable passed into
        # paramters
        
        # We are building this list to eventually establish the aggregated AST later
        neighbour_ast_predicate_list = []

        # XXX The base table for table cannot be an onjoin table. This occurs when 
        #    1) onjoin,
        #    1) parent 2) onjoin
        
        for _neighbour, _relation in relations_11:
            if _neighbour in seen:
                continue

            # Stop iterating when done
            if not missing_fields and not query.get_subquery_names():
                break

            old_sq_names = query.get_subquery_names()

            # let's recursively determine if this neighbour is useful
            missing_fields_before = set() | missing_fields
            _predicate= _relation.get_predicate()
            _ast, missing_fields, query, _relations_1N_sq, _relations_1N = self.process_query(_neighbour, _predicate, query, missing_fields, metadata, allowed_capabilities, user, seen, depth, level+1)
            
            # First, save apart newly learned 1..N relationships
            relations_1N.update(_relations_1N)
            relations_1N_sq.update(_relations_1N_sq)

            if not _ast:
                continue 
    
            # The neighbour is useful
            neighbour_ast_predicate_list.append((_ast, _predicate))
    
            # The list of missing fields should have reduced
            if not query.get_subquery_names() < old_sq_names:
                assert missing_fields < missing_fields_before, "The set of missing fields should have reduced (1)"
            else:
                assert missing_fields <= missing_fields_before, "The set of missing fields should have reduced (2)"
            
        # Either we have explored all queries, or we have explored all needed ones at this stage

        # For convenience of notations
        keep_root_c = bool(neighbour_ast_predicate_list)

        keep_root = keep_root_a or keep_root_b or keep_root_c
        if not keep_root:
            # Of course:
            #  - ast = None
            #  - missing_fields is unchanged
            #  - we have no relations_1N_sq
            return (None, missing_fields, query, {}, relations_1N)

        # Let's build the AST (provided fields + PK for linking to parent ast + FK for children ast)

        queried_fields = set() | keep_root_a
        added_fields   = set()
        if predicate:
            print "root=", root.get_name(), "predicate=", predicate, "predicate begin=", predicate_begin
            print "added", predicate.get_value_names(), "for connecting upper instances"
            added_fields |= predicate.get_value_names()
        queried_fields |= added_fields
        for _ast, _predicate in neighbour_ast_predicate_list:
            queried_fields |= _predicate.get_field_names()

        # if we are a parent, and already need to retrieve children, then we can skip the parent
        if not metadata.is_parent(root) and neighbour_ast_predicate_list:
            ast, _ = neighbour_ast_predicate_list.pop()
            added_fields |= keep_root_a 
            # we need to add fields from the parent (that we will find in the child also) to be sure the projection will work nicely
            # XXX we might not have asked all fields to the children since they were in the parent
        else:
            ast = self.build_union(query, root, queried_fields, metadata, user)
            
        if not ast:
            # Could not return an AST (lack of capabilities ? priviledges ?)
            Log.warning("Could not build AST because no table for '%r' was available in current platforms" % root)
            # XXX Maybe we should mark the fields as unreachable... otherwise we might find another path that is wrong
            # Anyways, we have already reduced missing_fields...
            return (None, missing_fields, query, (), ())
        
        # Proceed to joins with (remaining) children
        for _ast, _predicate in neighbour_ast_predicate_list:
            ast.left_join(_ast, _predicate)

        # XXX We need to do it from subquery to keep keys for 1..N relationships
        if ast:
            # Note: we need to keep track of added fields, since we don't have the optimization originating from Subquery
            # This might be done now that we have relation.get_relation_name()
            Log.tmp("missing_fields_begin - missing_fields", missing_fields_begin - missing_fields)
            Log.tmp("added fields", added_fields)
            ast.optimize_projection((missing_fields_begin - missing_fields) | added_fields)

        return (ast, missing_fields, query, relations_1N_sq, relations_1N)
    
    def explore(self, missing_fields, query):
        # Initializations
        root, predicate = self.root, self.predicate

        relations_11, relations_1N, relations_1N_sq = (), {}, {}

        # Should return:
        # A promise for an AST
        # The set of missing fields after exploration
        # Parts of the query remaining to be answered
        # Available 11, 1Nsq and 1N through me (and me only)

    def done_callback(self):
        # Trigger callback to wake up people relying on me
        self.callback(ast)

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

                # The platform might be ONJOIN (no retrieve capability), but we
                # might be able to collect the keys, so we have disabled the following code
                # XXX Improve platform capabilities support
                # XXX if not capabilities.retrieve: continue

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
            ast.optimize_selection(query.get_where())
            ast.optimize_projection(query.get_select())

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
        # TODO handle properly query related to an unknown object
        assert root, "Invalid root object check whether Manifold has fetched metadata concerning '%s'" % query.get_from()

        Log.debug(" " * 4 * depth, root, predicate, query, missing_fields)
        missing_fields_begin = set() | missing_fields
        predicate_begin = copy.deepcopy(predicate)

        # we initialize the variable only once, otherwise just look at neighbours
        if not seen: seen = (root,) 

        # Prepare return values (Note: query and missing_fields are being edited in place)
        ast = None
        relations_11, relations_1N, relations_1N_sq = (), {}, {}
        # query and missing_fields are being edited in place

        # since we can get them in the parent (unless backwards reference?)
        assert root.keys.one().get_field_names() not in missing_fields, "Requesting key fields in child table"

        root_provided_fields = root.get_field_names()
        if root.get_name() == 'traceroute': # not root.capabilities.retrieve:
            Log.warning("HARDCODED TRACEROUTE AS ONJOIN")
            # Keys fields need to be provided by subqueries (and these will be executed before)
            root_key_fields = root.keys.one().get_field_names()
            print "REMOVED FROM root_provided_fields", root_key_fields
            root_provided_fields -= root_key_fields
            # XXX Do we need to prevent these fields to be found in the 1..1 related tables ?
        print "ROOT PROVIDED FIELDS ==", root_provided_fields
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
                if relation.get_type() not in [Relation.types.LINK, Relation.types.CHILD, Relation.types.PARENT]: # direct 1..1
                    relation_name = relation.get_relation_name()
                    if relation_name in sq_names:
                        keep_root_b.add(relation_name)
                        sq_names.remove(relation_name)
                        relations_1N_sq[relation_name] = (root, neighbour, relation, query.subquery(relation_name))
                        query.remove_subquery(relation_name)
                    else:
                        relations_1N[relation_name] = (root, neighbour, relation)
                else:
                    # No need for root since it will be used immediately (or never)
                    relations_11 += ((neighbour, relation),)


        # We create an exploration task for each of the 11 relationships
        # We will go through tasks and store further tasks until they give all answers or we reach a stopping condition.
        # Task will answer when it is sure it does not need further exploration
        

        # We might need to continue exploring 1..1 relationships. As we can
        # have cycles in the recursive exploration, we need to maintain a
        # set of tables already explored, the seen variable passed into
        # paramters
        
        # We are building this list to eventually establish the aggregated AST later
        neighbour_ast_predicate_list = []

        # XXX The base table for table cannot be an onjoin table. This occurs when 
        #    1) onjoin,
        #    1) parent 2) onjoin
        
        for _neighbour, _relation in relations_11:
            if _neighbour in seen:
                continue

            # Stop iterating when done
            if not missing_fields and not query.get_subquery_names():
                break

            old_sq_names = query.get_subquery_names()

            # let's recursively determine if this neighbour is useful
            missing_fields_before = set() | missing_fields
            _predicate= _relation.get_predicate()
            _ast, missing_fields, query, _relations_1N_sq, _relations_1N = self.process_query(_neighbour, _predicate, query, missing_fields, metadata, allowed_capabilities, user, seen, depth, level+1)
            
            # First, save apart newly learned 1..N relationships
            relations_1N.update(_relations_1N)
            relations_1N_sq.update(_relations_1N_sq)

            if not _ast:
                continue 
    
            # The neighbour is useful
            neighbour_ast_predicate_list.append((_ast, _predicate))
    
            # The list of missing fields should have reduced
            if not query.get_subquery_names() < old_sq_names:
                assert missing_fields < missing_fields_before, "The set of missing fields should have reduced (1)"
            else:
                assert missing_fields <= missing_fields_before, "The set of missing fields should have reduced (2)"
            
        # Either we have explored all queries, or we have explored all needed ones at this stage

        # For convenience of notations
        keep_root_c = bool(neighbour_ast_predicate_list)

        keep_root = keep_root_a or keep_root_b or keep_root_c
        if not keep_root:
            # Of course:
            #  - ast = None
            #  - missing_fields is unchanged
            #  - we have no relations_1N_sq
            return (None, missing_fields, query, {}, relations_1N)

        # Let's build the AST (provided fields + PK for linking to parent ast + FK for children ast)

        queried_fields = set() | keep_root_a
        added_fields   = set()
        if predicate:
            print "root=", root.get_name(), "predicate=", predicate, "predicate begin=", predicate_begin
            print "added", predicate.get_value_names(), "for connecting upper instances"
            added_fields |= predicate.get_value_names()
        queried_fields |= added_fields
        for _ast, _predicate in neighbour_ast_predicate_list:
            queried_fields |= _predicate.get_field_names()

        # if we are a parent, and already need to retrieve children, then we can skip the parent
        if not metadata.is_parent(root) and neighbour_ast_predicate_list:
            ast, _ = neighbour_ast_predicate_list.pop()
            added_fields |= keep_root_a 
            # we need to add fields from the parent (that we will find in the child also) to be sure the projection will work nicely
            # XXX we might not have asked all fields to the children since they were in the parent
        else:
            ast = self.build_union(query, root, queried_fields, metadata, user)
            
        if not ast:
            # Could not return an AST (lack of capabilities ? priviledges ?)
            Log.warning("Could not build AST because no table for '%r' was available in current platforms" % root)
            # XXX Maybe we should mark the fields as unreachable... otherwise we might find another path that is wrong
            # Anyways, we have already reduced missing_fields...
            return (None, missing_fields, query, (), ())
        
        # Proceed to joins with (remaining) children
        for _ast, _predicate in neighbour_ast_predicate_list:
            ast.left_join(_ast, _predicate)

        # XXX We need to do it from subquery to keep keys for 1..N relationships
        if ast:
            # Note: we need to keep track of added fields, since we don't have the optimization originating from Subquery
            # This might be done now that we have relation.get_relation_name()
            Log.tmp("missing_fields_begin - missing_fields", missing_fields_begin - missing_fields)
            Log.tmp("added fields", added_fields)
            ast.optimize_projection((missing_fields_begin - missing_fields) | added_fields)

        return (ast, missing_fields, query, relations_1N_sq, relations_1N)

    def process_subqueries(self, root, predicate, query, missing_fields, metadata, allowed_capabilities, user, depth):
        """
        """
        Log.debug(' '*4*depth, root, predicate, query, missing_fields)

        if depth >= 3:
            return (None, missing_fields, query) # Nothing found

        # 1. Can we answer the query only looking at the current depth level
        # We make a first pass, without considering 1..N relationships
        # Eventually, if 1..N relationships prove to be necessary, we might
        # have to make a second pass to connect them

        # The query might be modified in place, let's make a backup
        initial_query = query.copy()
        missing_fields_before = copy.copy(missing_fields)

        # FIRST PASS
        # ==========
            

        # XXX relations 1..N will now be returned as a dict indexed by name
        ast, _missing_fields, query, relations_1N_sq, relations_1N = self.process_query(root, predicate, query, missing_fields, metadata, allowed_capabilities, user, None, depth, 0)
        if ast:
            print "RETURNED AST="
            ast.dump()
        else:
            print "returned NONE"


        # In case we have an onjoin table, we expect missing key fields will be found in 1.N tables in the remaining code

        # We need to remember the fields provided by the 1..1 depth level for second pass
        pass1_fields = missing_fields_before - _missing_fields 
        missing_fields = _missing_fields
            
        # Elements of the subquery
        children_ast_relation_map = {}

        # We don't expect any duplicate in the 1..N relationships, thanks to normalization


        # Let's first consider 1..N_sq, aka those explicitely specified by the user
        # = explicit subqueries
        print "[[ EXPLICIT SUBQUERIES ]]"
        for _name, (_root, _neighbour, _relation, _sq) in relations_1N_sq.items():
            # We might not need _root, unless this allows more easily to connect the SQ

            # No break since we want to explore _all_ explicit shortcuts
            # Recursive call
            missing_fields_rec = missing_fields | _sq.get_select() | _sq.get_where().get_field_names()
                
            # We move filters of fields still missing (shortcuts) to the subquery
            #for p in query.filters:
            #    if p.get_key in missing_fields_rec:
            #        print "FORWARDING PREDICATE TO SUBQUERY", p
            #        _sq.select(p)

            _ast, missing_fields, query = self.process_subqueries(_neighbour, _relation.get_predicate(), _sq, missing_fields_rec, metadata, allowed_capabilities, user, depth+1)
            if _ast:
                children_ast_relation_map[_name] = (_ast, _relation)

        print "[[ IMPLICIT SHORTCUTS ]]"
        # Implicit shortcuts for subqueries
        for _name, (_root, _neighbour, _relation) in relations_1N.items():
            if not missing_fields and not query.get_subquery_names():
                break

            _ast, missing_fields, query = self.process_subqueries(_neighbour, _relation.get_predicate(), query, missing_fields, metadata, allowed_capabilities, user, depth+1)
            if _ast:
                children_ast_relation_map[_name] = (_ast, _relation)

        #if ast:
        #    # Here we need to proceed to filtering before the query is ever returned (we might have children or not)
        #    # only on fields that have been found in the ast
        #    try:
        #        f = Filter()
        #        for p in initial_query.get_where():
        #            if not p.get_key() in missing_fields:
        #                f.add(p)
        #        ast.optimize_selection(f)
        #        print "AFTER OPTIMIZE SELECTIOn"
        #        ast.dump()
        #    except Exception, e:
        #        print "EEE/", e
        

        if not children_ast_relation_map:
            Log.debug(['#', '=', '-', '.'][depth]*80)
            return (ast, missing_fields, query)

        print "AST FROM PASS1 (pass1 fields=", pass1_fields,")"
        ast.dump()

        # SECOND PASS
        # ===========
        # We might need a second pass (for sure if not ast), since we have more missing fields. They are the FK to connect subqueries
        # This won't change missing_fields, and we can safely ignore 1..N relations of any kind
        pass2_fields = pass1_fields # XXX should be done by  process_query below # | predicate.get_field_names()
        for _name, (_ast, _relation) in children_ast_relation_map.items():
            pass2_fields |= _relation.get_predicate().get_field_names()
        ast, _missing_fields, _, _, _ = self.process_query(root, predicate, initial_query, pass2_fields, metadata, allowed_capabilities, user, None, depth, 0)
        assert not _missing_fields, "Missing fields are not expected in pass two (unless build fails)... How to handle ?"
            
        children_ast_relation_map = dict([ (_name, (a.get_root(), p)) for _name, (a, p) in children_ast_relation_map.items() ])

        # We are now to assemble parent query and subqueries
        # If the parent is on join, we might have to run some of the child queries before 
        if not root.capabilities.retrieve:
            # Let's split subqueries in two groups
            xproduct_child  = []
            remaining_child = []
            # according to root key fields (need to take predicate into account XXX)
            root_key_fields = root.keys.one().get_field_names()
            Log.tmp("  > root_key_fields=", root_key_fields)
            
            for _name, (_ast, _relation) in children_ast_relation_map.items():
                print "LOOP relation=", _name
                if _name in root_key_fields:
                    xproduct_child.append((_ast, _relation))
                    root_key_fields.remove(_name)
                else:
                    remaining_child.append((_ast, _relation))

            print "MISSING RELATIONS", root_key_fields
            for name in root_key_fields:
                # We need to issue additional subqueries to retrieve key fields only
                table = metadata.find_node(name)
                # Maybe we already tried and failed
                # We will have to connect those tables
                raise Exception, "Additional tables not implemented"
                xproduct_child.append(XXX)


            Log.tmp("CROSS PRODUCT TABLES", xproduct_child)
            new_ast = AST().cross_product(xproduct_child)

            Log.tmp("REMAINING IN SUBQUERIES", remaining_child)
            if remaining_child:
                # We should have hops here
                ast.subquery(remaining_child)
                ast.dump()

            Log.tmp("performing left join")
            new_ast.left_join(ast, None) # XXX predicate
            new_ast.dump()

            Log.tmp("FINAL AST")
            ast = new_ast
            ast.dump()

        else:
            ast.subquery(children_ast_relation_map)

        Log.debug(['#', '=', '-', '.'][depth]*80)
        return (ast, missing_fields, query)

# XXX Note for later: what about holes in the subquery chain. Is there a notion
# of inject ? How do we collect subquery results two or more levels up to match
# the structure (with shortcuts) as requested by the user.

            
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

