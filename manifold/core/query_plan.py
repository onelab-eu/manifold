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

import copy, random
from types                         import StringTypes
from twisted.internet.defer        import Deferred, DeferredList

from manifold.core.ast             import AST
from manifold.core.filter          import Filter
from manifold.core.query           import Query
from manifold.core.result_value    import ResultValue
from manifold.core.table           import Table 
from manifold.operators.demux      import Demux
from manifold.util.callback        import Callback
from manifold.util.log             import Log
from manifold.util.misc            import is_sublist
from manifold.util.predicate       import Predicate, eq
from manifold.util.type            import returns, accepts

TASK_11, TASK_1Nsq, TASK_1N = range(0, 3)
MAX_DEPTH = 3

class Stack(object):
    """
    Stack is use to prior some tasks while exploring the 3nf
    graph in order to build the QueryPlan. We first visit the
    tables reachables from the root Table by only traversing
    1..1 arcs. If this is not sufficient to serve the user
    Query, we extend this 3nf-subgraph by adding a part of
    the 3nf graph requiring to traverse 1..N link. This Stack
    allows to order this exploration.

    Stack maintains a set of stacks (one per priority), each
    of these storing a set of ExploreTask. Thus, building
    a QueryPlan only requires one Stack instance.
    - Pushing an ExploreTask in the Stack dispatches it to
    the appropriate nested stack.
    - Poping an ExploreTask extracts the ExploreTask having
    the higher priority.
    """

    def __init__(self, root_task):
        """
        Constructor.
        Args:
            root_task: an ExploreTask, corresponding to the
            3nf graph exploration starting from the root Table.
        """
        self.tasks = {
            TASK_11  : [root_task],
            TASK_1Nsq: [],
            TASK_1N  : [],
        }

    def push(self, task, priority):
        """
        Push an ExploreTask in this Stack.
        Args:
            task: An ExploreTask instance.
            priority: The corresponding priority, which is a
                value among {TASK_11, TASK_1Nsq, TASK_1N}.
        """
        #Log.debug("Adding to stack with priority %d : %r" % (priority, task))
        self.tasks[priority].append(task)

    def pop(self):
        """
        Pop an ExploreTask from this Stack.
        Returns:
            The ExploreTask having the higher priority (if this Stack
            contains at least one ExploreTask), None otherwise.
        """
        for priority in [TASK_11, TASK_1Nsq, TASK_1N]:
            tasks = self.tasks[priority]
            if tasks:
                return tasks.pop(0)
        return None

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif the Stack does not contains any ExploreTask instance.
        """
        return all(map(lambda x: not x, self.tasks.values()))

    def dump(self):
        """
        (Debug function). Dump the ExploreTask embeded in this Stack
        using the logger.
        """
        for priority in [TASK_11, TASK_1Nsq, TASK_1N]:
            Log.tmp("PRIO %d : %r" % (priority, self.tasks[priority]))

class ExploreTask(Deferred):
    """
    A pending exploration of the metadata graph
    """

    def __init__(self, root, relation, path, parent, depth):
        """
        Constructor.
        Args:
            root:
            relation:
            path:
            parent:
            depth: An positive integer value, corresponding to the number of
                none 1..1 args traversed from the root Table to the current
        """
        assert root != None, "ExploreTask::__init__(): invalid root = %s" % root
        # Context
        self.root        = root
        self.relation    = relation
        self.path        = path
        self.parent      = parent
        self.depth       = depth
        # Result
        self.ast = None
        self.keep_root_a = set()
        self.subqueries  = {}

        self.identifier = random.randint(0,9999)

        Deferred.__init__(self)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this ExploreTask instance.
        """
        return "<ExploreTask %d -- %s -- %s [%d]>" % (self.identifier, self.root.get_name(), self.relation, self.depth)

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this ExploreTask instance.
        """
        return self.__repr__()

    def cancel(self):
        self.callback(None)

    @staticmethod
    def prune_from_query(query, found_fields):
        new_fields = query.get_select() - found_fields
        query.select(None).select(new_fields)
        
        old_filter = query.get_where()
        new_filter = Filter()
        for pred in old_filter:
            if pred.get_key() not in found_fields:
                new_filter.add(pred)
        query.filter_by(None).filter_by(new_filter)

    def store_subquery(self, ast, relation):
        #Log.debug(ast, relation)
        if not ast: return
        self.subqueries[relation.get_relation_name()] = (ast, relation)

    def explore(self, stack, missing_fields, metadata, allowed_platforms, allowed_capabilities, user, seen_set, query_plan):
        
        #Log.tmp("Search in ", self.root.get_name(), "for fields", missing_fields, 'path=', self.path, "SEEN SET =", seen_set)
        relations_11, relations_1N, relations_1Nsq = (), {}, {}
        deferred_list = []

        # self.path = X.Y.Z indicates the subqueries we have traversed
        # We are thus able to answer to parts of the query at the root,
        # after X, after X, Z, after X.Y after X.Y.Z, after X.Z, after Y.Z, and
        # X.Y.Z

#DEPRECATED|         # This is to be improved
#DEPRECATED|         # missing points to unanswered parts of the query
#DEPRECATED|         missing_subqueries = []
#DEPRECATED|         def get_query_parts(local_path, query, shortcut=None):
#DEPRECATED|             missing_subqueries.append((query, shortcut))
#DEPRECATED|             for name, sq in query.get_subqueries().items():
#DEPRECATED|                 new_path = local_path + [name]
#DEPRECATED|                 flag, shortcut = is_sublist(self.path, new_path)
#DEPRECATED|                 if flag:
#DEPRECATED|                     get_query_parts(new_path, sq, shortcut)
#DEPRECATED|         get_query_parts([], missing)
#DEPRECATED|         # XXX We could return the shortcut to subquery, to inform on how to
#DEPRECATED|         # process results

        root_provided_fields = self.root.get_field_names()
        root_key_fields = self.root.keys.one().get_field_names()

        # Which fields we are keeping for the current table, and which we are removing from missing_fields
        self.keep_root_a = set()
        for field in root_provided_fields:
            for missing in list(missing_fields):
                # missing has dots inside
                # hops.ttl --> missing_path == ["hops"] missing_field == ["ttl"]
                missing_list = missing.split('.')
                missing_path, (missing_field,) = missing_list[:-1], missing_list[-1:]
                flag, shortcut = is_sublist(missing_path, self.path) #self.path, missing_path)

                if flag and missing_field == field:
                    #print 'current table provides missing field PATH=', self.path, 'field=', field, 'missing=', missing
                    self.keep_root_a.add(field)

                    # We won't search those fields in subsequent explorations,
                    # unless the belong to the key of an ONJOIN table
                    is_onjoin = self.root.capabilities.is_onjoin()
                    
                    if not is_onjoin or field not in root_key_fields:
                        missing_fields.remove(missing)
                    
        assert self.depth == 1 or root_key_fields not in missing_fields, "Requesting key fields in child table"

        if self.keep_root_a:
            # XXX NOTE that we have built an AST here without taking into account fields for the JOINs and SUBQUERIES
            # It might not pose any problem though if they come from the optimization phase
            self.ast = self.build_union(self.root, self.keep_root_a, allowed_platforms, metadata, user, query_plan)

        if self.depth == MAX_DEPTH:
            self.callback(self.ast)
            return

        # In all cases, we have to list neighbours for returning 1..N relationships. Let's do it now. 
        for neighbour in metadata.graph.successors(self.root):
            for relation in metadata.get_relations(self.root, neighbour):
                name = relation.get_relation_name()

                if name in seen_set:
                    continue

                seen_set.add(name)

                if relation.requires_subquery():
                    subpath = self.path[:]
                    subpath.append(name)
                    task = ExploreTask(neighbour, relation, subpath, self, self.depth+1)
                    task.addCallback(self.store_subquery, relation)

                    relation_name = relation.get_relation_name()

                    # The relation has priority if at least one field is like PATH.relation.xxx
                    priority = TASK_1N
                    for missing in missing_fields:
                        if missing.startswith("%s.%s." % (self.path, relation.get_relation_name())):
                            priority = TASK_1Nsq
                            break
                    #priority = TASK_1Nsq if relation_name in missing_subqueries else TASK_1N
                    
                else:
                    task = ExploreTask(neighbour, relation, self.path, self.parent, self.depth)
                    task.addCallback(self.perform_left_join, relation, allowed_platforms, metadata, user, query_plan)
                    priority = TASK_11

                deferred_list.append(task)
                stack.push(task, priority)

        DeferredList(deferred_list).addCallback(self.all_done, allowed_platforms, metadata, user, query_plan)

    def all_done(self, result, allowed_platforms, metadata, user, query_plan):
        #Log.debug("DONE", self, result)
        #for (success, value) in result:
        #    if not success:
        #        raise value.trap(Exception)
        #        continue
        if self.subqueries:
            self.perform_subquery(allowed_platforms, metadata, user, query_plan)
        self.callback(self.ast)

    def perform_left_join(self, ast, relation, allowed_platforms, metadata, user, query_plan):
        """
        Connect a new AST to the current AST using a LeftJoin Node.
        Args:
            ast: A child AST that must be connected to self.ast using LEFT JOIN 
            relation: The Relation connecting the child Table and the parent Table involved in this LEFT jOIN.
            metadata: The DBGraph instance related to the 3nf graph
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query
        """
        #Log.debug(ast, relation)
        if not ast: return
        if not self.ast:
            # XXX not sure about fields
            self.ast = self.build_union(self.root, self.root.keys.one().get_field_names(), allowed_platforms, metadata, user, query_plan)
        self.ast.left_join(ast, relation.get_predicate())

    def perform_subquery(self, allowed_platforms, metadata, user, query_plan):
        # We need to build an AST just to collect subqueries
        # XXX metadata not defined
        #if not self.ast:
        #    self.ast = self.build_union(self.root, self.keep_root_a, metadata, user)
        if not self.ast:
            fields = set()
            for _, (_, relation) in self.subqueries.items():
                fields |= relation.get_predicate().get_field_names()
            self.ast = self.build_union(self.root, fields, allowed_platforms, metadata, user, query_plan)
        
        if self.root.capabilities.is_onjoin():
            # Let's identify tables involved in the key
            root_key_fields = self.root.keys.one().get_field_names()
            xp_ast_relation, sq_ast_relation = [], []
            xp_key = ()
            xp_value = ()
            for name, ast_relation in self.subqueries.items():
                if name in root_key_fields:
                    ast, relation = ast_relation
                    key, _, value = relation.get_predicate().get_tuple()
                    xp_key   += (value,)
                    xp_value += (key,)
                    xp_ast_relation.append(ast_relation)
                else:
                    sq_ast_relation.append(ast_relation)

            ast = self.ast

            if sq_ast_relation:
                ast.subquery(sq_ast_relation)
            q = Query.action('get', self.root.get_name()).select(set(xp_key))
            self.ast = AST().cross_product(xp_ast_relation, q)
            predicate = Predicate(xp_key, eq, xp_value)

            self.ast.left_join(ast, predicate)
        else:
            self.ast.subquery(self.subqueries.values())

    def build_union(self, table, needed_fields, allowed_platforms, metadata, user, query_plan):
        Log.debug(table, needed_fields)
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
                # We create 'get' queries by default, this will be overriden in set_ast
                query = Query.action('get', method.get_name()).select(fields)

                platform = method.get_platform()
                capabilities = metadata.get_capabilities(platform, query.object)

                if not platform in allowed_platforms:
                    continue

                # The platform might be ONJOIN (no retrieve capability), but we
                # might be able to collect the keys, so we have disabled the following code
                # XXX Improve platform capabilities support
                # XXX if not capabilities.retrieve: continue

                from_ast = AST(user = user).From(platform, query, capabilities, key)

                query_plan.froms.append(from_ast.root)

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
                #TODO from_node.addCallback(from_ast.callback)

                query_plan.froms.append(from_ast.root)

                # Add DUP and SELECT to this AST
                from_ast.dup(key_dup).projection(select_fields)
                
            from_asts.append(from_ast)

        # Add the current table in the query plane 
        # Process this table, which is the root of the 3nf tree
        if not from_asts:
            return None
        return AST().union(from_asts, key)

class QueryPlan(object):
    """
    Building a query plan consists in setting the AST and the list of Froms.
    """

    def __init__(self):
        """
        Constructor.
        """
        # TODO metadata, user should be a property of the query plan
        self.ast = AST()
        self.froms = []

    def add_from(from_node):
        """
        Add a From node to the query plan. FromTable Node are not stored
        in self.froms.
        """
        if isinstance(fromnode, From):
            self.froms.append(fromnode)

    def get_result_value_array(self):
        # Iterate over gateways to get their result values
        # XXX We might need tasks
        result = []
        for from_node in self.froms:
            # If no Gateway 
            if not from_node.gateway: continue
            result.extend(from_node.gateway.get_result_value())
        return result

    def inject_at(self, query):
        """
        Update From Nodes of the QueryPlan in order to take into account AT
        clause involved in a user Query.
        Args:
            query: The Query issued by the user.
        """
        Log.warning("HARDCODED: AT injection in FROM Nodes: %r" % self.froms)
        for from_node in self.froms:
            from_node.query.timestamp = query.get_timestamp()

    def set_ast(self, ast, query):
        """
        Complete an AST in order to take into account SELECT and WHERE clauses
        involved in a user Query.
        Args:
            ast: An AST instance made of Union, LeftJoin, SubQuery and From Nodes.
            query: The Query issued by the user.
        """
        ast.optimize(query)
        self.inject_at(query)
        self.ast = ast
    
        # Update the main query to add applicative information such as action and params
        # NOTE: I suppose params cannot have '.' inside
        for from_node in self.froms:
            q = from_node.get_query()
            if q.get_from() == query.get_from():
                q.action = query.get_action()
                q.params = query.get_params()

    def build(self, query, metadata, allowed_platforms, allowed_capabilities, user = None):
        """
        Build the QueryPlan involving several Gateways according to a 3nf
        graph and a user Query. If only one Gateway is involved, you should
        use QueryPlan::build_simple.
        Args:
            query: The Query issued by the user.
            metadata: The 3nf graph (DBGraph instance).
            allowed_platforms: A list of platform names (list of String).
            allowed_capabilities: A Capabilities instance or None.
            user: A User instance or None.
        """
        root = metadata.find_node(query.get_from())
        if not root:
            Log.error("query_plan::build(): Cannot find %s in metadata, known tables are %s" % (query.get_from(), metadata.get_table_names()))
        
        root_task = ExploreTask(root, relation=None, path=[], parent=self, depth=1)
        root_task.addCallback(self.set_ast, query)

        stack = Stack(root_task)
        seen = {} # path -> set()

        missing_fields  = set()
        missing_fields |= query.get_select()
        missing_fields |= query.get_where().get_field_names()

        while missing_fields:
            task = stack.pop()
            if not task:
                Log.warning("Exploration terminated without finding fields: %r" % missing_fields)
                break

            pathstr = '.'.join(task.path)
            if not pathstr in seen:
                seen[pathstr] = set()
            task.explore(stack, missing_fields, metadata, allowed_platforms, allowed_capabilities, user, seen[pathstr], query_plan = self)

        while not stack.is_empty():
            task = stack.pop()
            task.cancel()
    
        # Do we need to wait for self.ast here ?

    # XXX Note for later: what about holes in the subquery chain. Is there a notion
    # of inject ? How do we collect subquery results two or more levels up to match
    # the structure (with shortcuts) as requested by the user.

    def build_simple(self, query, metadata, allowed_capabilities):
        """
        Builds a QueryPlan (self) related to a single Gateway.
        If several Gateways are involved, you must use QueryPlan::build.
        Args:
            query: The Query issued by the user.
            metadata:
            allowed_capabilities: The Capabilities related to this Gateway.
        """
        # XXX allowed_capabilities should be a property of the query plan !

        # XXX Check whether we can answer query.object


        # Here we assume we have a single platform
        platform = metadata.keys()[0]
        announce = metadata[platform][query.get_from()] # eg. table test
        
        # Set up an AST for missing capabilities (need configuration)

        # Selection ?
        if query.filters and not announce.capabilities.selection:
            if not allowed_capabilities.selection:
                raise Exception, 'Cannot answer query: SELECTION'
            add_selection = query.filters
            query.filters = Filter()
        else:
            add_selection = None

        # Projection ?
        announce_fields = announce.get_table().get_fields()
        if query.fields < announce_fields and not announce.capabilities.projection:
            if not allowed_capabilities.projection:
                raise Exception, 'Cannot answer query: PROJECTION'
            add_projection = query.fields
            query.fields = set()
        else:
            add_projection = None

        table = Table({platform:''}, {}, query.get_from(), set(), set())
        key = metadata.get_key(query.get_from())
        capabilities = metadata.get_capabilities(platform, query.get_from())
        self.ast = self.ast.From(table, query, capabilities, key)

        # XXX associate the From node to the Gateway
        from_node = self.ast.root
        self.add_from(from_node)
        #from_node.set_gateway(gw_or_router)
        #gw_or_router.query = query

        if not self.root:
            return
        if add_selection:
            self.ast.optimize_selection(add_selection)
        if add_projection:
            self.ast.optimize_projection(add_projection)

        self.inject_at(query)

    def execute(self, deferred = None):
        """
        Execute the QueryPlan in order to query the appropriate
        sources of data, collect, combine and returns the records
        requested by the user.
        Args:
            deferred: may be set to None.
        Returns:
            
        """
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
        Log.tmp(deferred)
        return deferred

    def dump(self):
        """
        Dump this AST to the standard output.
        """
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

