#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An ExploreTask is a Task built when the QueryPlan
# explores the underlying DBGraph.
# 
# QueryPlan class builds, process and executes Queries
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import random
from types                         import StringTypes
from twisted.internet.defer        import Deferred, DeferredList

from manifold.core.ast             import AST
from manifold.core.filter          import Filter
from manifold.core.query           import Query
from manifold.core.stack           import Stack, TASK_11, TASK_1Nsq, TASK_1N
from manifold.operators.demux      import Demux
from manifold.operators.From       import From
from manifold.util.log             import Log
from manifold.util.misc            import is_sublist
from manifold.util.predicate       import Predicate, eq
from manifold.util.type            import returns, accepts

MAX_DEPTH = 3

class ExploreTask(Deferred):
    """
    A pending exploration of the metadata graph
    """

    def __init__(self, interface, root, relation, path, parent, depth):
        """
        Constructor.
        Args:
            root:
            relation: A Relation instance
            path:
            parent:
            depth: An positive integer value, corresponding to the number of
                none 1..1 args traversed from the root Table to the current
        """
        assert root != None, "ExploreTask::__init__(): invalid root = %s" % root

        # Context
        self._interface  = interface
        self.root        = root
        self.relation    = relation
        self.path        = path
        self.parent      = parent
        self.depth       = depth

        # Result
        self.ast         = None
        self.keep_root_a = set()
        self.subqueries  = dict() 

        self.identifier  = random.randint(0, 9999)

        Deferred.__init__(self)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this ExploreTask instance.
        """
        return "(%s --> %s)" % (self.root.get_name(), self.relation)

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this ExploreTask instance.
        """
        return "<ExploreTask %d -- %s -- %s [%d]>" % (self.identifier, self.root.get_name(), self.relation, self.depth)

    def cancel(self):
        self.callback(None)

#DEPRECATED|    @staticmethod
#DEPRECATED|    def prune_from_query(query, found_fields):
#DEPRECATED|        new_fields = query.get_select() - found_fields
#DEPRECATED|        query.select(None).select(new_fields)
#DEPRECATED|        
#DEPRECATED|        old_filter = query.get_where()
#DEPRECATED|        new_filter = Filter()
#DEPRECATED|        for pred in old_filter:
#DEPRECATED|            if pred.get_key() not in found_fields:
#DEPRECATED|                new_filter.add(pred)
#DEPRECATED|        query.filter_by(None).filter_by(new_filter)

    def store_subquery(self, ast, relation):
        #Log.debug(ast, relation)
        if not ast: return
        self.subqueries[relation.get_relation_name()] = (ast, relation)

    def explore(self, stack, missing_fields, metadata, allowed_platforms, allowed_capabilities, user, seen_set, query_plan):
        """
        Explore the metadata graph to find how to serve each queried fields. We
        explore the DBGraph by prior the 1..1 arcs exploration (DFS) by pushing
        one ExploreTask instance in a Stack per 1..1 arc. If some queried
        fields are not yet served, we push in a Stack which 1..N arcs could
        serve them (BFS) (one ExploreTask per out-going 1..N arc). 
        Args:
            stack: A Stack instance where we push new ExploreTask instances. 
            missing_fields: A set of String containing field names (which
                may be prefixed, such has hops.ttl) involved in the Query.
            metadata: The DBGraph instance related to the 3nf graph.
            allowed_platforms: A set of String where each String correponds
                to a queried platform name.
            allowed_platforms: A Capabilities instance.
            user: The User issuing the Query.
            seen_set: A set of String containing the served field names
                involved in the Query.
            query_plan: The QueryPlan instance we're recursively building and
                related to the User Query.
        """
        Log.debug("Search in", self.root.get_name(), "for fields", missing_fields, 'path=', self.path, "SEEN SET =", seen_set)
        Log.tmp("Search in", self.root.get_name(), "for fields", missing_fields, 'path=', self.path, "SEEN SET =", seen_set)
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
#OBSOLETE|            self.ast = self.build_union(self.root, self.keep_root_a, allowed_platforms, metadata, user, query_plan)
            self.ast = self.perform_union(self.root, allowed_platforms, metadata, user, query_plan)

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
                    task = ExploreTask(self._interface, neighbour, relation, subpath, self, self.depth+1)
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
                    task = ExploreTask(self._interface, neighbour, relation, self.path, self.parent, self.depth)
                    task.addCallback(self.perform_left_join, relation, allowed_platforms, metadata, user, query_plan)
                    priority = TASK_11

                deferred_list.append(task)
                stack.push(task, priority)

        DeferredList(deferred_list).addCallback(self.all_done, allowed_platforms, metadata, user, query_plan)

    def all_done(self, result, allowed_platforms, metadata, user, query_plan):
        """

        Args:
            result:
            allowed_platforms: A set of String where each String correponds to a queried platform name.
            metadata: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """

        #Log.debug("DONE", self, result)
        #for (success, value) in result:
        #    if not success:
        #        raise value.trap(Exception)
        #        continue
        try:
            if self.subqueries:
                self.perform_subquery(allowed_platforms, metadata, user, query_plan)
            self.callback(self.ast)
        except Exception, e:
            Log.error("Exception caught in ExploreTask::all_done: %s" % e)
            self.cancel()
            raise e

    def perform_left_join(self, ast, relation, allowed_platforms, metadata, user, query_plan):
        """
        Connect a new AST to the current AST using a LeftJoin Node.
        Args:
            ast: A child AST that must be connected to self.ast using LEFT JOIN .
            relation: The Relation connecting the child Table and the parent Table involved in this LEFT jOIN.
            metadata: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        #Log.debug(ast, relation)
        if not ast: return
        if not self.ast:
#OBSOLETE|            # XXX not sure about fields
#OBSOLETE|            self.ast = self.build_union(self.root, self.root.keys.one().get_field_names(), allowed_platforms, metadata, user, query_plan)
            self.ast = self.perform_union(self.root, allowed_platforms, metadata, user, query_plan)
        self.ast.left_join(ast, relation.get_predicate())

    def perform_subquery(self, allowed_platforms, metadata, user, query_plan):
        """
        Connect a new AST to the current AST using a SubQuery Node.
        If the connected table is "on join", we will use a LeftJoin and a CrossProduct Node instead.
        Args:
            metadata: The DBGraph instance related to the 3nf graph.
            allowed_platforms: A set of String where each String correponds to a queried platform name.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        # We need to build an AST just to collect subqueries
#OBSOLETE|        # XXX metadata not defined
#OBSOLETE|        #if not self.ast:
#OBSOLETE|        #    self.ast = self.build_union(self.root, self.keep_root_a, metadata, user)
#OBSOLETE|        if not self.ast:
#OBSOLETE|            fields = set()
#OBSOLETE|            for _, (_, relation) in self.subqueries.items():
#OBSOLETE|                fields |= relation.get_predicate().get_field_names()
#OBSOLETE|            self.ast = self.build_union(self.root, fields, allowed_platforms, metadata, user, query_plan)
        if not self.ast:
            self.ast = self.perform_union(self.root, allowed_platforms, metadata, user, query_plan)
        
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
                    assert isinstance(key, StringTypes), "Invalid key"
                    assert not isinstance(value, tuple), "Invalid value"
                    xp_key   += (value,)
                    xp_value += (key,)
                    xp_ast_relation.append(ast_relation)
                else:
                    sq_ast_relation.append(ast_relation)

            ast = self.ast

            if sq_ast_relation:
                ast.subquery(sq_ast_relation)
            query = Query.action('get', self.root.get_name()).select(set(xp_key))
            self.ast = AST(self._interface).cross_product(xp_ast_relation, query)
            predicate = Predicate(xp_key, eq, xp_value)

            self.ast.left_join(ast, predicate)
        else:
            self.ast.subquery(self.subqueries.values())

    # TODO rename: perform_union and remove needed_fields parameter
    def perform_union(self, table, allowed_platforms, metadata, user, query_plan):
#OBSOLETE|    def build_union(self, table, needed_fields, allowed_platforms, metadata, user, query_plan):
        """
        Complete a QueryPlan instance by adding an Union of From Node related
        to a same Table.
        Args:
            table: The 3nf Table, potentially provided by several platforms.
            allowed_platforms: A set of String where each String correponds to a queried platform name.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
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
            Log.tmp("TABLE=%r" % table)
            try:
                for method, keys in table.map_method_keys.items():
                    if key in table.map_method_keys[method]: 
                        map_method_bestkey[method] = key 
            except AttributeError:
                map_method_bestkey[table.name] = key

        # For each platform related to the current table, extract the
        # corresponding table and build the corresponding FROM node
        map_method_fields = table.get_annotation()
        for method, fields in map_method_fields.items(): 
            if method.get_name() == table.get_name():
                # The table announced by the platform fits with the 3nf schema
                # Build the corresponding FROM 
                #sub_table = Table.make_table_from_platform(table, fields, method.get_platform())

                # XXX We lack field pruning
                # We create 'get' queries by default, this will be overriden in set_ast
                query = Query.action('get', method.get_name()).select(fields)

                platform = method.get_platform()
                capabilities = metadata.get_capabilities(platform, query.get_from())

                if allowed_platforms and not platform in allowed_platforms:
                    continue

                # The current platform::table might be ONJOIN (no retrieve capability), but we
                # might be able to collect the keys, so we have disabled the following code
                # XXX Improve platform capabilities support
                # XXX if not capabilities.retrieve: continue

                # We need to connect the right gateway
                # XXX

                from_ast = AST(self._interface, user = user).From(platform, query, capabilities, key)
                query_plan.add_from(from_ast.get_root())

                Log.tmp("methods_demux")
                try:
                    if method in table.methods_demux:
                        from_ast.demux().projection(list(fields))
                        demux_node = from_ast.get_root().get_child()
                        assert isinstance(demux_node, Demux), "Bug"
                        map_method_demux[method] = demux_node 
                except AttributeError:
                    pass

            else:
                # The table announced by the platform doesn't fit with the 3nf schema
                # Build a FROMTABLE + DUP(best_key) + SELECT(best_key u {fields}) branch
                # and plug it to the above the DEMUX node referenced in map_method_demux
                # Ask this FROM node for fetching fields
                demux_node = map_method_demux[method]
                from_node = demux_node.get_child()
                key_dup = map_method_bestkey[method]
                select_fields = list(set(fields) | set(key_dup))
                from_node.add_fields_to_query(fields)

                print "FROMTABLE -- DUP(%r) -- SELECT(%r) -- %r -- %r" % (key_dup, select_fields, demux_node, from_node) 

                # Build a new AST (the branch we'll add) above an existing FROM node
                from_ast = AST(self._interface, user = user)
                from_ast.root = demux_node
                Log.warning("ExploreTask: TODO: plug callback")
                #TODO from_node.addCallback(from_ast.callback)

                query_plan.add_from(from_ast.get_root())

                # Add DUP and SELECT to this AST
                from_ast.dup(key_dup).projection(select_fields)
                
            from_asts.append(from_ast)

        # Add the current table in the query plane 
        # Process this table, which is the root of the 3nf tree
        if not from_asts:
            return None
        return AST(self._interface).union(from_asts, key)


