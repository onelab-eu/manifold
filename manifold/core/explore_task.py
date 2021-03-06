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
from manifold.core.relation        import Relation # ROUTERV2
from manifold.core.stack           import Stack, TASK_11, TASK_1Nsq, TASK_1N
#DEPRECATED|LOIC|#from manifold.operators.demux      import Demux
from manifold.operators.From       import From
#from manifold.operators.rename     import Rename
from manifold.types                import BASE_TYPES
from manifold.util.log             import Log
from manifold.util.misc            import is_sublist
from manifold.util.predicate       import Predicate, eq
from manifold.util.type            import returns, accepts

MAX_DEPTH = 3

class ExploreTask(Deferred):
    """
    A pending exploration of the dbgraph graph
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
        self.ast            = None
        self.sq_rename_dict = dict()
        self.keep_root_a    = set()
        self.subqueries     = dict() 

        self.identifier  = random.randint(0, 9999)

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
        self.callback((None, dict()))

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

    def store_subquery(self, ast_sq_rename_dict, relation):
        ast, sq_rename_dict = ast_sq_rename_dict
        #Log.debug(ast, relation)
        if not ast: return

        self.sq_rename_dict.update(sq_rename_dict)

        self.subqueries[relation.get_relation_name()] = (ast, relation)

    def explore(self, stack, missing_fields, dbgraph, allowed_platforms, allowed_capabilities, user, seen_set, query_plan):
        """
        Explore the dbgraph graph to find how to serve each queried fields. We
        explore the DBGraph by prior the 1..1 arcs exploration (DFS) by pushing
        one ExploreTask instance in a Stack per 1..1 arc. If some queried
        fields are not yet served, we push in a Stack which 1..N arcs could
        serve them (BFS) (one ExploreTask per out-going 1..N arc). 
        Args:
            stack: A Stack instance where we push new ExploreTask instances. 
            missing_fields: A set of String containing field names (which
                may be prefixed, such has hops.ttl) involved in the Query.
            dbgraph: The DBGraph instance related to the 3nf graph.
            allowed_platforms: A set of String where each String correponds
                to a queried platform name.
            allowed_platforms: A Capabilities instance.
            user: The User issuing the Query.
            seen_set: A set of String containing the served field names
                involved in the Query.
            query_plan: The QueryPlan instance we're recursively building and
                related to the User Query.
        Returns:
            foreign_key_fields
        """
        #Log.debug("Search in ", self.root.get_name(), "for fields", missing_fields, 'path=', self.path, "SEEN SET =", seen_set)
        relations_11, relations_1N, relations_1Nsq = (), {}, {}
        deferred_list = []

        foreign_key_fields = dict()
        #rename_dict = dict()

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
        
        # ROUTERV2 We might also query foreign keys of backward links
        for neighbour in dbgraph.graph.successors(self.root):
            for relation in dbgraph.get_relations(self.root, neighbour):
                if relation.get_type() == Relation.types.LINK_1N_BACKWARDS:
                    relation_name = relation.get_relation_name()
                    if relation_name not in missing_fields:
                        continue

                    # For backwards links at the moment, the name of the relation is the name/type of the table
                    # let's add the keys of this relation, since we have not explored children links yet
                    table = dbgraph.find_node(relation_name)
                    key = table.get_keys().one()
                    _additional_fields = set(["%s.%s" % (relation_name, field.get_name()) for field in key])
                    missing_fields |= _additional_fields

                    # ... and remove the relation from requested fields
                    missing_fields.remove(relation_name)

                    foreign_key_fields[relation_name] = _additional_fields
        
        root_key = self.root.keys.one()
        root_key_fields = root_key.get_field_names()

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

                # ROUTERV2
                # So far we have search for fields pointing to the current
                # table, but we might also be interested in relationship to
                # other tables where only the key is requested. For example,
                # Get('user', [slices.slice_hrn])
                #   user.slices is a list of slice_hrn, since slice_hrn is key
                # of slices, or type slice.
                #
                # The missing_list might be problematic in cases such as :
                #   user.slices.slice_hrn
                #
                if len(missing_list) <= 1: continue

                # XXX We should be sure that we do this only if we need the key of the table
                continue

                missing_path, (missing_field, missing_pkey) = missing_list[:-2], missing_list[-2:]
                # Example here: in user table
                #   missing_path  = []
                #   missing_field = 'slices'
                #   missing_pkey  = 'slice_hrn'

                # Additional condition:
                #   the field is key of the refered table
                if not missing_field == field: continue

                field_type    = self.root.get_field_type(missing_field)
                if field_type in BASE_TYPES: continue


                refered_table = dbgraph.find_node(field_type, get_parent = True)
                if not refered_table: continue

                key = refered_table.get_keys().one()
                # a key is a set of fields
                if set([missing_pkey]) != key.get_field_names(): continue
                

                flag, shortcut = is_sublist(missing_path, self.path)
                if flag:
                    print "#" * 80
                    print "rename field=", field, "missing=", missing
                    self.sq_rename_dict[field] = missing

                    self.keep_root_a.add(field)
                    is_onjoin = self.root.capabilities.is_onjoin()
                    print "missing fields.remove", missing
                    if not is_onjoin or field not in root_key_fields:
                        missing_fields.remove(missing)
                
                # END ROUTERV2
                
        assert self.depth == 1 or root_key_fields not in missing_fields, "Requesting key fields in child table"

        if self.keep_root_a:
            # XXX NOTE that we have built an AST here without taking into account fields for the JOINs and SUBQUERIES
            # It might not pose any problem though if they come from the optimization phase
#OBSOLETE|            self.ast = self.build_union(self.root, self.keep_root_a, allowed_platforms, dbgraph, user, query_plan)
            self.perform_union_all(self.root, allowed_platforms, dbgraph, user, query_plan)


        if self.depth == MAX_DEPTH:
            self.callback((self.ast, dict()))
            return foreign_key_fields

        # In all cases, we have to list neighbours for returning 1..N relationships. Let's do it now. 
        for neighbour in dbgraph.graph.successors(self.root):
            for relation in dbgraph.get_relations(self.root, neighbour):
                name = relation.get_relation_name()

                if name and name in seen_set:
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

                else:
                    task = ExploreTask(neighbour, relation, self.path, self.parent, self.depth)

                    #if relation.get_type() == Relation.types.PARENT:
                    #    # HERE, instead of doing a left join between a PARENT
                    #    # and a CHILD table, we will do a UNION
                    #    task.addCallback(self.perform_union, root_key, allowed_platforms, dbgraph, user, query_plan)
                    #else:
                    task.addCallback(self.perform_left_join, relation, allowed_platforms, dbgraph, user, query_plan)

                    priority = TASK_11

                deferred_list.append(task)
                stack.push(task, priority)

        DeferredList(deferred_list).addCallback(self.all_done, allowed_platforms, dbgraph, user, query_plan)

        return foreign_key_fields

    def all_done(self, result, allowed_platforms, dbgraph, user, query_plan):
        """

        Args:
            result:
            allowed_platforms: A set of String where each String correponds to a queried platform name.
            dbgraph: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """

        if self.subqueries:
            self.perform_subquery(allowed_platforms, dbgraph, user, query_plan)
        self.callback((self.ast, self.sq_rename_dict))

    def perform_left_join(self, ast_sq_rename_dict, relation, allowed_platforms, dbgraph, user, query_plan):
        """
        Connect a new AST to the current AST using a LeftJoin Node.
        Args:
            ast: A child AST that must be connected to self.ast using LEFT JOIN .
            relation: The Relation connecting the child Table and the parent Table involved in this LEFT jOIN.
            dbgraph: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        ast, sq_rename_dict = ast_sq_rename_dict

        if not ast: return

        self.sq_rename_dict.update(sq_rename_dict)

        if not self.ast:
            # This can occur if no interesting field was found in the table, but it is just used to connect children tables
            self.perform_union_all(self.root, allowed_platforms, dbgraph, user, query_plan)
        self.ast.left_join(ast, relation.get_predicate())

    def perform_subquery(self, allowed_platforms, dbgraph, user, query_plan):
        """
        Connect a new AST to the current AST using a SubQuery Node.
        If the connected table is "on join", we will use a LeftJoin and a CrossProduct Node instead.
        Args:
            dbgraph: The DBGraph instance related to the 3nf graph.
            allowed_platforms: A set of String where each String correponds to a queried platform name.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        # We need to build an AST just to collect subqueries
#OBSOLETE|        # XXX dbgraph not defined
#OBSOLETE|        #if not self.ast:
#OBSOLETE|        #    self.ast = self.build_union(self.root, self.keep_root_a, dbgraph, user)
#OBSOLETE|        if not self.ast:
#OBSOLETE|            fields = set()
#OBSOLETE|            for _, (_, relation) in self.subqueries.items():
#OBSOLETE|                fields |= relation.get_predicate().get_field_names()
#OBSOLETE|            self.ast = self.build_union(self.root, fields, allowed_platforms, dbgraph, user, query_plan)
        if not self.ast:
            self.perform_union_all(self.root, allowed_platforms, dbgraph, user, query_plan)
        
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
            query = Query.action('get', self.root.get_name()).select(set(xp_key))
            self.ast = AST().cross_product(xp_ast_relation, query)
            predicate = Predicate(xp_key, eq, xp_value)

            self.ast.left_join(ast, predicate)
        else:
            self.ast.subquery(self.subqueries.values())

    def perform_union(self, ast_sq_rename_dict, key, allowed_platforms, dbgraph, user, query_plan):
        """
        """
        ast, sq_rename_dict = ast_sq_rename_dict

        # The relation parameter is only present for ensuring a uniform interface to perform_* functions
        if not ast:
            return
        if not self.ast:
            self.ast = AST()
        self.ast.union(ast, key)

    # TODO rename: perform_union and remove needed_fields parameter
    def perform_union_all(self, table, allowed_platforms, dbgraph, user, query_plan):
#OBSOLETE|    def build_union(self, table, needed_fields, allowed_platforms, dbgraph, user, query_plan):
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

#MANDO|DEPRECATED|        # TO BE REMOVED ?
#MANDO|DEPRECATED|        # Exploring this tree according to a DFS algorithm leads to a table
#MANDO|DEPRECATED|        # ordering leading to feasible successive joins
#MANDO|DEPRECATED|        map_method_bestkey = dict()
#MANDO|DEPRECATED|#DEPRECATED|LOIC|        map_method_demux   = dict()
#MANDO|DEPRECATED|
#MANDO|DEPRECATED|        # XXX I don't understand this -- Jordan
#MANDO|DEPRECATED|        # Update the key used by a given method
#MANDO|DEPRECATED|        # The more we iterate, the best the key is
#MANDO|DEPRECATED|        if key:
#MANDO|DEPRECATED|            for method, keys in table.map_method_keys.items():
#MANDO|DEPRECATED|                if key in table.map_method_keys[method]: 
#MANDO|DEPRECATED|                    map_method_bestkey[method] = key 

        # For each platform related to the current table, extract the
        # corresponding table and build the corresponding FROM node
        map_method_fields = table.get_annotations()
        for method, fields in map_method_fields.items(): 
            # The table announced by the platform fits with the 3nf schema
            # Build the corresponding FROM 
            #sub_table = Table.make_table_from_platform(table, fields, method.get_platform())

            platform = method.get_platform()
            capabilities = dbgraph.get_capabilities(platform, method.get_name())

            map_field_local = {f.get_name(): f.is_local() for f in table.get_fields()}
            selected_fields  = set([f for f in fields if not map_field_local[f]])
            selected_fields |= self.keep_root_a
            
            if not capabilities.projection:
                all_fields = set([f.get_name() for f in table.get_fields()])
                # IN 3NF, this is not necessarily all fields
                selected_fields = all_fields
            
            # We create 'get' queries by default, this will be overriden in set_ast
            query = Query.action('get', method.get_name()).select(selected_fields)
            #query = Query.action('get', method.get_name()).select(fields)

            if not platform in allowed_platforms:
                continue

            # The current platform::table might be ONJOIN (no retrieve capability), but we
            # might be able to collect the keys, so we have disabled the following code
            # XXX Improve platform capabilities support
            if self.relation is None and not capabilities.retrieve: continue

            from_ast = AST(user = user).From(platform, query, capabilities, key)
            if from_ast:
                query_plan.add_from(from_ast.get_root())
                self.perform_union((from_ast, dict()), key, allowed_platforms, dbgraph, user, query_plan)
