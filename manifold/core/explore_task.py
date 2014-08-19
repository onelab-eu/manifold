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

from types                          import StringTypes
from twisted.internet.defer         import Deferred, DeferredList

from manifold.core.ast              import AST
from manifold.core.filter           import Filter
from manifold.core.fields           import Fields
from manifold.core.query            import Query, ACTION_GET
from manifold.core.relation         import Relation
from manifold.core.stack            import Stack, TASK_11, TASK_1Nsq, TASK_1N
from manifold.types                 import BASE_TYPES
from manifold.util.log              import Log
from manifold.util.misc             import is_sublist
from manifold.util.predicate        import Predicate, eq
from manifold.util.type             import returns, accepts

MAX_DEPTH = 5

class ExploreTask(Deferred):
    """
    A pending exploration of the metadata graph
    """
    last_identifier = 0

    def __init__(self, interface, root, relation, path, parent, depth):
        """
        Constructor.
        Args:
            interface: A Router instance.
            root     : A Table instance.
            relation : A Relation instance
            path     : A list instance.
            parent   : An ExploreTask instance.
            depth    : A positive integer value, corresponding to the number of
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
        self.keep_root_a = Fields()
#        self.subqueries  = dict()
        self.sq_rename_dict = dict()

        ExploreTask.last_identifier += 1
        self.identifier  = ExploreTask.last_identifier

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

    def default_errback(self, failure):
        print "DEFAULT ERRBACK", failure

    def cancel(self):
        self.callback((None, dict()))

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
            allowed_platforms: A set of String where each String corresponds
                to a queried platform name.
            allowed_platforms: A Capabilities instance.
            user: The User issuing the Query.
            seen_set: A set of String containing the served field names
                involved in the Query.
            query_plan: The QueryPlan instance we're recursively building and
                related to the User Query.
        Returns:
            foreign_key_fields

        Modifies in place:
            missing_fields
            seen_set
        """
        #Log.tmp("Search in", self.root.get_name(), "for fields", missing_fields, 'path=', self.path, "SEEN SET =", seen_set, "depth=", self.depth)
        relations_11, relations_1N, relations_1Nsq = (), {}, {}
        deferred_list = []

        foreign_key_fields = dict()
        #rename_dict = dict()

        # self.path = X.Y.Z indicates the subqueries we have traversed
        # We are thus able to answer to parts of the query at the root,
        # after X, after X, Z, after X.Y after X.Y.Z, after X.Z, after Y.Z, and
        # X.Y.Z

        # We have a list of missing fields to search for in the current table
        # and beyond. Since we might have subfields, the first step is to group
        # those subfields according to their method.
        #
        # missing_parent_fields:
        #
        # map_parent_missing_subfields:
        #
        # map_original_field:
        #
        missing_parent_fields, map_parent_missing_subfields, map_original_field, rename\
            = missing_fields.split_subfields(True, self.path, True)
#MANDO|            = missing_fields.split_subfields(include_parent = True,\
#MANDO|                    current_path = self.path,               \
#MANDO|                    allow_shortcuts = True)

        root_provided_fields = self.root.get_field_names()

        # We might also query foreign keys of backward links
        for neighbour in sorted(metadata.graph.successors(self.root)):
            for relation in sorted(metadata.get_relations(self.root, neighbour)):
                if relation.get_type() == Relation.types.LINK_1N_BACKWARDS:
                    relation_name = relation.get_relation_name()
                    if relation_name not in missing_fields:
                        continue

                    # For backwards links at the moment, the name of the relation is the name/type of the table
                    # let's add the keys of this relation, since we have not explored children links yet
                    table = metadata.find_node(relation_name)
                    key = table.get_keys().one()
                    _additional_fields = set(["%s.%s" % (relation_name, field.get_name()) for field in key])
                    missing_fields |= _additional_fields

                    # ... and remove the relation from requested fields
                    missing_fields.remove(relation_name)

                    foreign_key_fields[relation_name] = _additional_fields

        root_key = self.root.keys.one()
        root_key_fields = root_key.get_field_names()

        # We store in self.keep_root_a the field of interest in the current
        # root_table, that will be removed from missing_fields

        #....... Rewritten

        self.keep_root_a |= missing_parent_fields & root_provided_fields

        for f in self.keep_root_a:
            if f in rename and rename[f] is not None:
                self.sq_rename_dict[Fields.join(rename[f], f)] = f

        for field in self.keep_root_a:
            # Now if we are requesting has the key as the only subfield, we can also
            # retrieve it from the current table (as a foreign key), at the cost of
            # a Rename operation for the final results (query and record rewriting).

            # for normal fields
            if field in missing_fields:
                missing_fields.remove(field)

            # for shortcuts
            if field not in map_parent_missing_subfields:
                # ..unless already done # XXX EXPLAIN BOTH LINES
                if map_original_field[field] != field:
                    missing_fields.remove(map_original_field[field])
#DEPRECATED|#UNTIL WE EXPLAIN|                    # We won't search those fields in subsequent explorations,
#DEPRECATED|#UNTIL WE EXPLAIN|                    # unless the belong to the key of an ONJOIN table
#DEPRECATED|#UNTIL WE EXPLAIN|                    is_onjoin = self.root.capabilities.is_onjoin()
#DEPRECATED|#UNTIL WE EXPLAIN|                    V
#DEPRECATED|#UNTIL WE EXPLAIN|                    print "xx"
#DEPRECATED|#UNTIL WE EXPLAIN|                    print "is_onjoin", is_onjoin
#DEPRECATED|#UNTIL WE EXPLAIN|                    print "field", field
#DEPRECATED|#UNTIL WE EXPLAIN|                    print "root_key_fields", root_key_fields
#DEPRECATED|#UNTIL WE EXPLAIN|                    print "fiend in rkf", field in root_key_fields
#DEPRECATED|#UNTIL WE EXPLAIN|
#DEPRECATED|#UNTIL WE EXPLAIN|                    # XXX why ?
#DEPRECATED|#UNTIL WE EXPLAIN|                    if not is_onjoin or field not in root_key_fields:
#DEPRECATED|#UNTIL WE EXPLAIN|                        print "yy"
#DEPRECATED|#UNTIL WE EXPLAIN|                        # Set is changing during iteration !!!
#DEPRECATED|#UNTIL WE EXPLAIN|                        missing_fields.remove(missing)
#DEPRECATED|#UNTIL WE EXPLAIN|

#WHATFOR?|            else:
#WHATFOR?|                # Example: we are in hops and we want 1) hops.probes.ip and 2) # hops.ip
#WHATFOR?|                # self.keep_root_a has probes, and we suppose:
#WHATFOR?|                # > field = 'probes'
#WHATFOR?|                subfields = map_parent_missing_subfields[field]
#WHATFOR?|                print "field=", field
#WHATFOR?|                print "subfields=", subfields
#WHATFOR?|
#WHATFOR?|                # hence subfields = map_parent_missing_subfields['probes'] = {'ip'}
#WHATFOR?|
#WHATFOR?|                if len(subfields) > 1: continue
#WHATFOR?|                # Note: composite keys not taken into account yet
#WHATFOR?|
#WHATFOR?|                subfield = iter(subfields).next()
#WHATFOR?|
#WHATFOR?|                # > subfield = 'ip'
#WHATFOR?|
#WHATFOR?|                field_type = self.root.get_field_type(field)
#WHATFOR?|                if field_type in BASE_TYPES: continue # Should not happen
#WHATFOR?|
#WHATFOR?|                # > field_type = 'ip' (not a base type)
#WHATFOR?|
#WHATFOR?|                refered_table = metadata.find_node(field_type, get_parent = True)
#WHATFOR?|                if not refered_table: continue
#WHATFOR?|
#WHATFOR?|                key = refered_table.get_keys().one()
#WHATFOR?|                # a key is a set of fields
#WHATFOR?|                if subfields != key.get_field_names(): continue
#WHATFOR?|
#WHATFOR?|                # In the record, we will rename field.subfield by field
#WHATFOR?|                field_before = Fields.join(field, subfield)
#WHATFOR?|                print "field", field
#WHATFOR?|                print "subfield", subfield
#WHATFOR?|                print "field_before", field_before
#WHATFOR?|                rename_dict[field_before] = field
#WHATFOR?|                print "rename_dict[field_before]=", field
#WHATFOR?|
#WHATFOR?|                # > field_before = 'probes.ip'
#WHATFOR?|                # > rename_dict['probes.ip'] = 'ip'
#WHATFOR?|
#WHATFOR?|                is_onjoin = self.root.capabilities.is_onjoin()
#WHATFOR?|                # Explain two conditions:
#WHATFOR?|                # a) not is_onjoin:
#WHATFOR?|                # b) field not in root_key_fields:
#WHATFOR?|                if not is_onjoin or field not in root_key_fields:
#WHATFOR?|                    # > map_original_field['probes'] = ?????
#WHATFOR?|#DEPRECATED|                    if field_before != map_original_field[field]:
#WHATFOR?|#DEPRECATED|                        print "a)"
#WHATFOR?|#DEPRECATED|                        before_original = Fields.join(map_original_field[field], subfield)
#WHATFOR?|#DEPRECATED|                        print "before_original=", before_original
#WHATFOR?|#DEPRECATED|                        print "  . map_original_field[field]", map_original_field[field]
#WHATFOR?|#DEPRECATED|                        print "  . subfield", subfield
#WHATFOR?|#DEPRECATED|                    else:
#WHATFOR?|                    print "b)"
#WHATFOR?|                    before_original = map_original_field[field]
#WHATFOR?|                    print "before_original = map_original_field[field] = ", map_original_field[field]
#WHATFOR?|
#WHATFOR?|                    missing_fields.remove(before_original)

        #........ End rewritten

        #assert self.depth == 1 or root_key_fields not in missing_fields, "Requesting key fields in child table"

        if self.keep_root_a:
            # XXX NOTE that we have built an AST here without taking into account fields for the JOINs and SUBQUERIES
            # It might not pose any problem though if they come from the optimization phase
#OBSOLETE|            self.ast = self.build_union(self.root, self.keep_root_a, allowed_platforms, metadata, user, query_plan)
            self.perform_union_all(self.root, allowed_platforms, metadata, user, query_plan)

            # ROUTERV2
            #if rename_dict:
            #    # If we need to rename fields after retrieving content from the table...
            #    self.ast.rename(rename_dict)

        if self.depth == MAX_DEPTH:
            self.callback((self.ast, dict()))
            return foreign_key_fields

        # In all cases, we have to list neighbours for returning 1..N relationships. Let's do it now.
        for neighbour in metadata.graph.successors(self.root):
            for relation in metadata.get_relations(self.root, neighbour):
                name = relation.get_relation_name()

                # XXX Sometimes we might want to add the type: if we have not
                # found f in "ip source", we will not find it in "ip
                # destination"
                # We need to take care of explicit relations only
                if name:
                    if name in seen_set or name in self.path: # XXX Sometimes we need to look at a table in the path, in case there was a barrier in the path of the field requested by the user
                        continue
                    seen_set.add(name)

                if relation.requires_subquery():
                    subpath = self.path[:]
                    subpath.append(name)
                    task = ExploreTask(self._interface, neighbour, relation, subpath, self, self.depth+1)
                    task.addCallback(self.perform_subquery, relation, allowed_platforms, metadata, user, query_plan)
                    task.addErrback(self.default_errback)

                    relation_name = relation.get_relation_name()

                    # The relation has priority if at least one field is like PATH.relation.xxx
                    priority = TASK_1N
                    for missing in missing_fields:
                        # XXX self.path is a list !!!!XXX
                        if missing.startswith("%s.%s." % (self.path, relation.get_relation_name())):
                            priority = TASK_1Nsq
                            break
                    #priority = TASK_1Nsq if relation_name in missing_subqueries else TASK_1N

                else:
                    task = ExploreTask(self._interface, neighbour, relation, self.path, self.parent, self.depth)
                    if relation.get_type() == Relation.types.PARENT:
                        # HERE, instead of doing a left join between a PARENT
                        # and a CHILD table, we will do a UNION
                        task.addCallback(self.perform_union, root_key, allowed_platforms, metadata, user, query_plan)
                    else:
                        task.addCallback(self.perform_left_join, relation, allowed_platforms, metadata, user, query_plan)
                    task.addErrback(self.default_errback)
                    priority = TASK_11

                deferred_list.append(task)
                stack.push(task, priority)

        d = DeferredList(deferred_list)
        d.addCallback(self.all_done, allowed_platforms, metadata, query_plan)
        d.addErrback(self.default_errback)

        return foreign_key_fields

    def all_done(self, result, allowed_platforms, metadata, query_plan):
        """

        Args:
            result:
            allowed_platforms: A set of String where each String corresponds to a queried platform name.
            metadata: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """

        try:
            self.callback((self.ast, self.sq_rename_dict))
        except Exception, e:
            Log.error("Exception caught in ExploreTask::all_done: %s" % e)
            self.cancel()
            raise e

    def perform_left_join(self, ast_sq_rename_dict, relation, allowed_platforms, metadata, user, query_plan):
        """
        Connect a new AST to the current AST using a LeftJoin Node.
        Args:
            relation: The Relation connecting the child Table and the parent Table involved in this LEFT jOIN.
            metadata: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        # ast should be equal to self.ast. We need to pass it as a parameter to the defer callback
        ast, sq_rename_dict = ast_sq_rename_dict
        if not ast: return
        self.sq_rename_dict.update(sq_rename_dict)
        if not self.ast:
            # This can occur if no interesting field was found in the table, but it is just used to connect children tables
            self.perform_union_all(self.root, allowed_platforms, metadata, user, query_plan)
        self.ast.left_join(ast, relation.get_predicate().copy())

    # XXX sq_rename_dict ?????? really ????
    def perform_subquery(self, ast_sq_rename_dict, relation, allowed_platforms, metadata, user, query_plan):
        """
        Connect a new AST to the current AST using a SubQuery Node.
        If the connected table is "on join", we will use a LeftJoin
        and a CartesianProduct Node instead.
        Args:
            allowed_platforms: A set of String where each String corresponds to a queried platform name.
            metadata: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        ast, sq_rename_dict = ast_sq_rename_dict
        if not ast: return
        self.sq_rename_dict.update(sq_rename_dict)

        # We need to build an AST just to collect subqueries
        if not self.ast:
            self.perform_union_all(self.root, allowed_platforms, metadata, user, query_plan)

        self.ast.subquery(ast, relation)

#        # This might be more simple if moved in all_done
#        if self.sq_rename_dict:
#            self.ast.rename(self.sq_rename_dict)
#            self.sq_rename_dict = dict()

    def perform_union(self, ast, key, allowed_platforms, metadata, user, query_plan):
        """
        Args:
            ast:
            key:
            allowed_platforms: A set of String where each String corresponds to a queried platform name.
            metadata: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        if not ast:
            return
        if not self.ast:
            self.ast = AST(self._interface)
        self.ast.union(ast, key)

    def perform_union_all(self, table, allowed_platforms, metadata, user, query_plan):
        """
        Complete a QueryPlan instance by adding an Union of From Node related
        to a same Table.
        Args:
            table: The 3nf Table, potentially provided by several platforms.
            allowed_platforms: A set of String where each String corresponds to a queried platform name.
            metadata: The DBGraph instance related to the 3nf graph.
            user: The User issuing the Query.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        from_asts = list()
        key = table.get_keys().one()

        # For each platform related to the current table, extract the
        # corresponding table and build the corresponding FROM node
        map_method_fields = table.get_annotation()
        ##### print "map_method_fields", map_method_fields
        for method, fields in map_method_fields.items():
            #Log.tmp("method", method.get_name())
            #Log.tmp("table", table.get_name())
            # The table announced by the platform fits with the 3nf schema
            # Build the corresponding FROM
            #sub_table = Table.make_table_from_platform(table, fields, method.get_platform())

            # XXX We lack field pruning
            # We create 'get' queries by default, this will be overriden in query_plan::fix_froms
            # - Here we could keep all fields, but local fields we be
            # repreented many times in the query plan, that will mess up
            # when we try to optimize selection/projection
            # - If we select (fields & self.keep_root_a), then we cannot
            # know later on where a field needed for a join that has been
            # injected can be found
            # - With unique naming, we could adopt the first solution. To
            # balance both, we will remove local fields.
            map_field_local = {f.get_name(): f.is_local() for f in table.get_fields()}
            selected_fields  = Fields([f for f in fields if not map_field_local[f]])
            selected_fields |= self.keep_root_a

            query = Query.action(ACTION_GET, method.get_name()).select(selected_fields)

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

            from_ast = AST(self._interface).From(platform, query, capabilities, key)

            if from_ast:
                self.perform_union(from_ast, key, allowed_platforms, metadata, user, query_plan)
