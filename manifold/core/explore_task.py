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
#DEPRECATED|
#DEPRECATED|    def store_subquery(self, ast_sq_rename_dict, relation):
#DEPRECATED|        ast, sq_rename_dict = ast_sq_rename_dict
#DEPRECATED|        #Log.debug(ast, relation)
#DEPRECATED|        if not ast: return
#DEPRECATED|        self.sq_rename_dict.update(sq_rename_dict)
#DEPRECATED|        self.subqueries[relation.get_relation_name()] = (ast, relation)

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
        """
        #Log.tmp("Search in", self.root.get_name(), "for fields", missing_fields, 'path=', self.path, "SEEN SET =", seen_set, "depth=", self.depth)
        relations_11, relations_1N, relations_1Nsq = (), {}, {}
        deferred_list = []

        foreign_key_fields = dict()
        rename_dict = dict()

        # self.path = X.Y.Z indicates the subqueries we have traversed
        # We are thus able to answer to parts of the query at the root,
        # after X, after X, Z, after X.Y after X.Y.Z, after X.Z, after Y.Z, and
        # X.Y.Z

        # We have a list of missing fields to search for in the current table
        # and beyond. Since we might have subfields, the first step is to group
        # those subfields according to their method.
        missing_parent_fields, map_parent_missing_subfields, map_original_field, rename\
            = missing_fields.split_subfields(True, self.path, True)
#MANDO|            = missing_fields.split_subfields(include_parent = True,\
#MANDO|                    current_path = self.path,               \
#MANDO|                    allow_shortcuts = True)

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

        root_key_fields = self.root.keys.one().get_field_names()

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
                # ..unless already done
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

            else:
                subfields = map_parent_missing_subfields[field]

                if len(subfields) > 1: continue
                # Note: composite keys not taken into account yet

                subfield = iter(subfields).next()

                field_type = self.root.get_field_type(field)
                if field_type in BASE_TYPES: continue # Should not happen

                refered_table = metadata.find_node(field_type, get_parent = True)
                if not refered_table: continue

                key = refered_table.get_keys().one()
                # a key is a set of fields
                if subfields != key.get_field_names(): continue

                # In the record, we will rename field.subfield by field
                field_before = Fields.join(field, subfield)
                rename_dict[field_before] = field

                is_onjoin = self.root.capabilities.is_onjoin()
                if not is_onjoin or field not in root_key_fields:
                    before_original = Fields.join(map_original_field[field], subfield)
                    missing_fields.remove(before_original)

        #........ End rewritten

#DEPRECATED|        # Which fields we are keeping for the current table, and which we are removing from missing_fields
#DEPRECATED|        for field in root_provided_fields:
#DEPRECATED|
#DEPRECATED|            # We use a list since the set is changing during iteration
#DEPRECATED|            #print "list(missing_fields)", list(missing_fields)
#DEPRECATED|            for missing in list(missing_fields):
#DEPRECATED|                # missing has dots inside
#DEPRECATED|                # hops.ttl --> missing_path == ["hops"] missing_field == ["ttl"]
#DEPRECATED|                missing_list = missing.split('.')
#DEPRECATED|                missing_path, (missing_field,) = missing_list[:-1], missing_list[-1:]
#DEPRECATED|                flag, shortcut = is_sublist(missing_path, self.path) #self.path, missing_path)
#DEPRECATED|                if flag and missing_field == field:
#DEPRECATED|                    #print 'current table provides missing field PATH=', self.path, 'field=', field, 'missing=', missing
#DEPRECATED|                    self.keep_root_a.add(field)
#DEPRECATED|
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
#DEPRECATED|                    missing_fields.remove(missing)
#DEPRECATED|
#DEPRECATED|
#DEPRECATED|                # ROUTERV2
#DEPRECATED|                # So far we have search for fields pointing to the current
#DEPRECATED|                # table, but we might also be interested in relationship to
#DEPRECATED|                # other tables where only the key is requested. For example,
#DEPRECATED|                # Get('user', [slices.slice_hrn])
#DEPRECATED|                #   user.slices is a list of slice_hrn, since slice_hrn is key
#DEPRECATED|                # of slices, or type slice.
#DEPRECATED|                #
#DEPRECATED|                # The missing_list might be problematic in cases such as :
#DEPRECATED|                #   user.slices.slice_hrn
#DEPRECATED|                #
#DEPRECATED|                if len(missing_list) <= 1: continue
#DEPRECATED|
#DEPRECATED|                missing_path, (missing_field, missing_pkey) = missing_list[:-2], missing_list[-2:]
#DEPRECATED|                # Example here: in user table
#DEPRECATED|                #   missing_path  = []
#DEPRECATED|                #   missing_field = 'slices'
#DEPRECATED|                #   missing_pkey  = 'slice_hrn'
#DEPRECATED|
#DEPRECATED|                # Additional condition:
#DEPRECATED|                #   the field is key of the refered table
#DEPRECATED|                if not missing_field == field: continue
#DEPRECATED|
#DEPRECATED|                field_type    = self.root.get_field_type(missing_field)
#DEPRECATED|                if field_type in BASE_TYPES: continue
#DEPRECATED|
#DEPRECATED|
#DEPRECATED|                refered_table = metadata.find_node(field_type, get_parent = True)
#DEPRECATED|                if not refered_table: continue
#DEPRECATED|
#DEPRECATED|                key = refered_table.get_keys().one()
#DEPRECATED|                # a key is a set of fields
#DEPRECATED|                if set([missing_pkey]) != key.get_field_names(): continue
#DEPRECATED|
#DEPRECATED|
#DEPRECATED|                #print "FOUND", missing, " in ", self.root.get_name()
#DEPRECATED|                # The rest is the same
#DEPRECATED|                flag, shortcut = is_sublist(missing_path, self.path)
#DEPRECATED|                if flag:
#DEPRECATED|                    # BUG HERE
#DEPRECATED|                    print "we keep field=", field
#DEPRECATED|                    print "   . this field should give us", missing_field, "and", missing_pkey
#DEPRECATED|                    rename_dict[field] = missing
#DEPRECATED|
#DEPRECATED|                    self.keep_root_a.add(field)
#DEPRECATED|                    is_onjoin = self.root.capabilities.is_onjoin()
#DEPRECATED|                    if not is_onjoin or field not in root_key_fields:
#DEPRECATED|                        missing_fields.remove(missing)
#DEPRECATED|
#DEPRECATED|                # END ROUTERV2


        assert self.depth == 1 or root_key_fields not in missing_fields, "Requesting key fields in child table"

        if self.keep_root_a:
            # XXX NOTE that we have built an AST here without taking into account fields for the JOINs and SUBQUERIES
            # It might not pose any problem though if they come from the optimization phase
#OBSOLETE|            self.ast = self.build_union(self.root, self.keep_root_a, allowed_platforms, metadata, user, query_plan)
            self.ast = self.perform_union(self.root, allowed_platforms, metadata, query_plan)

            # ROUTERV2
            if rename_dict:
                # If we need to rename fields after retrieving content from the table...
                self.ast.rename(rename_dict)

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
                    task.addCallback(self.perform_subquery, relation, allowed_platforms, metadata, query_plan)
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
                    task.addCallback(self.perform_left_join, relation, allowed_platforms, metadata, query_plan)
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
#DEPRECATED|            if self.subqueries:
#DEPRECATED|                self.perform_subquery(allowed_platforms, metadata, query_plan)
#DEPRECATED|                if self.sq_rename_dict:
#DEPRECATED|                    self.ast.rename(self.sq_rename_dict)
#DEPRECATED|                    self.sq_rename_dict = dict()
            self.callback((self.ast, self.sq_rename_dict))
        except Exception, e:
            Log.error("Exception caught in ExploreTask::all_done: %s" % e)
            self.cancel()
            raise e

    def perform_left_join(self, ast_sq_rename_dict, relation, allowed_platforms, metadata, query_plan):
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
            self.ast = self.perform_union(self.root, allowed_platforms, metadata, query_plan)
        self.ast.left_join(ast, relation.get_predicate().copy())

    def perform_subquery(self, ast_sq_rename_dict, relation, allowed_platforms, metadata, query_plan):
        """
        Connect a new AST to the current AST using a SubQuery Node.
        If the connected table is "on join", we will use a LeftJoin
        and a CartesianProduct Node instead.
        Args:
            metadata: The DBGraph instance related to the 3nf graph.
            allowed_platforms: A set of String where each String corresponds to a queried platform name.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        ast, sq_rename_dict = ast_sq_rename_dict
        if not ast: return
        self.sq_rename_dict.update(sq_rename_dict)

        # We need to build an AST just to collect subqueries
        if not self.ast:
            self.ast = self.perform_union(self.root, allowed_platforms, metadata, query_plan)

        self.ast.subquery(ast, relation)

        # This might be more simple if moved in all_done
        if self.sq_rename_dict:
            self.ast.rename(self.sq_rename_dict)
            self.sq_rename_dict = dict()


    def perform_union(self, table, allowed_platforms, metadata, query_plan):
        """
        Complete a QueryPlan instance by adding an Union of From Node related
        to a same Table.
        Args:
            table: The 3nf Table, potentially provided by several platforms.
            allowed_platforms: A set of String where each String corresponds to a queried platform name.
            query_plan: The QueryPlan instance related to this Query, and that we're updating.
        """
        from_asts = list()
        key = table.get_keys().one()

        # TO BE REMOVED ?
        # Exploring this tree according to a DFS algorithm leads to a table
        # ordering leading to feasible successive joins
        map_method_bestkey = dict()
        map_method_demux   = dict()

#DISABLED|        # XXX I don't understand this -- Jordan
#DISABLED|        # Update the key used by a given method
#DISABLED|        # The more we iterate, the best the key is
#DISABLED|        if key:
#DISABLED|            try:
#DISABLED|                for method, keys in table.map_method_keys.items():
#DISABLED|                    if key in table.map_method_keys[method]:
#DISABLED|                        map_method_bestkey[method] = key
#DISABLED|            except AttributeError:
#DISABLED|                map_method_bestkey[table.name] = key

        # For each platform related to the current table, extract the
        # corresponding table and build the corresponding FROM node
        map_method_fields = table.get_annotation()
        ##### print "map_method_fields", map_method_fields
        for method, fields in map_method_fields.items():
            Log.tmp("method", method.get_name())
            Log.tmp("table", table.get_name())
            if method.get_name() == table.get_name():
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
                selected_fields  = set([f for f in fields if not map_field_local[f]])
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
#DEPRECATED|                query_plan.add_from(from_ast.get_root())
#DISABLED|                try:
#DISABLED|                    if method in table.methods_demux:
#DISABLED|                        from_ast.demux().projection(list(fields))
#DISABLED|                        demux_node = from_ast.get_root().get_child()
#DISABLED|                        assert isinstance(demux_node, Demux), "Bug"
#DISABLED|                        map_method_demux[method] = demux_node
#DISABLED|                except AttributeError:
#DISABLED|                    pass
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
#                from_ast = AST(self._interface, user = user)
                from_ast = AST(self._interface)
                from_ast.root = demux_node
                Log.warning("ExploreTask: TODO: plug callback")
                #TODO from_node.addCallback(from_ast.callback)

#DEPRECATED|                query_plan.add_from(from_ast.get_root())

                # Add DUP and SELECT to this AST
                from_ast.dup(key_dup).projection(select_fields)

            from_asts.append(from_ast)

        # Add the current table in the query plane
        # Process this table, which is the root of the 3nf tree
        if not from_asts:
            print "#### NONE"
            return None
        return AST(self._interface).union(from_asts, key)


