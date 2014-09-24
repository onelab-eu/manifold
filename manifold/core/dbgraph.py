#DEPRECATED|#!/usr/bin/env python
#DEPRECATED|# -*- coding: utf-8 -*-
#DEPRECATED|#
#DEPRECATED|# A DBGraph represent a DB schema.
#DEPRECATED|# - Each vertex corresponds to a Table.
#DEPRECATED|# - Each arc corresponds to a Relation.
#DEPRECATED|#
#DEPRECATED|# Copyright (C) UPMC Paris Universitas
#DEPRECATED|# Authors:
#DEPRECATED|#   Jordan Augé       <jordan.auge@lip6.fr> 
#DEPRECATED|#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#DEPRECATED|
#DEPRECATED|# NOTE: The fastest way to traverse all edges of a graph is via
#DEPRECATED|# adjacency_iter(), but the edges() method is often more convenient.
#DEPRECATED|
#DEPRECATED|from copy                       import deepcopy
#DEPRECATED|from networkx                   import draw_graphviz, DiGraph
#DEPRECATED|from traceback                  import format_exc
#DEPRECATED|from types                      import StringTypes
#DEPRECATED|
#DEPRECATED|from manifold.core.capabilities import Capabilities
#DEPRECATED|from manifold.core.method       import Method
#DEPRECATED|from manifold.core.key          import Key
#DEPRECATED|from manifold.core.keys         import Keys
#DEPRECATED|from manifold.core.relation     import Relation
#DEPRECATED|from manifold.core.table        import Table 
#DEPRECATED|from manifold.util.predicate    import Predicate 
#DEPRECATED|from manifold.util.log          import Log
#DEPRECATED|from manifold.util.type         import accepts, returns
#DEPRECATED|
#DEPRECATED|# TODO DBGraph should inherit nx.DiGraph
#DEPRECATED|class DBGraph(object):
#DEPRECATED|    def check_init(tables, map_method_capabilities):
#DEPRECATED|        """
#DEPRECATED|        (Internal usage) Check DbGraph::__init__() parameters consistency.
#DEPRECATED|        """
#DEPRECATED|        table_names_1 = set([table.get_name()  for table  in tables])
#DEPRECATED|        table_names_2 = set([method.get_name() for method in map_method_capabilities.keys()])
#DEPRECATED|        assert table_names_1 == table_names_2,\
#DEPRECATED|            "Inconsistency between:\n- table_names_1 = %s\n- table_names_2 = %s" % (
#DEPRECATED|                table_names_1,
#DEPRECATED|                table_names_2
#DEPRECATED|            )
#DEPRECATED|
#DEPRECATED|    def __init__(self, tables, map_method_capabilities):
#DEPRECATED|        """
#DEPRECATED|        Constructor.
#DEPRECATED|        Args:
#DEPRECATED|            tables: A frozenset of Table instances.
#DEPRECATED|            map_method_capabilities: A dict {Method : Capabilities}
#DEPRECATED|        """
#DEPRECATED|        assert isinstance(tables, frozenset), "Invalid tables = %s (%s)" % (tables, type(tables))
#DEPRECATED|        assert isinstance(map_method_capabilities, dict)
#DEPRECATED|
#DEPRECATED|        self.graph = DiGraph()
#DEPRECATED|        for table in tables:
#DEPRECATED|            self.append(table)
#DEPRECATED|        self._map_method_capabilities = map_method_capabilities
#DEPRECATED|
#DEPRECATED|    @returns(Capabilities)
#DEPRECATED|    def get_capabilities(self, platform_name, table_name):
#DEPRECATED|        """
#DEPRECATED|        Retrieve Capabilities related to a given Table provided by
#DEPRECATED|        a given platform.
#DEPRECATED|        Args:
#DEPRECATED|            platform_name: The name of a Manifold platform.
#DEPRECATED|            table_name: The name of the Table.
#DEPRECATED|        Returns:
#DEPRECATED|            The corresponding Capabilities/
#DEPRECATED|        """
#DEPRECATED|        method = Method(platform_name, table_name)
#DEPRECATED|        try:
#DEPRECATED|            return self._map_method_capabilities[method]
#DEPRECATED|        except KeyError, e:
#DEPRECATED|            Log.error("DbGraph::get_capabilities(): Cannot find method %s in {%s}" % (
#DEPRECATED|                method,
#DEPRECATED|                ", ".join(["%s" % m for m in self._map_method_capabilities.keys()]))
#DEPRECATED|            )
#DEPRECATED|            raise e
#DEPRECATED|
#DEPRECATED|    @returns(Keys)
#DEPRECATED|    def get_key(self, method):
#DEPRECATED|        """
#DEPRECATED|        Retrieve Key instances related to a given Method.
#DEPRECATED|        Args:
#DEPRECATED|            method: A Method instance identifying a Table related
#DEPRECATED|                to a given platform in this DbGraph.
#DEPRECATED|        """
#DEPRECATED|        self.find_node(method).get_keys()
#DEPRECATED|
#DEPRECATED|    def make_arc(self, u, v):
#DEPRECATED|        """
#DEPRECATED|        Connect a "u" Table to a "v" Table (if necessary) in the DbGraph
#DEPRECATED|        Args:
#DEPRECATED|            u: The source node (Table instance)
#DEPRECATED|            v: The target node (Table instance)
#DEPRECATED|        """
#DEPRECATED|        #-----------------------------------------------------------------------
#DEPRECATED|
#DEPRECATED|        returns(Predicate)
#DEPRECATED|        @accepts(set, Key)
#DEPRECATED|        def make_predicate(fields_u, key_v):
#DEPRECATED|            """
#DEPRECATED|            Compute the Predicate to JOIN a "u" Table with "v" Table
#DEPRECATED|            Args:
#DEPRECATED|                fields_u: The set of Field of u required to JOIN with v
#DEPRECATED|                key_v: The Key of v involved in the JOIN. You may pass None
#DEPRECATED|                           if v has no key.
#DEPRECATED|            Returns :
#DEPRECATED|                - either None iif u embeds a set of v instances
#DEPRECATED|                - either a Predicate instance which indicates how to join u and v
#DEPRECATED|            """
#DEPRECATED|            if len(fields_u) == 1 and list(fields_u)[0].is_array():
#DEPRECATED|                # u embed an array of element of type v, so there is
#DEPRECATED|                # no JOIN and thus no Predicate.
#DEPRECATED|                # Note that v do not even require to have a key
#DEPRECATED|                return None
#DEPRECATED|
#DEPRECATED|            # u and v can be joined
#DEPRECATED|            # This code only support Key made of only one Field
#DEPRECATED|            assert key_v,                       "Can't join with None key"
#DEPRECATED|            assert len(fields_u) == len(key_v), "Can't join fields = %r with key = %r" % (fields_u, key_v)
#DEPRECATED|            assert len(key_v) == 1,             "Composite key not supported: key = %r" % key_v
#DEPRECATED|
#DEPRECATED|            return Predicate(
#DEPRECATED|                "%s" % list(fields_u)[0].get_name(),
#DEPRECATED|                "==",
#DEPRECATED|                "%s" % list(key_v)[0].get_name()
#DEPRECATED|            )
#DEPRECATED|
#DEPRECATED|        #-----------------------------------------------------------------------
#DEPRECATED|
#DEPRECATED|        if u == v:
#DEPRECATED|            return
#DEPRECATED|
#DEPRECATED|        relations = u.get_relations(v)
#DEPRECATED|        if relations:
#DEPRECATED|            #Log.tmp("%s --> %s (%s)" % (u.get_name() , v.get_name(), relations))
#DEPRECATED|            self.graph.add_edge(u, v, relations=relations)
#DEPRECATED|            Log.debug("NEW EDGE %s" % self.format_arc(u, v))
#DEPRECATED|
#DEPRECATED|#        if relation_uv:
#DEPRECATED|#            (type, fields_u) = relation_uv
#DEPRECATED|#            key_v = list(v.get_keys())[0] if len(v.get_keys()) > 0 else None
#DEPRECATED|#
#DEPRECATED|#            # XXX Predicate and field_u are redundant, but fields are needed
#DEPRECATED|#            # for pruned tree while predicate only hold field names. Shall we
#DEPRECATED|#            # evolve predicates towards supporting Fields ?
#DEPRECATED|#            predicate = make_predicate(fields_u, key_v)
#DEPRECATED|#            self.graph.add_edge(u, v, relation=Relation(type, predicate))
#DEPRECATED|#            Log.debug("NEW EDGE %s" % self.format_arc(u, v))
#DEPRECATED|
#DEPRECATED|    def append(self, u):
#DEPRECATED|        """
#DEPRECATED|        Add a table node not yet in the DB graph and build the arcs
#DEPRECATED|        to connect this node to the existing node.
#DEPRECATED|        Args:
#DEPRECATED|            u: The Table instance we are adding to the graph.
#DEPRECATED|        """
#DEPRECATED|        # Adding the node u in the graph (if not yet in the graph) 
#DEPRECATED|        if u in self.graph.nodes():
#DEPRECATED|            raise ValueError("%r is already in the graph" % u)
#DEPRECATED|
#DEPRECATED|        self.graph.add_node(u)
#DEPRECATED|
#DEPRECATED|        # For each node v != u in the graph, check whether we can connect
#DEPRECATED|        # u to v and v to u 
#DEPRECATED|        for v, data in self.graph.nodes(True):
#DEPRECATED|            self.make_arc(u, v)
#DEPRECATED|            self.make_arc(v, u)
#DEPRECATED|
#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def format_arc(self, u, v):
#DEPRECATED|        """
#DEPRECATED|        Print a DbGraph arc
#DEPRECATED|        Args:
#DEPRECATED|            u: The source node (Table instance)
#DEPRECATED|            v: The target node (Table instance)
#DEPRECATED|        """
#DEPRECATED|        relations = self.get_relations(u,v)
#DEPRECATED|        relation_str = ', '.join(map(lambda r: "%r" % r, relations))
#DEPRECATED|        return "%r -> %r : %s" % (u, v, relation_str)
#DEPRECATED|
#DEPRECATED|    def plot(self):
#DEPRECATED|        """
#DEPRECATED|        Produce the graphviz file related to a DBGraph and show the graph
#DEPRECATED|        """
#DEPRECATED|        from matplotlib.pyplot import show
#DEPRECATED|        draw_graphviz(self.graph)
#DEPRECATED|        show()
#DEPRECATED|
#DEPRECATED|    #@returns(Table)
#DEPRECATED|    def find_node(self, table_name, get_parent=True):
#DEPRECATED|        """
#DEPRECATED|        Search a Table instance in the DbGraph for a given table name.
#DEPRECATED|        If several Table have the same name, it returns the parent Table.
#DEPRECATED|        Args:
#DEPRECATED|            table_name: A String value (the name of the table)
#DEPRECATED|            get_parent: A boolean.
#DEPRECATED|        Returns:
#DEPRECATED|            The corresponding Table instance, None if not found
#DEPRECATED|        """
#DEPRECATED|        for table in self.graph.nodes(False):
#DEPRECATED|            if table.get_name() == table_name:
#DEPRECATED|                if get_parent:
#DEPRECATED|                    # We need to check whether it has a parent with the same name
#DEPRECATED|                    for parent, _ in self.graph.in_edges(table):
#DEPRECATED|                        if parent.get_name() == table_name:
#DEPRECATED|                            return parent
#DEPRECATED|                return table
#DEPRECATED|        return None
#DEPRECATED|
#DEPRECATED|    @returns(set)
#DEPRECATED|    def get_table_names(self):
#DEPRECATED|        """
#DEPRECATED|        Retrieve the set of Table names belonging to this DBGraph.
#DEPRECATED|        Returns:
#DEPRECATED|            A set of String instances.
#DEPRECATED|        """
#DEPRECATED|        return set([table.get_name() for table in self.graph.nodes(False)])
#DEPRECATED|
#DEPRECATED|    @returns(bool)
#DEPRECATED|    def is_parent(self, table_or_table_name):
#DEPRECATED|        return not bool(self.get_parent(table_or_table_name))
#DEPRECATED|
#DEPRECATED|    @returns(Table)
#DEPRECATED|    def get_parent(self, table):
#DEPRECATED|        """
#DEPRECATED|        Retrieve the parent Table of a given Table. The parent
#DEPRECATED|        table is unique and has no predecessor having the same
#DEPRECATED|        table name.
#DEPRECATED|        Args:
#DEPRECATED|            table: A Table instance or a String instance.
#DEPRECATED|        Returns:
#DEPRECATED|            The parent Table. It may be table itself if it has
#DEPRECATED|            no parent Table.
#DEPRECATED|        """
#DEPRECATED|        if not isinstance(table, StringTypes):
#DEPRECATED|            table = self.find_node(table, get_parent = False)
#DEPRECATED|        for parent, x in self.graph.in_edges(table):
#DEPRECATED|            if parent.get_name() == table.get_name():
#DEPRECATED|                return parent
#DEPRECATED|        return None
#DEPRECATED|        
#DEPRECATED|    @returns(list)
#DEPRECATED|    def get_announce_tables(self):
#DEPRECATED|        """
#DEPRECATED|        The Table instances stored in this DBGraph that should be announced.
#DEPRECATED|        Returns:
#DEPRECATED|            A list of Table instances.
#DEPRECATED|        """
#DEPRECATED|        tables = list()
#DEPRECATED|        for table in self.graph.nodes(False):
#DEPRECATED|            # Ignore child tables with the same name as parents
#DEPRECATED|            keep = True
#DEPRECATED|            for parent, _ in self.graph.in_edges(table):
#DEPRECATED|                if parent.get_name() == table.get_name():
#DEPRECATED|                    keep = False
#DEPRECATED|            if keep:
#DEPRECATED|                fields = set(self.get_fields(table))
#DEPRECATED|                t = Table(table.get_platforms(), table.get_name(), fields, table.get_keys())
#DEPRECATED|                
#DEPRECATED|                # XXX We hardcode table capabilities
#DEPRECATED|                t.capabilities.retrieve   = True
#DEPRECATED|                t.capabilities.join       = True
#DEPRECATED|                t.capabilities.selection  = True
#DEPRECATED|                t.capabilities.projection = True
#DEPRECATED|
#DEPRECATED|                tables.append(t)
#DEPRECATED|        return tables
#DEPRECATED|
#DEPRECATED|    # Let's do a DFS by maintaining a prefix
#DEPRECATED|    def get_fields(self, root, prefix=''):
#DEPRECATED|        """
#DEPRECATED|        Produce edges in a depth-first-search starting at source.
#DEPRECATED|        """
#DEPRECATED|
#DEPRECATED|        def table_fields(table, prefix):
#DEPRECATED|            #return ["%s%s" % (prefix, f) for f in table.fields]
#DEPRECATED|            out = []
#DEPRECATED|            for f in table.fields.values():
#DEPRECATED|                # We will modify the fields of the Field object, hence we need
#DEPRECATED|                # to make a copy not to affect the original one
#DEPRECATED|                g = deepcopy(f)
#DEPRECATED|                g.field_name = "%s%s" % (prefix, f.get_name())
#DEPRECATED|                out.append(g)
#DEPRECATED|            return out
#DEPRECATED|
#DEPRECATED|        visited = set()
#DEPRECATED|
#DEPRECATED|        for f in table_fields(root, prefix):
#DEPRECATED|            yield f
#DEPRECATED|        visited.add(root)
#DEPRECATED|        stack = [(root, self.graph.edges_iter(root, data=True), prefix)]
#DEPRECATED|
#DEPRECATED|        # iterate considering edges ...
#DEPRECATED|        while stack:
#DEPRECATED|            parent,children,prefix = stack[-1]
#DEPRECATED|            try:
#DEPRECATED|                
#DEPRECATED|                parent, child, data = next(children)
#DEPRECATED|                relations = data['relations']
#DEPRECATED|                for relation in relations:
#DEPRECATED|                    if child not in visited:
#DEPRECATED|                        if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
#DEPRECATED|                            # Recursive call
#DEPRECATED|                            #for f in self.get_fields(child, "%s%s." % (prefix, child.get_name())):
#DEPRECATED|                            #    yield f
#DEPRECATED|                            pass
#DEPRECATED|                        else:
#DEPRECATED|                            # Normal JOINed table
#DEPRECATED|                            for f in table_fields(child, prefix):
#DEPRECATED|                                yield f
#DEPRECATED|                            visited.add(child)
#DEPRECATED|                            stack.append((child, self.graph.edges_iter(child, data=True), prefix))
#DEPRECATED|            except StopIteration:
#DEPRECATED|                stack.pop()
#DEPRECATED|
#DEPRECATED|    def get_relations(self, u, v):
#DEPRECATED|        # u --> v
#DEPRECATED|        if isinstance(u, StringTypes):
#DEPRECATED|            u = self.find_node(u)
#DEPRECATED|        if isinstance(v, StringTypes):
#DEPRECATED|            v = self.find_node(v)
#DEPRECATED|        return self.graph.edge[u][v]['relations']
#DEPRECATED|
#DEPRECATED|    def get_field_type(self, table, field_name):
#DEPRECATED|        return self.find_node(table).get_field_type(field_name)
#DEPRECATED|
#DEPRECATED|#DEPRECATED|     # FORGET ABOUT THIS METHOD, NOT USED ANYMORE
#DEPRECATED|#DEPRECATED|     def iter_tables(self, root):
#DEPRECATED|#DEPRECATED|         seen = []
#DEPRECATED|#DEPRECATED|         stack = [(None, root, None)]
#DEPRECATED|#DEPRECATED| 
#DEPRECATED|#DEPRECATED|         stack_11 = ()
#DEPRECATED|#DEPRECATED|         stack_1N = ()
#DEPRECATED|#DEPRECATED| 
#DEPRECATED|#DEPRECATED|         def iter_tables_rec(u, v, relation):
#DEPRECATED|#DEPRECATED|             # u = pred, v = current, relation(u->v)
#DEPRECATED|#DEPRECATED| 
#DEPRECATED|#DEPRECATED|             if v in seen: return
#DEPRECATED|#DEPRECATED|             seen.append(v)
#DEPRECATED|#DEPRECATED| 
#DEPRECATED|#DEPRECATED|             stack_11 += ((u, v, relation),)
#DEPRECATED|#DEPRECATED| 
#DEPRECATED|#DEPRECATED|             for neighbour in self.graph.successors(v):
#DEPRECATED|#DEPRECATED|                 for relation in self.get_relations(v, neighbour):
#DEPRECATED|#DEPRECATED|                     if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
#DEPRECATED|#DEPRECATED|                         # 1..N will be explored later, push on stack
#DEPRECATED|#DEPRECATED|                         stack_1N += ((v, neighbour, relation),)
#DEPRECATED|#DEPRECATED|                         continue
#DEPRECATED|#DEPRECATED|                     iter_tables_rec(v, neighbour, relation)
#DEPRECATED|#DEPRECATED| 
#DEPRECATED|#DEPRECATED|         iter_tables_rec(None, root, None)
#DEPRECATED|#DEPRECATED| 
#DEPRECATED|#DEPRECATED|         return (stack_11, stack_1N)
#DEPRECATED|
#DEPRECATED|
