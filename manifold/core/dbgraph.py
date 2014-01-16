#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A DBGraph represent a DB schema, where each node represents a Table
# and where a (u, v) arc means that Tables u and v can be joined
# (u left join v).
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# NOTE: The fastest way to traverse all edges of a graph is via
# adjacency_iter(), but the edges() method is often more convenient.

from copy                       import deepcopy
from networkx                   import draw_graphviz, DiGraph
from traceback                  import format_exc
from types                      import StringTypes

from manifold.core.capabilities import Capabilities
from manifold.core.method       import Method
from manifold.core.key          import Key, Keys
from manifold.core.relation     import Relation
from manifold.core.table        import Table 
from manifold.util.predicate    import Predicate 
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

# TODO DBGraph should inherit nx.DiGraph
class DBGraph(object):
    def check_init(tables, map_method_capabilities):
        """
        (Internal usage) Check DbGraph::__init__() parameters consistency.
        """
        table_names_1 = set([table.get_name()  for table  in tables])
        table_names_2 = set([method.get_name() for method in map_method_capabilities.keys()])
        assert table_names_1 == table_names_2,\
            "Inconsistency between:\n- table_names_1 = %s\n- table_names_2 = %s" % (
                table_names_1,
                table_names_2
            )

    def __init__(self, tables, map_method_capabilities):
        """
        Maintains a JOIN graph between the different tables of the database
        """
        self.graph = DiGraph()
        for table in tables:
            self.append(table)
        self._map_method_capabilities = map_method_capabilities

    @returns(Capabilities)
    def get_capabilities(self, platform_name, table_name):
        """
        Retrieve Capabilities related to a given Table provided by
        a given platform.
        Args:
            platform_name: The name of a Manifold platform.
            table_name: The name of the Table.
        Returns:
            The corresponding Capabilities/
        """
        method = Method(platform_name, table_name)
        try:
            return self._map_method_capabilities[method]
        except KeyError, e:
            Log.error("DbGraph::get_capabilities(): Cannot find method %s in {%s}" % (
                method,
                ", ".join(["%s" % m for m in self._map_method_capabilities.keys()]))
            )
            raise e

    @returns(Keys)
    def get_key(self, method):
        """
        Retrieve Key instances related to a given Method.
        Args:
            method: A Method instance identifying a Table related
                to a given platform in this DbGraph.
        """
        self.find_node(method).get_keys()

    def make_arc(self, u, v):
        """
        Connect a "u" Table to a "v" Table (if necessary) in the DbGraph
        Args:
            u: The source node (Table instance)
            v: The target node (Table instance)
        """
        #-----------------------------------------------------------------------

        returns(Predicate)
        @accepts(set, Key)
        def make_predicate(fields_u, key_v):
            """
            Compute the Predicate to JOIN a "u" Table with "v" Table
            Args:
                fields_u: The set of Field of u required to JOIN with v
                key_v: The Key of v involved in the JOIN. You may pass None
                           if v has no key.
            Returns :
                - either None iif u embeds a set of v instances
                - either a Predicate instance which indicates how to join u and v
            """
            if len(fields_u) == 1 and list(fields_u)[0].is_array():
                # u embed an array of element of type v, so there is
                # no JOIN and thus no Predicate.
                # Note that v do not even require to have a key
                return None

            # u and v can be joined
            # This code only support Key made of only one Field
            assert key_v,                       "Can't join with None key"
            assert len(fields_u) == len(key_v), "Can't join fields = %r with key = %r" % (fields_u, key_v)
            assert len(key_v) == 1,             "Composite key not supported: key = %r" % key_v

            return Predicate(
                "%s" % list(fields_u)[0].get_name(),
                "==",
                "%s" % list(key_v)[0].get_name()
            )

        #-----------------------------------------------------------------------

        if u == v:
            return

        relations = u.get_relations(v)
        if relations:
            self.graph.add_edge(u, v, relations=relations)
            Log.debug("NEW EDGE %s" % self.format_arc(u, v))
            print("NEW EDGE %s" % self.format_arc(u, v))

#        if relation_uv:
#            (type, fields_u) = relation_uv
#            key_v = list(v.get_keys())[0] if len(v.get_keys()) > 0 else None
#
#            # XXX Predicate and field_u are redundant, but fields are needed
#            # for pruned tree while predicate only hold field names. Shall we
#            # evolve predicates towards supporting Fields ?
#            predicate = make_predicate(fields_u, key_v)
#            self.graph.add_edge(u, v, relation=Relation(type, predicate))
#            Log.debug("NEW EDGE %s" % self.format_arc(u, v))

    def append(self, u):
        """
        \brief Add a table node not yet in the DB graph and build the arcs
            to connect this node to the existing node.
            There are 3 types of arcs (determines, includes, provides)
        \sa manifold.util.table.py
        \param u The Table instance we are adding to the graph.
        """
        # Adding the node u in the graph (if not yet in the graph) 
        if u in self.graph.nodes():
            raise ValueError("%r is already in the graph" % u)
        self.graph.add_node(u)

        # For each node v != u in the graph, check whether we can connect
        # u to v and v to u 
        for v, data in self.graph.nodes(True):
            self.make_arc(u, v)
            self.make_arc(v, u)

    @returns(StringTypes)
    def format_arc(self, u, v):
        """
        Print a DbGraph arc
        Args:
            u: The source node (Table instance)
            v: The target node (Table instance)
        """
        relations = self.get_relations(u,v)
        relation_str = ', '.join(map(lambda r: "%r" % r, relations))
        return "%r -> %r : %s" % (u, v, relation_str)

    def plot(self):
        """
        Produce the graphviz file related to a DBGraph and show the graph
        """
        from matplotlib.pyplot import show
        draw_graphviz(self.graph)
        show()

    @returns(Table)
    def find_node(self, table_name, get_parent=True):
        """
        Search a Table instance in the DbGraph for a given table name.
        If several Table have the same name, it returns the parent Table.
        Args:
            table_name: A String value (the name of the table)
        Returns:
            The corresponding Table instance, None if not found
        """
        for table in self.graph.nodes(False):
            if table.get_name() == table_name:
                if get_parent:
                    # We need to check whether it has a parent with the same name
                    for parent, _ in self.graph.in_edges(table):
                        if parent.get_name() == table_name:
                            return parent
                return table
        return None

    @returns(list)
    def get_table_names(self):
        """
        Retrieve the list of Table names belonging to this DBGraph.
        Returns:
            A list of String instances.
        """
        return [table.get_name() for table in self.graph.nodes(False)]

    @returns(bool)
    def is_parent(self, table_or_table_name):
        return not bool(self.get_parent(table_or_table_name))

    @returns(Table)
    def get_parent(self, table_or_table_name):
        if not isinstance(table_or_table_name, Table):
            table_or_table_name = self.find_node(table_or_table_name, get_parent=False)
        for parent, x in self.graph.in_edges(table_or_table_name):
            if parent.get_name() == table_or_table_name.get_name():
                return parent
        return None
        
    @returns(list)
    def get_announce_tables(self):
        tables = list()
        for table in self.graph.nodes(False):
            # Ignore child tables with the same name as parents
            keep = True
            for parent, _ in self.graph.in_edges(table):
                if parent.get_name() == table.get_name():
                    keep = False
            if keep:
                tables.append(Table(None, None, table.get_name(), set(self.get_fields(table)), table.get_keys()))
        return tables

    # Let's do a DFS by maintaining a prefix
    def get_fields(self, root, prefix=''):
        """
        Produce edges in a depth-first-search starting at source.
        """

        def table_fields(table, prefix):
            #return ["%s%s" % (prefix, f) for f in table.fields]
            out = []
            for f in table.fields.values():
                # We will modify the fields of the Field object, hence we need
                # to make a copy not to affect the original one
                g = deepcopy(f)
                g.field_name = "%s%s" % (prefix, f.get_name())
                out.append(g)
            return out

        visited = set()

        for f in table_fields(root, prefix):
            yield f
        visited.add(root)
        stack = [(root, self.graph.edges_iter(root, data=True), prefix)]

        # iterate considering edges ...
        while stack:
            parent,children,prefix = stack[-1]
            try:
                
                parent, child, data = next(children)
                relation = data['relations']
                if child not in visited:
                    if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
                        # Recursive call
                        #for f in self.get_fields(child, "%s%s." % (prefix, child.get_name())):
                        #    yield f
                        pass
                    else:
                        # Normal JOINed table
                        for f in table_fields(child, prefix):
                            yield f
                        visited.add(child)
                        stack.append((child, self.graph.edges_iter(child, data=True), prefix))
            except StopIteration:
                stack.pop()

    def get_relations(self, u, v):
        # u --> v
        if isinstance(u, StringTypes):
            u = self.find_node(u)
        if isinstance(v, StringTypes):
            v = self.find_node(v)
        return self.graph.edge[u][v]['relations']

    def get_field_type(self, table, field_name):
        return self.find_node(table).get_field_type(field_name)

#DEPRECATED|     # FORGET ABOUT THIS METHOD, NOT USED ANYMORE
#DEPRECATED|     def iter_tables(self, root):
#DEPRECATED|         seen = []
#DEPRECATED|         stack = [(None, root, None)]
#DEPRECATED| 
#DEPRECATED|         stack_11 = ()
#DEPRECATED|         stack_1N = ()
#DEPRECATED| 
#DEPRECATED|         def iter_tables_rec(u, v, relation):
#DEPRECATED|             # u = pred, v = current, relation(u->v)
#DEPRECATED| 
#DEPRECATED|             if v in seen: return
#DEPRECATED|             seen.append(v)
#DEPRECATED| 
#DEPRECATED|             stack_11 += ((u, v, relation),)
#DEPRECATED| 
#DEPRECATED|             for neighbour in self.graph.successors(v):
#DEPRECATED|                 for relation in self.get_relations(v, neighbour):
#DEPRECATED|                     if relation.get_type() in [Relation.types.LINK_1N, Relation.types.LINK_1N_BACKWARDS]:
#DEPRECATED|                         # 1..N will be explored later, push on stack
#DEPRECATED|                         stack_1N += ((v, neighbour, relation),)
#DEPRECATED|                         continue
#DEPRECATED|                     iter_tables_rec(v, neighbour, relation)
#DEPRECATED| 
#DEPRECATED|         iter_tables_rec(None, root, None)
#DEPRECATED| 
#DEPRECATED|         return (stack_11, stack_1N)

# TODO This should be a method of DBGraph and DBGraph should inherits DiGraph
@accepts(DiGraph)
def find_root(tree):
    """
    \brief Search the root node of a tree
    \param tree A DiGraph instance representing a tree
    \return The corresponding root node, None if not found
    """
    for u in tree.nodes():
        if not tree.in_edges(u):
            # The root is the only node with no incoming edge
            return u
    return None
    

