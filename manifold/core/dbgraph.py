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

from copy                       import deepcopy
from networkx                   import draw_graphviz, DiGraph
#OBSOLETE|from networkx.algorithms.traversal.depth_first_search import dfs_edges
import matplotlib.pyplot        as plt

#OBSOLETE|from manifold.core.query          import Query 
from manifold.core.table          import Table 
from manifold.core.key            import Key
from manifold.util.type           import returns, accepts
from manifold.util.predicate      import Predicate 

# TODO DBGraph should inherit nx.DiGraph
class DBGraph:
    def __init__(self, tables, map_method_capabilities):
        """
        Maintains a JOIN graph between the different tables of the database
        """
        self.graph = DiGraph()
        for table in tables:
            self.append(table)
        self.map_method_capabilities = map_method_capabilities

    def get_capabilities(self, platform, method):
        return self.map_method_capabilities[(platform, method)]

    def make_arc(self, u, v):
        """
        \brief Connect a "u" Table to a "v" Table (if necessary) in the DbGraph
        \param u The source node (Table instance)
        \param v The target node (Table instance)
        """
        #-----------------------------------------------------------------------

        #returns(Predicate)
        #@accepts(set, Key)
        def make_predicate(fields_u, key_v):
            """
            \brief Compute the Predicate to JOIN a "u" Table with "v" Table
            \param fields_u The set of Field of u required to JOIN with v
            \param key_v The Key of v involved in the JOIN. You may pass None
                if v has no key.
            \return This function returns :
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

        relation_uv = u.get_relation(v)
        if relation_uv:
            (label, fields_u) = relation_uv
            key_v = list(v.get_keys())[0] if len(v.get_keys()) > 0 else None
            #print "EDGE", u, "      *** ", label, " ***      ", v
            self.graph.add_edge(u, v, {
                "cost"      : True,
                "type"      : label,                          # "-->" | "~~>"
                "info"      : fields_u,                       # set of Field
                "predicate" : make_predicate(fields_u, key_v) # None | Predicate
            })
            #print "PREDICATE IN DBGRAPH", make_predicate(fields_u, key_v) 
            #self.print_arc(u, v)

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

    def print_arc(self, u, v):
        """
        \brief Print a (u, v) arc
        \param u The source node (Table instance)
        \param v The target node (Table instance)
        """
        arc = self.graph[u][v]
        print "%r %s %r via %r" % (u, arc["type"], v, arc["info"])

    def plot(self):
        """
        \brief Produce de graphviz file related to this DBGraph and show the graph
        """
        DBGraph.plot_graph(self.graph)

    @staticmethod
    #@accepts(DBGraph)
    def plot_graph(graph):
        """
        \brief Produce de graphviz file related to a DBGraph and show the graph
        \param graph A DBGraph instance
        """
        draw_graphviz(graph)
        plt.show()

#OBSOLETE|    def get_tree_edges(self, root):
#OBSOLETE|        return [e for e in dfs_edges(self.graph, root)]

    def find_node(self, table_name):
        """
        \brief Search a Table instance in the DbGraph for a given table name
        \param table_name A String value (the name of the table)
        \return The corresponding Table instance, None if not found
        """
        for table in self.graph.nodes(False):
            if table.get_name() == table_name:
                # We need to check whether it has a parent with the same name
                for parent, _ in self.graph.in_edges(table):
                    if parent.get_name() == table_name:
                        return parent
                return table
        return None

    def get_announce_tables(self):
        tables = []
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
                if child not in visited:
                    if data['type'] == '1..N':
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

#OBSOLETE|    #@returns(Table)
#OBSOLETE|    def get_root(self, query):
#OBSOLETE|        """
#OBSOLETE|        \brief Find the root node related to the queried table (see query.get_from())
#OBSOLETE|        \param query A Query instance
#OBSOLETE|        \return The corresponding node (Table instance)
#OBSOLETE|        """
#OBSOLETE|        assert isinstance(query, Query), "Invalid query = %r (%r)" % (query, type(query))
#OBSOLETE|        root = [node[0] for node in self.graph.nodes(True) if node[0].get_name() == query.get_from()]
#OBSOLETE|
#OBSOLETE|        if not root:
#OBSOLETE|            raise Exception, "Cannot find root '%s' for query '%s'. Nodes available: %s" % (
#OBSOLETE|                query.get_from(),
#OBSOLETE|                query,
#OBSOLETE|                ["%r" % node for node in self.graph.nodes(True)]
#OBSOLETE|            )
#OBSOLETE|
#OBSOLETE|        assert len(root) == 1, "Invalid DBGraph. Candidate(s) root(s) = %r" % root
#OBSOLETE|        return root[0]

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
    
#OBSOLETE|    def get_edges(self):
#OBSOLETE|        return dfs_edges(self.graph)
#OBSOLETE|
#OBSOLETE|    def get_successors(self, node):
#OBSOLETE|        """
#OBSOLETE|        \param node A node belonging to this DBGraph
#OBSOLETE|        \return A list of Table instances which corresponds to successors
#OBSOLETE|           of "node" in this DBGraph.
#OBSOLETE|        """
#OBSOLETE|        return self.graph.successors(node)
#OBSOLETE|
#OBSOLETE|    @staticmethod
#OBSOLETE|    def prune_tree(tree_edges, nodes, fields):
#OBSOLETE|        """
#OBSOLETE|        \brief returned a tree pruned from nodes not providing any useful field
#OBSOLETE|        """
#OBSOLETE|        # XXX to be improved
#OBSOLETE|        # XXX if a leaf only provides a key, then we need to remove it also,
#OBSOLETE|        # since we already have the foreign key in an other table
#OBSOLETE|        tree = DiGraph(tree_edges)
#OBSOLETE|        # *** Compute the query plane ***
#OBSOLETE|        for node in tree.nodes():
#OBSOLETE|            data = nodes[node]
#OBSOLETE|            if 'visited' in data and data['visited']:
#OBSOLETE|                break;
#OBSOLETE|            node_fields = [f.field_name for f in node.fields]
#OBSOLETE|            if (set(fields) & set(node_fields)):
#OBSOLETE|                # mark all nodes until we reach the root (no pred) or a marked node
#OBSOLETE|                cur_node = node
#OBSOLETE|                # XXX DiGraph.predecessors_iter(n)
#OBSOLETE|                    #link = True
#OBSOLETE|                while True:
#OBSOLETE|                    if 'visited' in data and data['visited']:
#OBSOLETE|                        break
#OBSOLETE|                    data['visited'] = True
#OBSOLETE|                    pred = tree.predecessors(cur_node)
#OBSOLETE|                    if not pred:
#OBSOLETE|                        break
#OBSOLETE|                    cur_node = pred[0]
#OBSOLETE|                    data = nodes[cur_node]
#OBSOLETE|        visited_tree_edges = [e for e in tree_edges if 'visited' in nodes[e[0]] and 'visited' in nodes[e[1]]]
#OBSOLETE|        for node in tree.nodes():
#OBSOLETE|            if 'visited' in nodes[node]:
#OBSOLETE|                del nodes[node]['visited']
#OBSOLETE|        return DiGraph(visited_tree_edges)
#OBSOLETE|
    
