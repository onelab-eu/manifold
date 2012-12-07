import networkx as nx
import matplotlib.pyplot as plt
from networkx.algorithms.traversal.depth_first_search import dfs_tree, dfs_edges
import copy

class DBGraph:
    def __init__(self, tables):
        """
        Maintains a JOIN graph between the different tables of the database
        """
        self.graph = nx.DiGraph()
        self.tables = tables # XXX remove
        for table in tables:
            self.append(table)

    def append(self, table):
        """
        \brief Add a table node not yet in the DB graph and build the arcs
            to connect this node to the existing node.
            - (u->v):1-1 is build is u provide a key of v
            - (u->v):1-N is build is u has a field of type v
        \param table The table we insert into the graph.
        """
        # Check table not yet in graph
        if table in self.graph.nodes():
            raise ValueError("%r is already in the graph" % table)

        # Add the "table" node to the graph and attach the corresponding sources nodes
        sources = [t for t in self.tables if list(table.keys)[0] in t.get_fields_from_keys()]
        print "DBGRAPH APPEND", table
        # It seems sources are useless now...
        self.graph.add_node(table, {'sources': sources})

        # We loop through the different _nodes_ of the graph to see whether
        # we need to establish some links
        for node, data in self.graph.nodes(True):
            # Ignore the node we've just added
            if node == table: # or set(node.keys) & set(table.keys):
                continue

            if node.determines(table):
                print "append(): %r --> %r" % (node, table)
                self.graph.add_edge(node, table, {'cost': True, 'type': None})
            elif table.determines(node):
                print "append(): %r --> %r" % (table, node)
                self.graph.add_edge(table, node, {'cost': True, 'type': None})
            elif node.includes(table):
                print "append(): %r ==> %r" % (node, table)
                self.graph.add_edge(node, table, {'cost': True, 'type': None})
            elif table.includes(node):
                print "append(): %r ==> %r" % (table, node)
                self.graph.add_edge(table, node, {'cost': True, 'type': None})
            elif node.provides(table):
                print "append(): %r ~~> %r" % (node, table)
                self.graph.add_edge(node, table, {'cost': True, 'type': None})
            elif table.provides(node):
                print "append(): %r ~~> %r" % (table, node)
                self.graph.add_edge(table, node, {'cost': True, 'type': None})

    def plot(self):
        DBGraph.plot_graph(self.graph)

    @staticmethod
    def plot_graph(graph):
        nx.draw_graphviz(graph)
        plt.show()

    def get_tree_edges(self, root):
        return [e for e in dfs_edges(self.graph, root)]

    def get_root(self, query):
        """
        Extract the query tree rooted at the fact table
        """
        root = [node[0] for node in self.graph.nodes(True) if node[0].name == query.fact_table]
        if not root:
            raise Exception, "Cannot find root '%s' for query '%s'. Nodes available: %s" % (
                query.fact_table,
                query,
                ["%s::%s" % (node[0].platform, node[0].name) for node in self.graph.nodes(True)]
            )
        return root[0]

    def get_tree_edges(self, root):
        edges = dfs_edges(self.graph, root)
        if not edges:
            raise Exception, "Cannot build tree for query %s" % query
        return edges

    #def prune_query_tree(tree, tree_edges, nodes, query_fields):

    @staticmethod
    def prune_tree(tree_edges, nodes, fields):
        # XXX
        # XXX need improvements
        # XXX
        tree = nx.DiGraph(tree_edges)
        # *** Compute the query plane ***
        for node in tree.nodes():
            data = nodes[node]
            if 'visited' in data and data['visited']:
                break;
            if (set(fields) & set(node.fields)):
                # mark all nodes until we reach the root (no pred) or a marked node
                cur_node = node
                # XXX DiGraph.predecessors_iter(n)
                    #link = True
                while True:
                    if 'visited' in data and data['visited']:
                        break
                    data['visited'] = True
                    pred = tree.predecessors(cur_node)
                    if not pred:
                        break
                    cur_node = pred[0]
                    data = nodes[cur_node]
        visited_tree_edges = [e for e in tree_edges if 'visited' in nodes[e[0]] and 'visited' in nodes[e[1]]]
        for node in tree.nodes():
            if 'visited' in nodes[node]:
                del nodes[node]['visited']
        return visited_tree_edges

    # Let's do a DFS by maintaining a prefix
    def get_fields(self, root, prefix=''):
        """
        Produce edges in a depth-first-search starting at source.
        """

        def table_fields(table, prefix):
            #return ["%s%s" % (prefix, f) for f in table.fields]
            out = []
            for f in table.fields:
                # We will modify the fields of the Field object, hence we need
                # to make a copy not to affect the original one
                g = copy.deepcopy(f)
                g.field_name = "%s%s" % (prefix, f.field_name)
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
                        for f in self.get_fields(child, "%s%s." % (prefix, child.name)):
                            yield f
                    else:
                        # Normal JOINed table
                        for f in table_fields(child, prefix):
                            yield f
                        visited.add(child)
                        stack.append((child, self.graph.edges_iter(child, data=True), prefix))
            except StopIteration:
                stack.pop()

       
           
                               


