import networkx          as nx
import matplotlib.pyplot as plt
from networkx.algorithms.traversal.depth_first_search import dfs_edges
from copy                                             import deepcopy

class DBGraph:
    def __init__(self, tables):
        """
        Maintains a JOIN graph between the different tables of the database
        """
        self.graph = nx.DiGraph()
        for table in tables:
            self.append(table)

    def append(self, u):
        """
        \brief Add a table node not yet in the DB graph and build the arcs
            to connect this node to the existing node.
            There are 3 types of arcs (determines, includes, provides)
        \sa tophat/util/table.py
        \param u The Table instance we are adding to the graph.
        """
        # Check table not yet in graph
        if u in self.graph.nodes():
            raise ValueError("%r is already in the graph" % u)
        self.graph.add_node(u)

        # We loop through the different _nodes_ of the graph to see whether
        # we need to establish some links
        for v, data in self.graph.nodes(True):
            # Ignore the node we've just added
            if u == v:
                continue

            # u -?-> v 
            relation_uv = u.get_relation(v)
            if relation_uv:
                (label, fields_u) = relation_uv
                print "%r %s %r" % (u, label, v)
                self.graph.add_edge(u, v, {"cost": True, "type": label, "info": fields_u})

            # v -?-> u
            relation_vu = v.get_relation(u)
            if relation_vu:
                (label, fields_v) = relation_vu
                print "%r %s %r" % (v, label, u)
                self.graph.add_edge(v, u, {"cost": True, "type": label, "info": fields_v})

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
        root = [node[0] for node in self.graph.nodes(True) if node[0].get_name() == query.fact_table]
        if not root:
            raise Exception, "Cannot find root '%s' for query '%s'. Nodes available: %s" % (
                query.fact_table,
                query,
                ["%r" % node for node in self.graph.nodes(True)]
            )
        return root[0]

    def get_edges(self):
        return dfs_edges(self.graph)

    def get_successors(self, node):
        """
        \param node A node belonging to this DBGraph
        \return A list of Table instances which corresponds to successors
           of "node" in this DBGraph.
        """
        return self.graph.successors(node)

#OBSOLETE|    @staticmethod
#OBSOLETE|    def prune_tree(tree_edges, nodes, fields):
#OBSOLETE|        """
#OBSOLETE|        \brief returned a tree pruned from nodes not providing any useful field
#OBSOLETE|        """
#OBSOLETE|        # XXX to be improved
#OBSOLETE|        # XXX if a leaf only provides a key, then we need to remove it also,
#OBSOLETE|        # since we already have the foreign key in an other table
#OBSOLETE|        tree = nx.DiGraph(tree_edges)
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
#OBSOLETE|        return nx.DiGraph(visited_tree_edges)
#OBSOLETE|
#OBSOLETE|    # Let's do a DFS by maintaining a prefix
#OBSOLETE|    def get_fields(self, root, prefix=''):
#OBSOLETE|        """
#OBSOLETE|        Produce edges in a depth-first-search starting at source.
#OBSOLETE|        """
#OBSOLETE|
#OBSOLETE|        def table_fields(table, prefix):
#OBSOLETE|            #return ["%s%s" % (prefix, f) for f in table.fields]
#OBSOLETE|            out = []
#OBSOLETE|            for f in table.fields:
#OBSOLETE|                # We will modify the fields of the Field object, hence we need
#OBSOLETE|                # to make a copy not to affect the original one
#OBSOLETE|                g = deepcopy(f)
#OBSOLETE|                g.field_name = "%s%s" % (prefix, f.field_name)
#OBSOLETE|                out.append(g)
#OBSOLETE|            return out
#OBSOLETE|
#OBSOLETE|        visited = set()
#OBSOLETE|
#OBSOLETE|        for f in table_fields(root, prefix):
#OBSOLETE|            yield f
#OBSOLETE|        visited.add(root)
#OBSOLETE|        stack = [(root, self.graph.edges_iter(root, data=True), prefix)]
#OBSOLETE|
#OBSOLETE|        # iterate considering edges ...
#OBSOLETE|        while stack:
#OBSOLETE|            parent,children,prefix = stack[-1]
#OBSOLETE|            try:
#OBSOLETE|                
#OBSOLETE|                parent, child, data = next(children)
#OBSOLETE|                if child not in visited:
#OBSOLETE|                    if data['type'] == '1..N':
#OBSOLETE|                        # Recursive call
#OBSOLETE|                        for f in self.get_fields(child, "%s%s." % (prefix, child.get_name())):
#OBSOLETE|                            yield f
#OBSOLETE|                    else:
#OBSOLETE|                        # Normal JOINed table
#OBSOLETE|                        for f in table_fields(child, prefix):
#OBSOLETE|                            yield f
#OBSOLETE|                        visited.add(child)
#OBSOLETE|                        stack.append((child, self.graph.edges_iter(child, data=True), prefix))
#OBSOLETE|            except StopIteration:
#OBSOLETE|                stack.pop()
