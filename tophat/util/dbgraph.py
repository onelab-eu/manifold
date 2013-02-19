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
        self.tables = tables # XXX remove
        for table in tables:
            self.append(table)

    def append(self, u):
        """
        \brief Add a table node not yet in the DB graph and build the arcs
            to connect this node to the existing node.
            There are 3 types of arcs (determines, includes, provides)
        \sa tophat/util/table.py
        \param u The table we insert into the graph.
        """
        # Check table not yet in graph
        if u in self.graph.nodes():
            raise ValueError("%r is already in the graph" % u)

        # Add the "u" node to the graph and attach the corresponding sources nodes
        sources = [t for t in self.tables if list(u.keys)[0] in t.get_keys()]
        # It seems sources are useless now...
        self.graph.add_node(u, {'sources': sources})

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
                self.graph.add_edge(u, v, {'cost': True, 'type': label, 'info': fields_u})


            # v -?-> u
            relation_vu = v.get_relation(u)
            if relation_vu:
                (label, fields_v) = relation_vu
                print "%r %s %r" % (v, label, u)
                self.graph.add_edge(v, u, {'cost': True, 'type': label, 'info': fields_v})

#            if u.is_connected_to(v):
#                self.graph.add_edge(u, v, {'cost': True, 'type': label, 'info': fields_u})
#            if v.is_connected_to(u):
#                self.graph.add_edge(v, u, {'cost': True, 'type': label, 'info': fields_v})


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

    #def prune_query_tree(tree, tree_edges, nodes, query_fields):

#    @staticmethod
#    def prune_tree(tree_edges, nodes, fields):
#        """
#        \brief returned a tree pruned from nodes not providing any useful field
#        """
#        # XXX to be improved
#        # XXX if a leaf only provides a key, then we need to remove it also,
#        # since we already have the foreign key in an other table
#        tree = nx.DiGraph(tree_edges)
#        # *** Compute the query plane ***
#        for node in tree.nodes():
#            data = nodes[node]
#            if 'visited' in data and data['visited']:
#                break;
#            node_fields = [f.field_name for f in node.fields]
#            if (set(fields) & set(node_fields)):
#                # mark all nodes until we reach the root (no pred) or a marked node
#                cur_node = node
#                # XXX DiGraph.predecessors_iter(n)
#                    #link = True
#                while True:
#                    if 'visited' in data and data['visited']:
#                        break
#                    data['visited'] = True
#                    pred = tree.predecessors(cur_node)
#                    if not pred:
#                        break
#                    cur_node = pred[0]
#                    data = nodes[cur_node]
#        visited_tree_edges = [e for e in tree_edges if 'visited' in nodes[e[0]] and 'visited' in nodes[e[1]]]
#        for node in tree.nodes():
#            if 'visited' in nodes[node]:
#                del nodes[node]['visited']
#        return nx.DiGraph(visited_tree_edges)
#
#    # Let's do a DFS by maintaining a prefix
#    def get_fields(self, root, prefix=''):
#        """
#        Produce edges in a depth-first-search starting at source.
#        """
#
#        def table_fields(table, prefix):
#            #return ["%s%s" % (prefix, f) for f in table.fields]
#            out = []
#            for f in table.fields:
#                # We will modify the fields of the Field object, hence we need
#                # to make a copy not to affect the original one
#                g = deepcopy(f)
#                g.field_name = "%s%s" % (prefix, f.field_name)
#                out.append(g)
#            return out
#
#        visited = set()
#
#        for f in table_fields(root, prefix):
#            yield f
#        visited.add(root)
#        stack = [(root, self.graph.edges_iter(root, data=True), prefix)]
#
#        # iterate considering edges ...
#        while stack:
#            parent,children,prefix = stack[-1]
#            try:
#                
#                parent, child, data = next(children)
#                if child not in visited:
#                    if data['type'] == '1..N':
#                        # Recursive call
#                        for f in self.get_fields(child, "%s%s." % (prefix, child.name)):
#                            yield f
#                    else:
#                        # Normal JOINed table
#                        for f in table_fields(child, prefix):
#                            yield f
#                        visited.add(child)
#                        stack.append((child, self.graph.edges_iter(child, data=True), prefix))
#            except StopIteration:
#                stack.pop()
