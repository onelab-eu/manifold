import os, sys
import xml.etree.cElementTree as ElementTree

from copy import deepcopy
import traceback
import threading
from twisted.internet import defer

import networkx as nx
import matplotlib.pyplot as plt
from networkx.algorithms.traversal.depth_first_search import dfs_tree, dfs_edges

from tophat.util.xmldict import *
from tophat.util.reactor_thread import ReactorThread
from tophat.core.filter import Filter, Predicate
from tophat.core.param import Param
from tophat.router import *
from tophat.core.sourcemgr import SourceManager
from tophat.gateways import *
from tophat.core.ast import AST

#from tophat.models import session, Platform

class ParameterError(StandardError): pass

UNIT_COST = 1

class Callback:
    def __init__(self, deferred=None, event=None):
        self.results = []
        self._deferred = deferred
        self.event=event

    def __call__(self, value):
        if not value:
            if self._deferred:
                self._deferred.callback(self.results)
            else:
                self.event.set()
            return
        # XXX What if we have multiple queries in parallel ?
        # we need to stored everything in separated lists
        self.results.append(value)

class Table:
    """
    Implements a database table schema.
    """

    def __init__(self, platform, name, fields, keys, partition=None, cost=1):
        self.platform = platform
        self.name = name
        self.fields = fields
        self.keys = keys
        self.partition = partition # an instance of a Filter
        # There will also be a list that the platform cannot provide, cf sources[i].fields
        self.cost = cost
        if isinstance(self.keys, (list, tuple)):
            self.keys = frozenset(self.keys)
        if isinstance(self.fields, (list, tuple)):
            self.fields = frozenset(self.fields)

    def __str__(self):
        #return "<Table name='%s' platform='%s' fields='%r' keys='%r'>" % (self.name, self.platform, self.fields, self.keys)
        if self.platform:
            return "%s::%s" % (self.platform, self.name)
        else:
            return self.name

    def get_fields_from_keys(self):
        fields = []
        for k in self.keys:
            if isinstance(k, (list, tuple)):
                fields.extend(list(k))
            else:
                fields.append(k)
        return fields


class THQuery(Query):
    """
    Implements a TopHat query.

    We assume this is a correct DAG specification.

    1/ A field designates several tables = OR specification.
    2/ The set of fields specifies a AND between OR clauses.
    """

    def __init__(self, *args, **kwargs):
        l = len(kwargs.keys())

        # Initialization from a tuple
        if len(args) in range(2,6) and type(args) == tuple:
            # Note: range(x,y) <=> [x, y[
            self.action, self.fact_table, self.filters, self.params, self.fields = args

        # Initialization from a dict (action & fact_table are mandatory)
        elif 'action' in kwargs  and 'fact_table' in kwargs:
            self.action = kwargs['action']
            del kwargs['action']
            self.fact_table = kwargs['fact_table']
            del kwargs['fact_table']

            if 'filters' in kwargs:
                self.filters = kwargs['filters']
                del kwargs['filters']
            else:
                self.filters = None

            if 'fields' in kwargs:
                self.fields = kwargs['fields']
                del kwargs['fields']
            else:
                self.fields = None

            if 'params' in kwargs:
                self.params = kwargs['params']
                del kwargs['params']
            else:
                self.params = None

            if kwargs:
                raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
                return
        else:
                raise ParameterError, "No valid constructor found for %s" % self.__class__.__name__

        # Processing filters
        if isinstance(self.filters, list):
            self.filters = Filter.from_list(self.filters)
        elif isinstance(self.filters, dict):
            self.filters = Filter.from_dict(self.filters)

        # Processing params
        if isinstance(self.params, dict):
            self.params = Param(self.params)

    def get_tuple(self):
        return (self.action, self.fact_table, self.filters, self.params, self.fields)

    def __str__(self):
        return "<THQuery action='%s' fact_table='%s' filters='%s' params='%s' fields='%r'>" % self.get_tuple()


class THDestination(Destination, THQuery):
    """
    Implements a destination in TopHat == a query
    """
    
    def __str__(self):
        return "<THDestination / THQuery: %s" % self.query



class THRoute(Route):
    """
    Implements a TopHat route.
    """

    def __init__(self, destination, peer, cost, timestamp):

        if type(destination) != THDestination:
            raise TypeError("Destination of type %s expected in argument. Got %s" % (type(destination), THDestination))

        # Assert the route corresponds to an existing peer
        # Assert the cost corresponds to a right cost
        # Eventually the timestamp would not be a parameter but assigned

        super(THRoute, self).__init__(self, destination, peer, cost, timestamp)

    def push(identifier, record):
        pass

class THCost(int):
    """
    Let's use (N, min,  +) semiring for cost
    """

    def __add__(self, other):
        return THCost(min(self, other))

    def __mul__(self, other):
        return THCost(self + other)

class THLocalRouter(LocalRouter):
    """
    Implements a TopHat router.

    Specialized to handle THAnnounces/THRoutes, ...
    """

    def __init__(self):
        self.reactor = ReactorThread()
        self.sourcemgr = SourceManager(self.reactor)
        LocalRouter.__init__(self, Table, object)
        self.event = threading.Event()

    def __enter__(self):
        self.reactor.startReactor()
        return self

    def __exit__(self, type, value, traceback):
        self.reactor.stopReactor()
        print "I: Reactor thread stopped. Waiting for thread to terminate..."
        self.reactor.join()

    def import_file(self, metadata):
        #print "I: Processing %s" % metadata
        tree = ElementTree.parse(metadata)
        root = tree.getroot()
        md = XmlDictConfig(root)
        
        # Checking the presence of a platform section
        if not 'platform' in md:
            raise Exception, "Error importing metadata file '%s': no platform specified" % metadata
        p_dict = md['platform']
        platform = p_dict['platform']

        # Update the peer/session table with platform and associated configuration
        # XXX
#        # Let's store gateway related information into the database too
#        gateways = MetadataGateways(self.api, {'platform': p['platform']})
#
#        # We add the platform name to the configuration
#        config = md['gateway']
#        config['platform'] = p['platform']
#
#        if not gateways:
#            print "I: Inserting new gateway for platform '%(platform)s'..." % p
#            g_dict = {
#                'config': json.dumps(config),
#                'platform_id': p['platform_id']
#            }
#            g = MetadataGateway(self.api, g_dict)
#            g.sync()
#        else:
#            print "I: Existing gateway for platform  '%(platform)s'. Updating..." % p
#            g = gateways[0]
#            g['config'] = json.dumps(config)
#            g.sync()

        # Checking the presence of a method section
        if not 'methods' in md:
            raise Exception, "Error importing metadata file '%s': no method section specified" % metadata
        methods = md['methods']

        # Checking the presence of at least a method
        if not 'method' in methods:
            raise Exception, "Error importing metadata file '%s': no method specified" % metadata
        methods = methods['method']

        if not isinstance(methods, list):
            methods = [methods]

        # Looping through the methods
        for method in methods:
            
            aliases = method['name'].split('|')

            #base = ['%s::%s' % (p_dict['platform'], aliases[0])]
            #base.extend(aliases)

            # XXX we currently restrict ourselves to the main alias 'nodes'
            tmp = [a for a in aliases if a == 'nodes']
            name = tmp[0] if tmp else aliases[0]

            # Checking the presence of a field section
            if not 'fields' in method:
                raise Exception, "Error importing metadata file '%s': no field section" % metadata
            field_arr = method['fields']
            # Checking the presence of at least a field
            if not 'field' in field_arr:
                raise Exception, "Error importing metadata file '%s': no field specified" % metadata

            # FIXME Currently we ignore detailed information about the fields
            if not isinstance(field_arr['field'], list):
                field_arr['field'] = [field_arr['field']]
            fields = [f['field'] for f in field_arr['field']]

            # Checking the presence of a keys section
            if not 'keys' in method:
                raise Exception, "Error importing metadata file  '%s': no key section" % metadata
            key_arr = method['keys']
            # Checking the presence of at least a key
            if not 'key' in key_arr:
                raise Exception, "Error importing metadata file '%s': no key specified" % metadata
            if not isinstance(key_arr['key'], list):
                keys = [key_arr['key']]
            else:
                keys = key_arr['key']
            
            # Creating a new Table for inserting into the RIB
            t = Table(platform, name, fields, keys)
            
            #print "Adding %s::%s to RIB" % (platform, name)
            self.rib[t] = platform

    def get_gateway(self, platform, table, fields):
        config_tophat = {
            'url': 'http://api.top-hat.info/API/'
        }
        config_myslice = {
            'url': 'http://api.myslice.info/API/'
        }
        config_ple = {
            'auth': 'ple.upmc',
            'user': 'ple.upmc.slicebrowser',
            'sm': 'http://www.planet-lab.eu:12347/',
            'registry': 'http://www.planet-lab.eu:12345/',
            'user_private_key': '/var/myslice/myslice.pkey',
            'caller': {'email': 'demo'}
        }

        query = THQuery('get', table, [], {}, fields)
        class_map = { 'ple': (SFA, config_ple) , 'tophat': (XMLRPC, config_tophat), 'myslice': (XMLRPC, config_myslice) }

        try:
            cls, conf = class_map[platform]
            return cls(platform, query, conf)
        except KeyError, key:
            raise Exception, "Platform missing '%s'" % key

    def get_static_routes(self, directory):
        for root, dirs, files in os.walk(directory):
            for d in dirs[:]:
                if d[0] == '.':
                    dirs.remove(d)
            metadata = [f for f in files if f[-3:] == 'xml']
            for m in metadata:
                self.import_file(os.path.join(root, m))
        

    def get_platform_max_fields(self, fields, join):
        # Search for the platform::method that allows for the largest number of missing fields
        _fields = [f.split('.')[0] for f in fields]
        maxfields = 0
        ret = (None, None)
        
        for dest, route in self.rib.items():
            # HACK to make tophat on join
            if not join and dest.platform in ['tophat', 'myslice']:
                continue
            isect = set(dest.fields).intersection(set(_fields))
            if len(isect) > maxfields:
                maxfields = len(isect)
                ret = (dest, isect)
        return ret

    def metadata_get_keys(self, table_name):
        for t in self.rib.keys(): # HUM
            if t.name == table_name:
                return t.keys
        return None

    def compute_query_plan(self, query):

        # XXX this should be replaced by a Steiner Tree computation
        # XXX This should manage subqueries !!!!

        from tophat.core.ast import AST
        from tophat.core.metadata import Metadata
        from tophat.core.gateway import Gateway

        fact_table, filters, fields = query.fact_table, query.filters, query.fields

        # This method is broken, need to replace it with steiner

        if filters:
            fields.extend(filters.keys())
        fields = set(fields)
         
        # Query plan 
        qp = AST(self)
        join = False 

        # Note: We could skip or restrict the set of platforms, and ask for routing or timing information 
        while True: 
            #print "REMAINING FIELDS: ", fields
            table, qfields = self.get_platform_max_fields(fields, join) 
            if not table: 
                raise Exception, "Cannot complete query: %s" % fields 

            #print "CALLING %s::%s for (%r)" % (table.platform, table.name, fields)

            #q = Query(fact_table, ts, {}, list(p['fields'])) 
            #gateways = MetadataGateways(self.api, {'platform': p['platform']}) 
            #if not gateways: 
            #    raise Exception, "No gateway found for platform '%(platform)s'" % p 
            #config = json.loads(gateways[0]['config']) 

            ## We add the caller to the config parameter 
            #config['caller'] = self.caller 

            # We need the key to perform the join
            key = table.keys[0]
            qfields.add(key)
            if not join: 
                qp = qp.From(table, qfields) 
                join = True 
            else: 
                r = AST(self).From(table, qfields) 
                # we join on hostname (hardcoded) 
                qp = qp.join(r, key)

            # Remove the fields we obtain from the ones yet to be queried
            for qf in qfields:
                fields = [f for f in fields if not f == qf and not f.startswith("%s." % qf)]
                #if qf in fields: 
                #    fields.remove(qf) 

            # Stop if we have no more fields to query
            if not fields: 
                break 

        # Now we apply the operators
        qp = qp.selection(query.filters) 
        qp = qp.projection(query.fields) 
        #qp = qp.sort(query.get_sort()) 
        #qp = qp.limit(query.get_limit()) 

        return qp
        #return list(qp._get()) 


    #def cb(self, value):
    #    print "callback"
    #    if not value:
    #        print "LAST VALUE =========================================="
    #        self.event.set()
    #    self.results.append(value)

    def do_forward(self, query, route, deferred):
        """
        Effectively runs the forwarding of the query to the route
        """

        # the router parameter is ignored until we clearly state what are the
        # different entities of a router and their responsabilities

        
        # Parameter route is ignored temporarily, we compute it naively...

        try:
            def attribute_closure(attributes, fd_set):
                """
                Compute the closure of a set of attributes under the set of functional dependencies fd_set
                """
                if not isinstance(attributes, (set, frozenset)):
                    closure = set([attributes])
                else:
                    closure = set(attributes)
                while True:
                    old_closure = closure.copy()
                    for y, z in fd_set: # Y -> Z
                        if y in closure:
                            closure.add(z)
                    if old_closure == closure:
                        break
                return closure
    
            def fd_minimal_cover(fd_set):
                # replace each FD X -> (A1, A2, ..., An) by n FD X->A1, X->A2, ..., X->An
                min_cover = set([(y, attr) for y, z in fd_set for attr in z if y != attr])
                for x, a in min_cover.copy():
                    reduced_min_cover = set([fd for fd in min_cover if fd != (x,a)])
                    x_plus = attribute_closure(x, reduced_min_cover)
                    if x == 'asn':
                        print "x_plus = ", x_plus
                    if a in x_plus:
                        if x == 'asn':
                            print "reduced : ", min_cover
                            print "removing: ", x, a
                            print "to : ", reduced_min_cover
                        min_cover = reduced_min_cover
                for x, a in min_cover:
                    if isinstance(x, frozenset):
                        for b in x:
                            # Compute (X-B)+ with respect to (G-(X->A)) U ((X-B)->A) = S
                            x_minus_b = frozenset([i for i in x if i != b])
                            s = set([fd for fd in min_cover if fd != (x,a)])
                            s.add((x_minus_b, a))
                            x_minus_b_plus = attribute_closure(x_minus_b, s) 
                            if b in x_minus_b_plus:
                                reduced_min_cover = set([fd for fd in min_cover if fd != (x,a)])
                                min_cover = reduced_min_cover
                                min_cover.add( (x_minus_b, a) )
                return min_cover

            def to_3nf(tables):
                # Build the set of functional dependencies
                fd_set = set([(key, g.fields) for g in tables for key in g.keys])
                print "I: fd_set built"

                # Find a minimal cover
                fd_set = fd_minimal_cover(fd_set)
                print "I: minimal cover done"
                
                # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
                # containing all FDs in G with the same determinant X ...
                determinants = {}
                for x, a in fd_set:
                    if not x in determinants:
                        determinants[x] = set([])
                    determinants[x].add(a)
                print "I: determinants ok"

                # ... create relaton R = (X, A1, A2, ..., An)
                relations = []
                for x, y in determinants.items():
                    # Platform list and names for the corresponding key x and values
                    sources = [t for t in tables if x in t.keys]
                    p = [s.platform for s in sources]
                    n = list(sources)[0].name
                    # Note, we do not manage multiple keys here...d
                    fields = list(y)
                    if isinstance(x, frozenset):
                        fields.extend(list(x))
                    else:
                        fields.append(x)
                    t = Table(','.join(p), n, fields, [x])
                    print "TABLE", x, " -- ", fields
                    relations.append(t)
                return relations

            def build_Gnf(tables_3nf):
                G_nf = nx.DiGraph() 
                for table in tables_3nf:
                    sources = [t for t in tables if list(table.keys)[0] in t.keys]
                    G_nf.add_node(table, {'sources': sources})

                    # We loop through the different _nodes_ of the graph to see whether
                    # we need to establish some links
                    for node, data in G_nf.nodes(True):
                        if node == table: # or set(node.keys) & set(table.keys):
                            continue

                        # Another table is pointing to the considered _table_:
                        # FK -> local.PK
                        link = False
                        for k in table.keys:
                            # Checking for the presence of each key of the table in previously inserted tables
                            if isinstance(k, frozenset):
                                if set(k) <= set(node.fields): # Multiple key XXX
                                    link = True
                            else:
                                if k in node.fields:
                                    link = True
                        if link:
                            print "EDGE: %s -> %s" % (node, table)
                            G_nf.add_edge(node, table, {'cost': True})
                        
                        # The considered _table_ has a pointer to the primary key of another table
                        # local.FK -> PK
                        link = False
                        # Testing for each possible key of the _node_
                        for k in node.keys:
                            if isinstance(k, frozenset):
                                if set(k) <= set(t.fields): # Multiple key XXX
                                    link = True
                            else:
                                # the considered key _k_ is a simple field
                                if k in table.fields:
                                    link = True

                        if link:
                            print "EDGE: %s -> %s" % (table, node)
                            G_nf.add_edge(table, node, {'cost': True})

                        # If _table_ names the object _node_ 1..N (or 1..1)
                        if node.name in table.fields:
                            G_nf.add_edge(table, node, {'cost': True, 'type': '1..N'})

                return G_nf

            def get_tree_edges(G_nf, root):
                return [e for e in dfs_edges(G_nf, root)]

            def get_root(G_nf, query):
                # Let's extract the query tree rooted at the fact table
                root = [node[0] for node in G_nf.nodes(True) if node[0].name == query.fact_table]
                if not root:
                    raise Exception, "no root found"
                #print "root=", root
                root = root[0]
                return root
    

            def prune_query_tree(tree, tree_edges, nodes, query_fields):
                # *** Compute the query plane ***
                print "compute query plane in tree"
                for node in tree.nodes():
                    data = nodes[node]
                    if 'visited' in data and data['visited']:
                        break;
                    if (set(query_fields) & set(node.fields)):
                        # mark all nodes until we reach the root (no pred) or a marked node
                        cur_node = node
                        # XXX DiGraph.predecessors_iter(n)
                            #link = True
                        while True:
                            if 'visited' in data and data['visited']:
                                break
                            data['visited'] = True
                            print "marking %s as visited" % cur_node
                            pred = tree.predecessors(cur_node)
                            if not pred:
                                break
                            cur_node = pred[0]
                            data = nodes[cur_node]
                visited_tree_edges = [e for e in tree_edges if 'visited' in nodes[e[0]] and 'visited' in nodes[e[1]]]
                #print ["%s %s" % (s,e) for s,e in visited_tree_edges]
                #print "Building tree of visited nodes"
                #tree = nx.DiGraph(visited_tree_edges)
                #return tree
                # clean up
                for node in tree.nodes():
                    if 'visited' in nodes[node]:
                        del nodes[node]['visited']
                return visited_tree_edges
    
            def process_query(query, G_nf):
                # We process a single query without caring about 1..N
                # former method
                nodes = dict(G_nf.nodes(True))

                # Builds the query tree rooted at the fact table
                root = get_root(G_nf, query)
                tree_edges = get_tree_edges(G_nf, root)
                tree = nx.DiGraph(tree_edges)

                # Plot it
                #nx.draw_graphviz(tree)
                #plt.show()

                # Necessary fields are the one in the query augmented by the keys in the filters
                needed_fields = set(query.fields)
                if query.filters:
                    needed_fields.update(query.filters.keys())
                print "needed fields", needed_fields

                # Prune the tree from useless tables
                visited_tree_edges = prune_query_tree(tree, tree_edges, nodes, needed_fields)
                #tree = prune_query_tree(tree, tree_edges, nodes, needed_fields)
                if not visited_tree_edges:
                    # The root is sufficient
                    print "The root is sufficient"
                    return AST(self).From(root, needed_fields)

                qp = None
                root = True
                for s, e in visited_tree_edges:
                    # We start at the root if necessary
                    if root:
                        print "a"
                        print s, e
                        print s.fields
                        local_fields = set(needed_fields) & s.fields
                        # We add fields necessary for performing joins = keys of all the children
                        # XXX does not work for multiple keys
                        ###print "LOCAL FIELDS", local_fields
                        ###for ss,ee in visited_tree_edges:
                        ###    if ss == s:
                        ###        local_fields.update(ee.keys)
                        ###print "LOCAL FIELDS", local_fields

                        if not local_fields:
                            print "b", local_fields
                            break

                        # We adopt a greedy strategy to get the required fields (temporary)
                        # We assume there are no partitions
                        first_join = True
                        left = AST(self)
                        sources = nodes[s]['sources'][:]
                        while True:
                            max_table, max_fields = get_table_max_fields(local_fields, sources)
                            if not max_table:
                                raise Exception, 'get_table_max_fields error: could not answer fields: %r for query %s' % (local_fields, query)
                            sources.remove(max_table)
                            if first_join:
                                left = AST(self).From(max_table, list(max_fields))
                                first_join = False
                            else:
                                right = AST(self).From(max_table, list(max_fields))
                                left = left.join(right, iter(s.keys).next())
                            local_fields.difference_update(max_fields)
                            needed_fields.difference_update(max_fields)
                            if not local_fields:
                                break
                            # read the key
                            local_fields.add(iter(s.keys).next())
                        qp = left
                        root = False

                    # Proceed with the JOIN
                    local_fields = set(needed_fields) & e.fields
                    # We add fields necessary for performing joins = keys of all the children
                    # XXX does not work for multiple keys
                    for ss,ee in visited_tree_edges:
                        if ss == e:
                            local_fields.update(ee.keys)

                    if not local_fields:
                        return qp

                    # We adopt a greedy strategy to get the required fields (temporary)
                    # We assume there are no partitions
                    first_join = True
                    left = AST(self)
                    sources = nodes[e]['sources'][:]
                    while True:
                        max_table, max_fields = get_table_max_fields(local_fields, sources)
                        if not max_table:
                            print "break max table 2"
                            break;
                        if first_join:
                            left = AST(self).From(max_table, list(max_fields))
                            first_join = False
                        else:
                            right = AST(self).From(max_table, list(max_fields))
                            left = left.join(right, iter(e.keys).next())
                        local_fields.difference_update(max_fields)
                        needed_fields.difference_update(max_fields)
                        if not local_fields:
                            break
                        # readd the key
                        local_fields.add(iter(e.keys).next())

                    qp = qp.join(left, iter(e.keys).next()) # XXX
                return qp
                
            # END EXPERIMENTAL CODE

            # FORMER QUERY PLAN COMPUTATION
            #qp = self.compute_query_plan(query)



            def process_subqueries(query, G_nf):
                qp = AST(self)

                cur_filters = []
                cur_params = []
                cur_fields = []
                subq = {}

                # XXX there are some parameters that will be answered by the parent !!!! no need to request them from the children !!!!
                # XXX XXX XXX XXX XXX XXX ex slice.resource.PROPERTY

                for pred in query.filters:
                    if '.' in pred.key:
                        method, subkey = pred.key.split('.', 1)
                        if not method in subq:
                            subq[method] = {}
                        if not 'filters' in subq[method]:
                            subq[method]['filters'] = []
                        subq[method]['filters'].append(Predicate(subkey, pred.op, pred.value))
                    else:
                        cur_filters.append(pred)

                # TODO params

                for field in query.fields:
                    if '.' in field:
                        method, subfield = field.split('.', 1)
                        if not method in subq:
                            subq[method] = {}
                        if not 'fields' in subq[method]:
                            subq[method]['fields'] = []
                        subq[method]['fields'].append(subfield)
                    else:
                        cur_fields.append(field)

                children_ast = []
                for method, subquery in subq.items():
                    # We need to add the keys of each subquery
                    # 
                    # We append the method name (eg. resources) which should return the list of keys
                    # (and eventually more information, but they will be ignored for the moment)
                    if not method in cur_fields:
                        cur_fields.append(method)

                    # Recursive construction of the processed subquery
                    subfilters = subquery['filters'] if 'filters' in subquery else []
                    subparams = subquery['params'] if 'params' in subquery else []
                    subfields = subquery['fields'] if 'fields' in subquery else []
                    subquery = THQuery(query.action, method, subfilters, subparams, subfields)
                    # XXX TODO we need callbacks between subqueries
                    child_ast = process_subqueries(subquery, G_nf)
                    children_ast.append(child_ast)

                parent = THQuery(query.action, query.fact_table, cur_filters, cur_params, cur_fields)
                print "processing query parent", parent
                parent_ast = process_query(parent, G_nf)
                qp = parent_ast
                qp.subquery(children_ast)
                return qp

            def get_table_max_fields(fields, tables):
                maxfields = 0
                ret = (None, None)
                for t in tables:
                    isect = set(fields).intersection(t.fields)
                    if len(isect) > maxfields:
                        maxfields = len(isect)
                        ret = (t, isect)
                return ret

            def run_query(query):
                output = []
                print "run_query"
                return output

            # BUILD THE QUERY PLAN
            #######################

            tables = self.rib.keys() # HUM
            tables_3nf = to_3nf(tables)
            for t in tables_3nf:
                print t, t.fields

            # Creates a join graph from the tables in normalized format
            G_nf = build_Gnf(tables_3nf)

            # Plot it
            #nx.draw_graphviz(G_nf)
            #plt.show()

            qp = process_subqueries(query, G_nf)


        except Exception ,e:
            print "Exception in do_forward", e
            traceback.print_exc()
            return []
            
        # We should now have a query plan
        print ""
        print "QUERY PLAN:"
        print "-----------"
        qp.dump()
        print ""
        print ""

        print "I: Install query plan."
        d = defer.Deferred() if deferred else None
        cb = Callback(d, self.event)
        qp.start()

        self.sourcemgr.run()

        if deferred:
            return d

        self.event.wait()
        self.event.clear()
        return cb.results


class THRouter(THLocalRouter, Router):
    pass


